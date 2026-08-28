"""Lease invariants: atomic claim, owner-bound refresh and release."""

from __future__ import annotations

import datetime as dt
from pathlib import Path
import sys
import tempfile
import unittest

import conftest  # noqa: F401  (path setup)

from support import build_bridge  # noqa: E402
from coordination_substrate.lease import (  # noqa: E402
    LeaseError,
    claim,
    read_lease,
    refresh,
    release,
)

DIGEST = "a" * 64
OTHER_DIGEST = "b" * 64
SESSION = "b4a20b7d-100b-4f13-a509-9340559ed468"
START = dt.datetime(2026, 8, 1, 12, 0, 0, tzinfo=dt.timezone.utc)


class LeaseTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.topology = build_bridge(self.root)

    def tearDown(self):
        self.temp.cleanup()

    def identity(self, **overrides):
        base = {
            "task_id": "T-1",
            "attempt": 1,
            "request_sha256": DIGEST,
            "owner": "worker-instance-1",
        }
        base.update(overrides)
        return base

    def claim(self, *, at=START, **overrides):
        return claim(
            self.topology,
            **self.identity(**overrides),
            harness="worker-harness",
            listener_pid=10,
            listener_child_pid=11,
            session_id=SESSION,
            clock=lambda: at,
        )

    def test_claim_creates_lease_with_expiry(self):
        result = self.claim()
        self.assertEqual(result["action"], "claimed")
        lease = result["lease"]
        self.assertEqual(lease["acquired_at"], "2026-08-01T12:00:00Z")
        self.assertEqual(lease["expires_at"], "2026-08-01T12:10:00Z")
        self.assertEqual(lease["expiry_seconds"], 600)
        self.assertEqual(read_lease(self.topology)["task_id"], "T-1")

    def test_identical_claim_replays_rather_than_duplicating(self):
        self.claim()
        again = self.claim()
        self.assertEqual(again["action"], "claim-replayed")

    def test_second_owner_cannot_take_a_held_lease(self):
        self.claim()
        with self.assertRaises(LeaseError):
            self.claim(owner="other-instance")
        with self.assertRaises(LeaseError):
            self.claim(task_id="T-2")
        with self.assertRaises(LeaseError):
            self.claim(request_sha256=OTHER_DIGEST)

    def test_refresh_extends_only_for_the_owner(self):
        self.claim()
        later = START + dt.timedelta(seconds=300)
        result = refresh(self.topology, **self.identity(), clock=lambda: later)
        self.assertEqual(result["lease"]["expires_at"], "2026-08-01T12:15:00Z")
        with self.assertRaises(LeaseError):
            refresh(self.topology, **self.identity(owner="intruder"), clock=lambda: later)

    def test_expired_lease_cannot_be_replayed(self):  # LEA-1
        self.claim()
        too_late = START + dt.timedelta(seconds=self.topology.lease_seconds + 1)
        with self.assertRaises(LeaseError) as ctx:
            self.claim(at=too_late)  # same owner, but the lease has expired
        self.assertIn("expired", str(ctx.exception).lower())
        # The documented recovery is release-then-claim; a bare re-claim must not
        # revive the stale lease at exit 0.
        release(self.topology, **self.identity())
        self.assertEqual(self.claim(at=too_late)["action"], "claimed")

    def test_concurrent_claims_yield_exactly_one_holder(self):  # TCI-1
        import threading
        barrier = threading.Barrier(2)
        results, errors = [], []

        def contend(owner):
            barrier.wait()
            try:
                results.append(claim(
                    self.topology, task_id="T-1", attempt=1, request_sha256=DIGEST,
                    owner=owner, harness="h", listener_pid=10, listener_child_pid=11,
                    session_id=SESSION, clock=lambda: START)["action"])
            except LeaseError as exc:
                errors.append(str(exc))

        threads = [threading.Thread(target=contend, args=(f"owner-{i}",)) for i in range(2)]
        for t in threads: t.start()
        for t in threads: t.join()
        # Exactly one atomic winner; the distinct second owner is refused.
        self.assertEqual(results, ["claimed"], f"expected one holder, got {results} / {errors}")
        self.assertEqual(len(errors), 1)

    def test_expired_lease_cannot_be_refreshed(self):
        self.claim()
        too_late = START + dt.timedelta(seconds=601)
        with self.assertRaises(LeaseError):
            refresh(self.topology, **self.identity(), clock=lambda: too_late)

    def test_release_frees_the_slot_only_for_the_owner(self):
        self.claim()
        with self.assertRaises(LeaseError):
            release(self.topology, **self.identity(owner="intruder"))
        result = release(self.topology, **self.identity())
        self.assertEqual(result["action"], "released")
        self.assertIsNone(read_lease(self.topology))
        # The slot is genuinely reusable afterwards.
        self.assertEqual(self.claim()["action"], "claimed")

    def test_invalid_identity_is_refused(self):
        for bad in (
            {"task_id": " padded "},
            {"attempt": 0},
            {"request_sha256": "short"},
            {"owner": ""},
        ):
            with self.subTest(bad=bad):
                with self.assertRaises(LeaseError):
                    self.claim(**bad)

    def test_invalid_session_identity_is_refused(self):
        with self.assertRaises(LeaseError):
            claim(
                self.topology,
                **self.identity(),
                harness="h",
                listener_pid=1,
                listener_child_pid=2,
                session_id="not-a-uuid",
                clock=lambda: START,
            )

    def test_naive_clock_is_refused(self):
        with self.assertRaises(LeaseError):
            self.claim(at=dt.datetime(2026, 8, 1, 12, 0, 0))

    REQUIRED_LEASE_FIELDS = {
        "schema_version",
        "task_id",
        "attempt",
        "request_sha256",
        "owner_instance_id",
        "harness",
        "listener_pid",
        "listener_child_pid",
        "session_id",
        "acquired_at",
        "heartbeat_at",
        "expires_at",
        "expiry_seconds",
    }
    # Present only when /proc could answer. Omitted rather than written null,
    # because absence means "identity never captured" and a null would be a
    # claim the code did not establish.
    OPTIONAL_LEASE_FIELDS = {"listener_start_ticks", "listener_child_start_ticks"}

    def test_lease_document_carries_only_the_declared_fields(self):
        """A whitelist, not a blacklist: nothing unexpected can reach disk."""
        lease = self.claim()["lease"]
        self.assertTrue(
            self.REQUIRED_LEASE_FIELDS <= set(lease),
            f"missing required fields: {self.REQUIRED_LEASE_FIELDS - set(lease)}",
        )
        self.assertEqual(
            set(lease) - self.REQUIRED_LEASE_FIELDS - self.OPTIONAL_LEASE_FIELDS,
            set(),
            "an undeclared field reached disk",
        )
        # Every value is a digest, an identifier, a pid, or a timestamp — no
        # free-form field exists through which a secret could be passed.
        for key, value in lease.items():
            self.assertIsInstance(value, (str, int), f"{key} has an unexpected type")

    def test_persisted_lease_matches_the_returned_document(self):
        returned = self.claim()["lease"]
        self.assertEqual(read_lease(self.topology), returned)


if __name__ == "__main__":
    unittest.main()

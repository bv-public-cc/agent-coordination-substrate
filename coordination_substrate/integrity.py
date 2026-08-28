#!/usr/bin/env python3
"""Ledger integrity: does the append-only log still say what it said?

This is the vocabulary-independent half of the source project's metrics tool.
It answers questions that hold for any bridge:

- Does every event still parse?
- Does every declared reference still hash to what was recorded?
- Are any ``(writer, sequence)`` identities duplicated?

It does **not** compute throughput. Throughput needs to know which event names
mean "assigned" and "accepted", which is project vocabulary; that lives in
:mod:`metrics_reference`.

The central distinction is ``reference_intent``:

``immutable_ref``
    Must always verify. A mismatch is a real integrity failure.
``snapshot_at_publication``
    A point-in-time record of a file expected to change. Informational.

Without that split, a routine draft revision and a tampered evidence file are
indistinguishable, and an integrity signal that cries wolf cannot be used.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import json
from pathlib import Path
import sys
from typing import Iterable, Sequence

import yaml

IMMUTABLE_REF = "immutable_ref"
SNAPSHOT_AT_PUBLICATION = "snapshot_at_publication"
UNKNOWN_INTENT = "unknown"

# Defect kinds that are expected and must never trip fail_closed. Everything
# else -- unparseable, not-an-object, immutable-failure, unknown intent,
# symlinked-target, missing-event-directory, path-escape, malformed-binding --
# is a reader-must-not-ignore condition. (INT-1, INT-2, INT-3.)
_BENIGN_KINDS = frozenset({"snapshot-drift", "snapshot-absent"})


def _escapes_root(root: Path, rel: str) -> bool:
    """True when a declared reference path resolves outside the ledger root.

    An absolute `path:` replaces the root under `/` semantics, `..` traverses
    out, and a symlinked parent redirects the target -- so containment is checked
    on the fully resolved path, not only the leaf. (INT-4.)
    """
    try:
        if Path(rel).is_absolute() or ".." in Path(rel).parts:
            return True
        resolved = (root / rel).resolve()
        return root not in resolved.parents and resolved != root
    except (OSError, ValueError):
        return True


class _StrictLoader(yaml.SafeLoader):
    """Reject duplicate mapping keys instead of silently taking the last."""


def _no_duplicate_keys(loader, node, deep=False):
    mapping = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in mapping:
            raise yaml.YAMLError(f"duplicate key {key!r}")
        mapping[key] = loader.construct_object(value_node, deep=deep)
    return mapping


_StrictLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _no_duplicate_keys
)


@dataclass
class Defect:
    kind: str
    path: str
    detail: str = ""
    referenced_by: str = ""

    def as_dict(self) -> dict:
        value = {"kind": self.kind, "path": self.path}
        if self.detail:
            value["detail"] = self.detail
        if self.referenced_by:
            value["referenced_by"] = self.referenced_by
        return value


@dataclass
class Record:
    path: str
    body: dict
    sha256: str


@dataclass
class IntegrityReport:
    loaded: int = 0
    rejected: int = 0
    bindings_checked: int = 0
    immutable_failures: int = 0
    snapshot_drift: int = 0
    unknown_intent: int = 0
    structural_failures: int = 0
    duplicate_identities: list = field(default_factory=list)
    defects: list = field(default_factory=list)

    @property
    def fail_closed(self) -> bool:
        """True when something happened that a reader must not ignore.

        Any defect whose kind is not explicitly benign trips this, plus duplicate
        identities. Snapshot drift and a legitimately-absent snapshot target are
        the only benign kinds. This scans the recorded defects so a new
        fail-closed kind cannot be forgotten in a hand-maintained counter list.
        """
        return bool(
            self.duplicate_identities
            or any(d.kind not in _BENIGN_KINDS for d in self.defects)
        )

    def as_dict(self) -> dict:
        return {
            "loaded": self.loaded,
            "rejected": self.rejected,
            "bindings_checked": self.bindings_checked,
            "immutable_failures": self.immutable_failures,
            "snapshot_drift": self.snapshot_drift,
            "unknown_intent": self.unknown_intent,
            "structural_failures": self.structural_failures,
            "duplicate_identities": self.duplicate_identities,
            "fail_closed": self.fail_closed,
            "defects": [defect.as_dict() for defect in self.defects],
        }


def load_events(root: Path, event_dirs: Sequence[str]) -> tuple[list, list]:
    """Parse every YAML event under ``event_dirs``. Never raises on bad input."""
    root = Path(root).resolve()
    records: list = []
    defects: list = []
    for relative in event_dirs:
        directory = root / relative
        if not directory.is_dir():
            defects.append(Defect("missing-event-directory", relative))
            continue
        for path in sorted(directory.glob("*.yaml")):
            rel = str(path.relative_to(root))
            try:
                raw = path.read_bytes()
                body = yaml.load(raw, Loader=_StrictLoader)
            except (OSError, yaml.YAMLError) as exc:
                defects.append(
                    Defect("unparseable", rel, type(exc).__name__)
                )
                continue
            if not isinstance(body, dict):
                defects.append(Defect("not-an-object", rel))
                continue
            records.append(Record(rel, body, hashlib.sha256(raw).hexdigest()))
    return records, defects


def find_duplicate_identities(records: Iterable) -> list:
    """Report any ``(writer, sequence)`` claimed by more than one event."""
    seen: dict = {}
    for record in records:
        writer = record.body.get("writer")
        sequence = record.body.get("sequence")
        # bool is a subclass of int; sequence: true must not pass as an integer,
        # and a string sequence must not silently exempt a colliding event. Such
        # records are surfaced as malformed-identity defects in verify(). (INT-5.)
        if not isinstance(writer, str) or not isinstance(sequence, int) or isinstance(sequence, bool):
            continue
        seen.setdefault((writer, sequence), []).append(record.path)
    return [
        {"writer": writer, "sequence": sequence, "paths": sorted(paths)}
        for (writer, sequence), paths in sorted(seen.items(), key=lambda item: item[0][1])
        if len(paths) > 1
    ]


def _declared_bindings(value: object):
    """Yield every dict that declares a reference_intent alongside a path+sha256.

    A path+sha256 pair WITHOUT a reference_intent is, by this schema's design,
    not a verified reference (pointers, results, and manifests carry the same
    two fields), so requiring the intent key here is deliberate and avoids
    misreading hundreds of legitimate pairs as defects. Closing the intent-drop
    attack (hiding a binding by omitting its intent) needs schema knowledge of
    which fields MUST be immutable references -- that is a project-vocabulary
    decision, out of scope for this vocabulary-independent checker. See the
    residual note in the finding disposition. (INT-2: the malformed-value half is
    fixed in check_bindings; the intent-omission half is documented, not guessed.)
    """
    if isinstance(value, dict):
        if "reference_intent" in value and "path" in value and "sha256" in value:
            yield value
        for child in value.values():
            yield from _declared_bindings(child)
    elif isinstance(value, list):
        for child in value:
            yield from _declared_bindings(child)


def check_bindings(root: Path, records: Iterable) -> tuple[int, list]:
    """Recompute every declared reference and classify what disagrees."""
    root = Path(root).resolve()
    checked = 0
    defects: list = []
    for record in records:
        for binding in _declared_bindings(record.body):
            intent = binding.get("reference_intent")
            rel = binding.get("path")
            expected = binding.get("sha256")
            # A binding-shaped dict with a non-string path or sha256 is malformed,
            # not absent: an all-digit sha256 parses as an int and would otherwise
            # vanish from verification. Surface and fail. (INT-2.)
            if not isinstance(rel, str) or not isinstance(expected, str):
                path_label = rel if isinstance(rel, str) else "<non-string path>"
                defects.append(Defect(
                    "malformed-binding", path_label,
                    f"path={type(rel).__name__} sha256={type(expected).__name__}",
                    record.path))
                continue
            checked += 1
            if intent not in (IMMUTABLE_REF, SNAPSHOT_AT_PUBLICATION):
                defects.append(
                    Defect(UNKNOWN_INTENT, rel, f"intent={intent!r}", record.path)
                )
                continue
            if _escapes_root(root, rel):
                defects.append(Defect("path-escape", rel, "resolves outside the ledger root", record.path))
                continue
            target = root / rel
            if target.is_symlink():
                defects.append(Defect("symlinked-target", rel, "", record.path))
                continue
            try:
                actual = hashlib.sha256(target.read_bytes()).hexdigest()
            except OSError:
                kind = (
                    "immutable-failure" if intent == IMMUTABLE_REF else "snapshot-absent"
                )
                defects.append(Defect(kind, rel, "target absent", record.path))
                continue
            if actual == expected:
                continue
            if intent == IMMUTABLE_REF:
                defects.append(
                    Defect(
                        "immutable-failure",
                        rel,
                        "content differs from the recorded digest",
                        record.path,
                    )
                )
            else:
                # Expected: the file was always going to change after this
                # publication recorded it. Reported, never alarming.
                defects.append(
                    Defect("snapshot-drift", rel, "changed since publication", record.path)
                )
    return checked, defects


def _malformed_identity_defects(records: Iterable) -> list:
    """Surface a sequenced event whose identity is ill-typed, so a colliding
    event cannot hide behind e.g. sequence: "7" or sequence: true. (INT-5.)

    Only *sequenced* events bear identity. A record with no sequence key or a
    null sequence (a heartbeat or a non-task notice) is not identity-bearing and
    is not flagged; the attack this closes is a present-but-mistyped sequence.
    """
    out = []
    for record in records:
        writer = record.body.get("writer")
        sequence = record.body.get("sequence")
        if sequence is None:
            continue  # unsequenced (heartbeat/notice): not identity-bearing
        if isinstance(sequence, int) and not isinstance(sequence, bool) and isinstance(writer, str):
            continue  # well-formed identity
        out.append(Defect(
            "malformed-identity", record.path,
            f"writer={type(writer).__name__} sequence={type(sequence).__name__}"))
    return out


_STRUCTURAL_KINDS = frozenset({
    "symlinked-target", "missing-event-directory", "path-escape",
    "malformed-binding", "malformed-identity", UNKNOWN_INTENT,
})


def verify(root: Path, event_dirs: Sequence[str]) -> IntegrityReport:
    """Full pass: load, hash-check every binding, and detect duplicate ids."""
    records, load_defects = load_events(root, event_dirs)
    checked, binding_defects = check_bindings(root, records)
    duplicates = find_duplicate_identities(records)
    identity_defects = _malformed_identity_defects(records)
    all_defects = load_defects + binding_defects + identity_defects

    report = IntegrityReport(
        loaded=len(records),
        rejected=sum(1 for d in load_defects if d.kind in {"unparseable", "not-an-object"}),
        bindings_checked=checked,
        duplicate_identities=duplicates,
        defects=all_defects,
    )
    for defect in all_defects:
        if defect.kind == "immutable-failure":
            report.immutable_failures += 1
        elif defect.kind == "snapshot-drift":
            report.snapshot_drift += 1
        elif defect.kind == UNKNOWN_INTENT:
            report.unknown_intent += 1
        if defect.kind in _STRUCTURAL_KINDS:
            report.structural_failures += 1
    return report


def render(report: IntegrityReport) -> str:
    lines = [
        "# Ledger integrity",
        "",
        f"- events loaded: **{report.loaded}**",
        f"- events rejected: **{report.rejected}**",
        f"- reference bindings checked: **{report.bindings_checked}**",
        f"- immutable failures: **{report.immutable_failures}**",
        f"- snapshot drift (expected): **{report.snapshot_drift}**",
        f"- unknown intent: **{report.unknown_intent}**",
        f"- structural failures: **{report.structural_failures}**",
        f"- duplicate identities: **{len(report.duplicate_identities)}**",
        "",
        f"**fail_closed: {str(report.fail_closed).lower()}**",
        "",
    ]
    alarming = [
        d for d in report.defects
        if d.kind not in {"snapshot-drift"}
    ]
    if alarming:
        lines.append("## Defects requiring attention")
        lines.append("")
        for defect in alarming:
            suffix = f" (referenced by `{defect.referenced_by}`)" if defect.referenced_by else ""
            detail = f" — {defect.detail}" if defect.detail else ""
            lines.append(f"- `{defect.kind}` `{defect.path}`{detail}{suffix}")
        lines.append("")
    if report.snapshot_drift:
        lines.append(
            f"{report.snapshot_drift} snapshot reference(s) changed since publication. "
            "That is the declared intent and is not a defect."
        )
        lines.append("")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Verify append-only ledger integrity.")
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--topology", default="coordination/topology.yaml")
    parser.add_argument("--event-dir", action="append", default=[])
    parser.add_argument("--format", choices=("json", "markdown"), default="markdown")
    parser.add_argument(
        "--strict",
        action="store_true",
        help="exit non-zero when fail_closed is true",
    )
    args = parser.parse_args(argv)

    event_dirs = args.event_dir
    if not event_dirs:
        from .topology import Topology, TopologyError

        try:
            event_dirs = list(Topology.load(args.root, args.topology).event_dirs)
        except TopologyError as exc:
            print(f"REFUSED: {exc}", file=sys.stderr)
            return 2

    report = verify(args.root, event_dirs)
    if args.format == "json":
        print(json.dumps(report.as_dict(), indent=2, sort_keys=True))
    else:
        print(render(report))
    return 1 if (args.strict and report.fail_closed) else 0


if __name__ == "__main__":
    raise SystemExit(main())

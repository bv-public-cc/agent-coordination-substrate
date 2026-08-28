#!/usr/bin/env python3
"""A minimal stand-in for the agent CLI, for integration tests.

Reads newline-delimited JSON on stdin and, when ``--replay-user-messages`` is
present, echoes each user message back on stdout. That echo is the receipt the
listener requires as proof of ingestion.

Behaviour switches for testing failure modes, via environment variables:

``FAKE_AGENT_NO_REPLAY``   accept input but never echo (transport ack timeout)
``FAKE_AGENT_EXIT``        exit immediately with this status code
``FAKE_AGENT_STDERR``      write this text to stderr before doing anything
"""

from __future__ import annotations

import json
import os
import sys


def main() -> int:
    noise = os.environ.get("FAKE_AGENT_STDERR")
    if noise:
        sys.stderr.write(noise)
        sys.stderr.flush()

    exit_code = os.environ.get("FAKE_AGENT_EXIT")
    if exit_code is not None:
        return int(exit_code)

    replay = "--replay-user-messages" in sys.argv
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            value = json.loads(line)
        except ValueError:
            continue
        if not replay or os.environ.get("FAKE_AGENT_NO_REPLAY"):
            continue
        content = value.get("message", {}).get("content", "")
        sys.stdout.write(
            json.dumps({"type": "user", "message": {"role": "user", "content": content}}) + "\n"
        )
        sys.stdout.flush()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Make the package and the shared fixtures importable without path hacks."""

from __future__ import annotations

from pathlib import Path
import sys

_ROOT = Path(__file__).resolve().parents[1]
for entry in (str(_ROOT), str(_ROOT / "tests")):
    if entry not in sys.path:
        sys.path.insert(0, entry)

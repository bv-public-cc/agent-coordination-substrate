"""Coordination substrate for multiple autonomous agents over shared state.

Four independent pieces, usable together or separately:

``topology``
    Declarative description of one bridge: an orchestrator, its agent roles,
    and the filesystem locations that carry authority between them.
``publisher``
    Gated append-only publication. An event is installed first, then the
    pointer is bound to that event's exact digest. Failure can orphan an
    event; it can never publish a pointer to missing or mismatched content.
``listener``
    Wake transport. Watches validated pointers and queues one turn into a
    long-lived agent subprocess, requiring a replay receipt as proof of
    ingestion rather than assuming a pipe write succeeded.
``lease``
    One exclusive mutating claim, acquired by atomic ``mkdir`` and bound to
    an owner tuple.

``integrity`` verifies an existing ledger without knowing any event names.
``metrics_reference`` is included as-is: it computes ledger integrity and
throughput over an append-only event log, but encodes one team's event
vocabulary. See ``docs/PORTING.md`` before reusing it.
"""

from .orient import OrientVerdict, decide as orient_decide
from .topology import ActiveWorkProbe, Role, Topology, TopologyError

__all__ = [
    "ActiveWorkProbe",
    "OrientVerdict",
    "Role",
    "Topology",
    "TopologyError",
    "orient_decide",
    "__version__",
]

__version__ = "0.1.0"

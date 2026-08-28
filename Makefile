PYTHON ?= python3

.PHONY: test lint demo integrity clean

test:
	$(PYTHON) -m unittest discover -s tests -p "test_*.py" -t tests

# Build a throwaway bridge, publish through it, and prove the pointer binds to
# the installed bytes. Leaves nothing behind.
demo:
	@$(PYTHON) scripts/demo.py

lint:
	$(PYTHON) -m compileall -q coordination_substrate tests && echo "compile clean"

clean:
	find . -name __pycache__ -type d -exec rm -rf {} + 2>/dev/null || true

# Verify an existing ledger.
#   make integrity ROOT=/path/to/repo
# Event directories come from ROOT/coordination/topology.yaml unless EVENT_DIRS
# is given, which lets the check run against a ledger that predates a topology:
#   make integrity ROOT=/path EVENT_DIRS="bridge/events bridge/audit-events"
ROOT ?= .
EVENT_DIRS ?=
integrity:
	$(PYTHON) -m coordination_substrate.integrity --root $(ROOT) --strict \
	  $(foreach dir,$(EVENT_DIRS),--event-dir $(dir))

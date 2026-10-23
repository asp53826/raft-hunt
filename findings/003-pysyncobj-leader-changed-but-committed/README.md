# 003 · PySyncObj reports LEADER_CHANGED for commands that do commit

| | |
|---|---|
| library | [PySyncObj](https://github.com/bakwc/PySyncObj) 0.3.17 (= `master` at 6962777) |
| property | client-visible outcome reporting (a documentation/behaviour mismatch that leads callers to apply commands twice) |
| found by | the harness's own first mistake: it trusted the docs, and 7 runs of the first campaign failed linearizability until it stopped |
| reproduction | [`unit_repro.py`](unit_repro.py) (pysyncobj only, no simulator) |
| proposed fix | [`fix.diff`](fix.diff) (docs) or return the existing `UNKNOWN_OUTCOME` |
| status | **not yet reported upstream** |

## What happens

`config.py` documents the error as:

```python
LEADER_CHANGED = 5      #: Similar to NOT_LEADER - leader has changed without command commit.
```

It's raised in one place, when a node sees a new leader:

```python
def __onLeaderChanged(self):
    for id in sorted(self.__commandsWaitingReply):
        self.__commandsWaitingReply[id](None, FAIL_REASON.LEADER_CHANGED)
```

`__commandsWaitingReply` holds commands this node forwarded to the leader
whose `apply_command_response` hasn't arrived yet. The old leader may well
have appended and replicated them already; if a majority has them, the next
leader commits them. The caller has been told "without command commit", and
the command is applied anyway. A caller that retries on that error (the
natural reading of the docs) applies it twice: a counter increments twice, a
queue gets two copies.

## Evidence

`unit_repro.py`, in a clean virtualenv with only `pysyncobj==0.3.17`:

```
a forwarded the command to b: {'type': 'apply_command', 'request_id': 1}
callback after the leader change: ['LEADER_CHANGED']
a applied up to 4 | counter = 1
REPRODUCED: incr() was reported LEADER_CHANGED (documented as 'without command commit') and committed anyway
```

In the simulator, seeds 0–399 of the default profile: 192 operations in 117
runs got `LEADER_CHANGED` and were committed anyway. No operation that got
`DISCARDED`, `NOT_LEADER`, `QUEUE_FULL`, `MISSING_LEADER` or
`REQUEST_DENIED` was ever committed, which matches reading the code: those
really are definite failures.

## Proposed fix

The outcome is genuinely unknown at that point, and the library already has
a code for that: `UNKNOWN_OUTCOME = 7`. Either:

- **docs only** ([`fix.diff`](fix.diff)): describe `LEADER_CHANGED` as an
  unknown outcome, so callers know a retry needs to be idempotent; or
- **behaviour:** call back with `UNKNOWN_OUTCOME` in `__onLeaderChanged`. More
  accurate, but callers matching on `LEADER_CHANGED` would need to change, so
  it's the maintainer's call.

## Draft issue

> **`LEADER_CHANGED` is documented as "without command commit", but the command can still be committed**
>
> `__onLeaderChanged` fails every forwarded command still waiting for the
> leader's `apply_command_response` with `LEADER_CHANGED`. If the old leader had
> already replicated it to a majority, the new leader commits it, so callers
> who retry on this error apply the command twice. Minimal reproduction (only
> pysyncobj 0.3.17): [unit_repro.py]. In 400 simulated fault runs, 192 commands
> got `LEADER_CHANGED` and were committed. Suggest documenting it as an unknown
> outcome, or returning `UNKNOWN_OUTCOME`.
>
> Found with deterministic simulation testing: https://github.com/asp53826/raft-hunt

"""Linearizability checking for a replicated key-value register.

Keys are independent registers, so a history is linearizable exactly when the
sub-history of every key is (linearizability is compositional). Each key is
checked with the Wing & Gong search plus Lowe's memoization, the same
algorithm Knossos and Porcupine use:

walk the call/return events in time order; at a call, try to linearize that
operation now (the model must accept it, and the resulting
(set-of-linearized-ops, state) pair must not have been explored before); at a
return whose operation is still pending, nothing earlier can be reordered to
make it fit, so backtrack. Reaching the end with every operation linearized
is a proof; exhausting the search is a counterexample.

Operations with an unknown outcome get a return event at +infinity, so they
may be placed anywhere after their invocation, including after everything
else, which is the same as never having happened. Reads with unknown outcome
carry no information and are dropped. Definite failures are dropped.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

from .history import INF, History, Op

MISSING = object()   # the register before any write


def step(state: Any, op: Op) -> tuple[bool, Any]:
    """Apply ``op`` to a register in ``state``: (allowed?, new state)."""
    if op.kind == "write":
        return True, op.value
    if op.kind == "read":
        if op.status == "info":
            return True, state
        seen = None if state is MISSING else state
        return seen == op.output, state
    if op.kind == "cas":
        expected, new = op.value
        current = None if state is MISSING else state
        if op.status == "info":
            # the outcome is unknown to the client, but not to the register:
            # cas succeeds exactly when the current value matches
            return True, (new if current == expected else state)
        if op.output:
            return current == expected, new
        return current != expected, state
    raise ValueError(f"unknown op kind {op.kind!r}")


@dataclass
class KeyResult:
    key: str
    ok: bool
    ops: int
    explored: int
    # on failure: the deepest linearized prefix the search reached, and the
    # operation whose return could not be preceded by any legal order
    prefix: list[int] = field(default_factory=list)
    stuck_on: Optional[int] = None


@dataclass
class Result:
    ok: bool
    keys: list[KeyResult]

    @property
    def failures(self) -> list[KeyResult]:
        return [k for k in self.keys if not k.ok]


def _written(o: Op):
    return o.value if o.kind == "write" else o.value[1] if o.kind == "cas" else None


def _relevant(ops: list[Op]) -> list[Op]:
    out = []
    for o in ops:
        if o.status == "fail":
            continue
        if o.kind == "read" and o.status == "info":
            continue
        out.append(o)
    # An unknown-outcome write whose value is unique and never observed can't be
    # told apart from one that never happened, so dropping it keeps the verdict
    # and shrinks the search (those writes carry +infinity returns and are most
    # of its cost). "Observed" means a read returned the value, a cas compared
    # against it, or a later cas failed: a failed cas sees "not equal to e",
    # which any other value explains, so writes invoked before its return stay.
    written = {}
    for o in out:
        v = _written(o)
        if v is not None:
            written[repr(v)] = written.get(repr(v), 0) + 1
    observed = set()
    for o in out:
        if o.kind == "read" and o.status == "ok":
            observed.add(repr(o.output))
        elif o.kind == "cas":
            observed.add(repr(o.value[0]))
    failed_cas_returns = [o.complete for o in out if o.kind == "cas" and o.status == "ok" and not o.output]
    last_failed_cas = max(failed_cas_returns, default=float("-inf"))
    return [o for o in out
            if not (o.status == "info" and o.kind in ("write", "cas")
                    and written.get(repr(_written(o))) == 1 and repr(_written(o)) not in observed
                    and o.invoke > last_failed_cas)]


def check_key(key: str, ops: list[Op], max_explored: int = 60_000_000) -> KeyResult:
    ops = _relevant(ops)
    n = len(ops)
    if n == 0:
        return KeyResult(key, True, 0, 0)
    # events: (time, kind, index). Calls sort before returns at equal times,
    # so touching intervals count as concurrent (the permissive choice).
    events = []
    for i, o in enumerate(ops):
        events.append((o.invoke, 0, i))
        events.append((o.complete if o.complete != INF else INF, 1, i))
    events.sort(key=lambda e: (e[0], e[1], e[2]))

    # doubly linked list over events so lifting an op is O(1)
    m = len(events)
    nxt = list(range(1, m)) + [None]
    prv = [None] + list(range(0, m))
    head = 0 if m else None
    call_pos = [0] * n
    ret_pos = [0] * n
    for p, (_, kind, i) in enumerate(events):
        (call_pos if kind == 0 else ret_pos)[i] = p

    def unlink(p):
        nonlocal head
        a, b = prv[p], nxt[p]
        if a is None:
            head = b
        else:
            nxt[a] = b
        if b is not None:
            prv[b] = a

    def relink(p):
        nonlocal head
        a, b = prv[p], nxt[p]
        if a is None:
            head = p
        else:
            nxt[a] = p
        if b is not None:
            prv[b] = p

    state: Any = MISSING
    linearized = 0                       # bitset of op indices
    seen: set[tuple[int, Any]] = set()
    stack: list[tuple[int, Any]] = []    # (op index, state before)
    explored = 0
    best_prefix: list[int] = []
    stuck: Optional[int] = None

    def freeze(s):
        return ("__missing__",) if s is MISSING else (repr(type(s)), repr(s))

    cur = head
    while cur is not None:
        explored += 1
        if explored > max_explored:
            raise RuntimeError(f"linearizability search for key {key!r} exceeded {max_explored} steps")
        _, kind, i = events[cur]
        if kind == 0:
            allowed, new_state = step(state, ops[i])
            if allowed:
                bits = linearized | (1 << i)
                memo = (bits, freeze(new_state))
                if memo not in seen:
                    seen.add(memo)
                    stack.append((i, state))
                    state, linearized = new_state, bits
                    unlink(call_pos[i]); unlink(ret_pos[i])
                    if len(stack) > len(best_prefix):
                        best_prefix = [s[0] for s in stack]
                    cur = head
                    continue
            cur = nxt[cur]
        else:
            # a pending op's return: something before it has to move
            if len(stack) == len(best_prefix):
                stuck = i        # the op that blocked the deepest attempt
            if not stack:
                return KeyResult(key, False, n, explored, [ops[j].id for j in best_prefix], ops[stuck].id)
            j, prev_state = stack.pop()
            state, linearized = prev_state, linearized & ~(1 << j)
            relink(ret_pos[j]); relink(call_pos[j])
            cur = nxt[call_pos[j]]
    return KeyResult(key, True, n, explored)


def check(history: History, **kw) -> Result:
    results = [check_key(k, history.for_key(k), **kw) for k in history.keys()]
    return Result(all(r.ok for r in results), results)

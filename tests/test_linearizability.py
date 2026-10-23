"""The checker is the judge, so it gets checked against a brute-force oracle."""
import itertools
import random

import pytest

from hunt.history import INF, History, Op
from hunt.linearizability import MISSING, check, check_key, step


def op(i, kind, value=None, invoke=0, complete=1, status="ok", output=None, key="x"):
    return Op(id=i, client=f"c{i}", kind=kind, key=key, value=value, invoke=invoke,
              complete=complete, status=status, output=output)


def test_sequential_history_is_linearizable():
    h = [op(0, "write", 1, 0, 1), op(1, "read", None, 2, 3, output=1), op(2, "write", 2, 4, 5),
         op(3, "read", None, 6, 7, output=2)]
    assert check_key("x", h).ok


def test_stale_read_after_completed_write_is_not():
    h = [op(0, "write", 1, 0, 1), op(1, "write", 2, 2, 3), op(2, "read", None, 4, 5, output=1)]
    r = check_key("x", h)
    assert not r.ok
    assert r.stuck_on == 2


def test_read_concurrent_with_write_may_see_either_value():
    base = [op(0, "write", 1, 0, 1), op(1, "write", 2, 2, 6)]
    assert check_key("x", base + [op(2, "read", None, 3, 4, output=1)]).ok
    assert check_key("x", base + [op(2, "read", None, 3, 4, output=2)]).ok


def test_read_of_initial_state_is_none():
    assert check_key("x", [op(0, "read", None, 0, 1, output=None)]).ok
    assert not check_key("x", [op(0, "read", None, 0, 1, output=7)]).ok


def test_cas_semantics():
    ok = [op(0, "write", 1, 0, 1), op(1, "cas", [1, 2], 2, 3, output=True), op(2, "read", None, 4, 5, output=2)]
    assert check_key("x", ok).ok
    bad = [op(0, "write", 1, 0, 1), op(1, "cas", [5, 2], 2, 3, output=True)]
    assert not check_key("x", bad).ok
    failed_cas_but_should_succeed = [op(0, "write", 1, 0, 1), op(1, "cas", [1, 2], 2, 3, output=False)]
    assert not check_key("x", failed_cas_but_should_succeed).ok


def test_unknown_write_may_or_may_not_happen():
    w = op(1, "write", 2, 2, INF, status="info")
    seen_it = [op(0, "write", 1, 0, 1), w, op(2, "read", None, 10, 11, output=2)]
    missed_it = [op(0, "write", 1, 0, 1), w, op(2, "read", None, 10, 11, output=1)]
    assert check_key("x", seen_it).ok
    assert check_key("x", missed_it).ok


def test_unknown_write_cannot_take_effect_before_it_was_invoked():
    h = [op(0, "write", 1, 0, 1), op(1, "read", None, 2, 3, output=2), op(2, "write", 2, 5, INF, status="info")]
    assert not check_key("x", h).ok


def test_failed_ops_are_ignored():
    h = [op(0, "write", 1, 0, 1), op(1, "write", 2, 2, 3, status="fail"), op(2, "read", None, 4, 5, output=1)]
    assert check_key("x", h).ok


def test_keys_are_independent():
    h = History([op(0, "write", 1, 0, 1, key="a"), op(1, "write", 9, 0, 1, key="b"),
                 op(2, "read", None, 2, 3, output=1, key="a"), op(3, "read", None, 2, 3, output=8, key="b")])
    r = check(h)
    assert not r.ok
    assert [k.key for k in r.failures] == ["b"]


# --- brute force oracle -----------------------------------------------------

def brute_force(ops):
    """Try every subset of unknown ops and every order consistent with real time."""
    ops = [o for o in ops if o.status != "fail" and not (o.kind == "read" and o.status == "info")]
    unknown = [o for o in ops if o.status == "info"]
    known = [o for o in ops if o.status != "info"]
    for r in range(len(unknown) + 1):
        for chosen in itertools.combinations(unknown, r):
            sub = known + list(chosen)
            for perm in itertools.permutations(sub):
                # real-time order: if a completed before b was invoked, a goes first
                pos = {o.id: k for k, o in enumerate(perm)}
                if any(a.complete < b.invoke and pos[a.id] > pos[b.id] for a in sub for b in sub):
                    continue
                state, good = MISSING, True
                for o in perm:
                    allowed, state = step(state, o)
                    if not allowed:
                        good = False
                        break
                if good:
                    return True
    return False


def random_history(rng, n):
    ops, t = [], 0.0
    for i in range(n):
        start = rng.uniform(0, 10)
        dur = rng.uniform(0.1, 4)
        kind = rng.choice(["write", "write", "read", "read", "cas"])
        status = rng.choices(["ok", "info", "fail"], [8, 2, 1])[0]
        if kind == "write":
            value, output = rng.randint(1, 3), None
        elif kind == "read":
            value, output = None, rng.choice([None, 1, 2, 3])
        else:
            value, output = [rng.choice([None, 1, 2, 3]), rng.randint(1, 3)], rng.random() < .5
        ops.append(op(i, kind, value, start, INF if status == "info" else start + dur, status, output))
    return ops


@pytest.mark.parametrize("seed", range(400))
def test_agrees_with_brute_force(seed):
    rng = random.Random(seed)
    ops = random_history(rng, rng.randint(1, 6))
    assert check_key("x", ops).ok == brute_force(ops)

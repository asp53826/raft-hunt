"""Finding 001: PySyncObj advances a follower's commit index over its own
unverified log when an append_entries carries no prevLogIdx (a snapshot
chunk), and then acknowledges a write the cluster never committed.

A scripted schedule, not a random one, so every step is visible:

  1. three nodes, leader L; commit x=1 through L
  2. cut L off; L is still leader until leaderFallbackTimeout, so it accepts
     put(x, "stale") at index k (term T) but can't commit it
  3. the other two elect M (term > T); M's no-op lands at index k, then
     put(x, "new") commits after it
  4. M compacts its log, so L is now behind M's first log index
  5. heal; M starts sending L its snapshot. Right after the first chunk is
     delivered, the connection drops (any network blip will do)
  6. L ticks: on that first chunk it set commitIndex = min(leader's commit,
     its own last index) = k, over its stale entry, applies it, and calls the
     "stale" writer's callback with SUCCESS

Run: PYTHONHASHSEED=0 python findings/001-pysyncobj-commit-from-snapshot-chunk/repro.py [--trace out.json]
Exit code 1 means the bug reproduced.
"""
import json
import os
import pickle
import sys

if os.environ.get("PYTHONHASHSEED") != "0":
    os.environ["PYTHONHASHSEED"] = "0"
    os.execv(sys.executable, [sys.executable, *sys.argv])

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from targets.pysyncobj.sim import FAIL_NAMES, Sim  # noqa: E402

out = []


def say(*a):
    line = " ".join(str(x) for x in a)
    out.append(line)
    print(line)


def leader(sim):
    ls = [n for n in sim.ids if sim.alive(n) and sim.nodes[n]._isLeader()]
    return max(ls, key=lambda n: sim.nodes[n].raftCurrentTerm) if ls else None


def put(sim, nid, key, value, opid, results):
    with sim.running(nid):
        sim.nodes[nid].put(key, value, opid, callback=lambda r, e: results.append((round(sim.world.now, 3), opid, FAIL_NAMES[e])))


def state(sim, nid):
    n = sim.nodes[nid]
    log = n._SyncObj__raftLog
    return {"term": n.raftCurrentTerm, "commit": n.raftCommitIndex, "applied": n.raftLastApplied,
            "log": [(e[1], e[2]) for e in log][-4:], "x": n._d.get("x")}


def main(trace_path=None):
    sim = Sim(seed=7, n=3, trace=True, conf_overrides={"leaderFallbackTimeout": 8.0})
    sim.boot()
    sim.run_until(3.0)
    L = leader(sim)
    results = []
    put(sim, L, "x", 1, 1, results)
    sim.run_until(4.0)
    say(f"[1] t={sim.world.now:.2f} leader {L}, committed x=1:", results, state(sim, L))

    others = [n for n in sim.ids if n != L]
    sim.partition([[L], others], silent=False)
    put(sim, L, "x", "stale", 2, results)
    sim.run_until(4.5)
    k = sim.nodes[L].raftLastApplied + 1
    say(f"[2] t={sim.world.now:.2f} {L} cut off but still leader; accepted put(x, 'stale') at index",
        state(sim, L)["log"][-1], "(not committed)")

    sim.run_until(7.0)
    M = leader(sim) if leader(sim) != L else max(others, key=lambda n: sim.nodes[n]._isLeader())
    M = next(n for n in others if sim.nodes[n]._isLeader())
    put(sim, M, "x", "new", 3, results)
    sim.run_until(8.0)
    say(f"[3] t={sim.world.now:.2f} {M} elected in term {sim.nodes[M].raftCurrentTerm}; committed x='new':",
        state(sim, M))

    with sim.running(M):
        sim.nodes[M].forceLogCompaction()
    sim.run_until(9.0)
    first = sim.nodes[M]._SyncObj__raftLog[0][1]
    say(f"[4] t={sim.world.now:.2f} {M} compacted its log; first index is now {first}; {L} still has",
        state(sim, L)["log"])

    # [5] heal, and cut L's connection the moment the first snapshot chunk lands
    cut = {"done": False}
    orig = Sim._on_deliver

    def deliver(self, data):
        src, dst, epoch, payload, mtype = data
        orig(self, data)
        if not cut["done"] and dst == L and src == M:
            msg = pickle.loads(payload)
            if "serialized" in msg:
                cut["done"] = True
                chunk = msg["serialized"]
                say(f"[5] t={self.world.now:.3f} {L} got snapshot chunk (first={chunk[1] if chunk else None}, "
                    f"last={chunk[2] if chunk else None}) with commit_index={msg['commit_index']}; connection drops now")
                say("    ", L, "after the chunk:", state(self, L))
                self.partition([[L], others], silent=False)

    Sim._on_deliver = deliver
    sim.heal()
    sim.run_until(16.0)
    Sim._on_deliver = orig

    st = state(sim, L)
    say(f"[6] t={sim.world.now:.2f} {L}:", st)
    say("    callbacks:", results)
    stale_ack = any(opid == 2 and code == "SUCCESS" for _, opid, code in results)
    cluster_x = sim.nodes[M]._d.get("x")
    say(f"    cluster (leader {M}) has x={cluster_x!r}; {L} has x={st['x']!r}")
    reproduced = stale_ack or st["x"] == "stale"
    if reproduced:
        say("REPRODUCED: put(x, 'stale') was never committed by the cluster, but", L,
            "applied it" + (" and told the client SUCCESS" if stale_ack else ""))
    else:
        say("not reproduced")
    if trace_path:
        with open(trace_path, "w") as f:
            json.dump({"narrative": out, "trace": sim.trace}, f)
    sim.cleanup()
    return 1 if reproduced else 0


if __name__ == "__main__":
    tp = sys.argv[sys.argv.index("--trace") + 1] if "--trace" in sys.argv else None
    sys.exit(main(tp))

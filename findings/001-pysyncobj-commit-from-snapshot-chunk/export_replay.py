"""Export finding 001's scripted schedule as step-by-step node states, for the
replay on the website. Same schedule as repro.py; after each step it records
every node's role, term, commit index, applied index, log (index, term, what)
and value of x, plus the client callbacks so far.

    PYTHONHASHSEED=0 python findings/001-pysyncobj-commit-from-snapshot-chunk/export_replay.py out.json
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

steps, acks = [], []


def what(cmd: bytes) -> str:
    if cmd[:1] == b"\x01":
        return "no-op"
    try:
        fn = pickle.loads(cmd[1:])
        args = fn[1] if isinstance(fn, tuple) and len(fn) > 1 else ()
        return f"x={args[1]!r}" if len(args) > 1 else "cmd"
    except Exception:
        return "cmd"


def snap(sim, title, text, focus=None):
    nodes = {}
    for nid in sim.ids:
        n = sim.nodes[nid]
        log = n._SyncObj__raftLog
        role = "leader" if n._isLeader() else "follower"
        nodes[nid] = {"role": role, "term": n.raftCurrentTerm, "commit": n.raftCommitIndex,
                      "applied": n.raftLastApplied, "x": n._d.get("x"),
                      "log": [[e[1], e[2], what(e[0])] for e in log],
                      "cut": sorted(o for o in sim.ids if o != nid and frozenset((nid, o)) in sim.blocked)}
    steps.append({"t": round(sim.world.now, 3), "title": title, "text": text, "focus": focus,
                  "nodes": nodes, "acks": list(acks)})


def leader(sim):
    ls = [n for n in sim.ids if sim.alive(n) and sim.nodes[n]._isLeader()]
    return max(ls, key=lambda n: sim.nodes[n].raftCurrentTerm) if ls else None


def put(sim, nid, value, opid):
    with sim.running(nid):
        sim.nodes[nid].put("x", value, opid, callback=lambda r, e: acks.append(
            {"t": round(sim.world.now, 3), "at": nid, "value": value, "result": FAIL_NAMES[e]}))


def main(out):
    sim = Sim(seed=7, n=3, conf_overrides={"leaderFallbackTimeout": 8.0})
    sim.boot()
    sim.run_until(3.0)
    L = leader(sim)
    others = [n for n in sim.ids if n != L]
    snap(sim, "Three nodes, one leader", f"{L} is the leader of term 1. Every node has the same log.", L)
    put(sim, L, 1, 1)
    sim.run_until(4.0)
    snap(sim, "x = 1 commits", f"A client writes x=1 through {L}. A majority stores it, it commits, every node applies it.", L)

    sim.partition([[L], others], silent=False)
    put(sim, L, "stale", 2)
    sim.run_until(4.5)
    snap(sim, f"{L} is cut off", f"The network splits {L} from the other two. {L} doesn't know it has lost the majority "
         f"yet, so it accepts x='stale' and appends it at the next index. It can't commit it: nobody else has it.", L)

    sim.run_until(7.0)
    M = next(n for n in others if sim.nodes[n]._isLeader())
    put(sim, M, "new", 3)
    sim.run_until(8.0)
    snap(sim, f"{M} takes over", f"The majority elects {M} in term 2. Its no-op lands at the same index as {L}'s stale "
         f"entry, and x='new' commits after it. This is normal Raft: {L}'s entry was never committed.", M)

    with sim.running(M):
        sim.nodes[M].forceLogCompaction()
    sim.run_until(9.0)
    snap(sim, f"{M} compacts its log", f"{M} replaces its old log entries with a snapshot. To catch {L} up later, it "
         f"will have to send that snapshot instead of individual entries.", M)

    orig, seen = Sim._on_deliver, {"chunk": False}

    def deliver(self, data):
        src, dst, epoch, payload, mtype = data
        orig(self, data)
        if not seen["chunk"] and dst == L and src == M and "serialized" in pickle.loads(payload):
            seen["chunk"] = True
            snap(self, "The bug: commit index from a snapshot chunk",
                 f"The partition heals and {M} starts sending {L} its snapshot. The first chunk carries {M}'s commit "
                 f"index but no log position to check against. {L} adopts it anyway, capped at the end of its own "
                 f"log, so its stale entry is now 'committed'. Then the connection drops.", L)
            self.partition([[L], others], silent=False)

    Sim._on_deliver = deliver
    sim.heal()
    sim.run_until(9.65)
    Sim._on_deliver = orig
    snap(sim, f"{L} applies and acknowledges it", f"{L} applies the entry it believes is committed. x becomes 'stale' "
         f"on {L}, and the client that wrote it is told SUCCESS. The cluster's value is 'new'. That write was never "
         f"committed and will be erased when the snapshot finally arrives.", L)
    with open(out, "w") as f:
        json.dump({"finding": "001", "library": "pysyncobj 0.3.17", "leader": L, "new_leader": M, "steps": steps}, f, indent=1)
    sim.cleanup()
    print(f"wrote {len(steps)} steps to {out}")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "replay-001.json")

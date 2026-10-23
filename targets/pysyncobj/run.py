"""Run seeded fault campaigns against PySyncObj.

    python -m targets.pysyncobj.run --seed 42            # one run, verbose
    python -m targets.pysyncobj.run --seeds 0:2000 -j 4  # a campaign
    python -m targets.pysyncobj.run --seed 42 --trace out.json

A run is: boot five nodes, let clients issue puts, gets and compare-and-sets
through the log while the nemesis injects faults, then heal everything,
restart whatever is down, let it settle, and have every client read every
key. The safety checks in sim.py run on every step; the client history is
checked for linearizability at the end.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from dataclasses import dataclass, field
from multiprocessing import Pool
from typing import Optional

if os.environ.get("PYTHONHASHSEED") != "0":
    # PySyncObj iterates sets of nodes; fixed string hashing keeps runs reproducible
    os.environ["PYTHONHASHSEED"] = "0"
    os.execv(sys.executable, [sys.executable, "-m", "targets.pysyncobj.run", *sys.argv[1:]])

from hunt.history import History  # noqa: E402
from hunt.linearizability import check  # noqa: E402
from targets.pysyncobj.sim import FAIL_NAMES, Sim, Violation  # noqa: E402

KEYS = ["a", "b", "c"]
# errors after which the command definitely was not and will not be applied
DEFINITE_FAIL = {"QUEUE_FULL", "MISSING_LEADER", "NOT_LEADER", "REQUEST_DENIED", "DISCARDED", "LEADER_CHANGED"}


@dataclass
class Profile:
    """How hard to push. Every number is drawn from the run's seed."""
    name: str = "default"
    nodes: int = 5
    duration: float = 60.0        # seconds of faults and traffic
    settle: float = 15.0          # fault-free time before the final reads
    clients: int = 4
    op_timeout: float = 4.0
    fault_gap: tuple = (1.0, 5.0)
    faults: dict = field(default_factory=lambda: {
        "partition": 3, "silent_partition": 3, "bridge": 1, "heal": 4, "crash": 2, "restart": 3,
        "pause": 2, "pause_leader": 2, "transfer": 2, "compact": 1,
    })
    conf: dict = field(default_factory=dict)


PROFILES = {
    "default": Profile(),
    "compaction": Profile(name="compaction", conf={"logCompactionMinEntries": 20, "logCompactionMinTime": 2},
                          faults={"partition": 3, "silent_partition": 2, "heal": 4, "crash": 3, "restart": 4,
                                  "pause_leader": 1, "compact": 3}),
    "crashy": Profile(name="crashy", faults={"crash": 5, "restart": 5, "partition": 1, "heal": 2, "pause_leader": 1}),
    "transfer": Profile(name="transfer", faults={"transfer": 6, "partition": 2, "silent_partition": 2, "heal": 3,
                                                 "pause_leader": 2, "crash": 1, "restart": 2}),
}


class Workload:
    def __init__(self, sim: Sim, history: History, profile: Profile):
        self.sim, self.h, self.p = sim, history, profile
        self.rng = sim.world.rng
        self.opid = 1000
        self.stopped = False
        self.home = {}

    def start(self):
        for i in range(self.p.clients):
            c = f"c{i + 1}"
            self.sim.schedule(self.rng.uniform(0.5, 1.5), "call", lambda c=c: self.next(c))

    def pick_node(self, client):
        alive = [n for n in self.sim.ids if self.sim.alive(n)]
        if not alive:
            return None
        node = self.home.get(client)
        if node not in alive or self.rng.random() < 0.1:
            node = self.rng.choice(alive)
            self.home[client] = node
        return node

    def next(self, client, final_key=None):
        if self.stopped and final_key is None:
            return
        nid = self.pick_node(client)
        if nid is None:
            self.sim.schedule(self.sim.world.now + 0.5, "call", lambda: self.next(client, final_key))
            return
        node = self.sim.nodes[nid]
        self.opid += 1
        opid = self.opid
        if final_key is not None:
            kind, key = "read", final_key
        else:
            r = self.rng.random()
            kind = "write" if r < 0.45 else "read" if r < 0.85 else "cas"
            key = self.rng.choice(KEYS)
        if kind == "write":
            value = opid
        elif kind == "cas":
            prior = [o.value for o in self.h.ops if o.key == key and o.kind == "write"]
            value = [self.rng.choice(prior) if prior and self.rng.random() < .7 else None, opid]
        else:
            value = None
        op = self.h.invoke(client, kind, key, value, self.sim.world.now)
        op.note = nid
        done = {"v": False}

        def callback(res, err):
            if done["v"]:
                return
            done["v"] = True
            name = FAIL_NAMES.get(err, str(err))
            t = self.sim.world.now
            if name == "SUCCESS":
                History.ok(op, t, res if kind != "write" else None)
            elif name in DEFINITE_FAIL:
                History.fail(op, t, name)
            else:
                History.info(op, name)
            self.sim.log("op", op.id, client, nid, kind, key, value, name, res if kind != "write" else None)
            if final_key is None:
                self.sim.schedule(t + self.rng.uniform(0, 0.3), "call", lambda: self.next(client))

        def timeout():
            if done["v"]:
                return
            done["v"] = True
            History.info(op, "client-timeout")
            self.sim.log("op", op.id, client, nid, kind, key, value, "TIMEOUT", None)
            if final_key is None:
                self.sim.schedule(self.sim.world.now + self.rng.uniform(0, 0.3), "call", lambda: self.next(client))

        with self.sim.running(nid):
            if kind == "write":
                node.put(key, value, opid, callback=callback)
            elif kind == "read":
                node.get(key, opid, callback=callback)
            else:
                node.cas(key, value[0], value[1], opid, callback=callback)
        self.sim.schedule(self.sim.world.now + self.p.op_timeout, "call", timeout)


class Nemesis:
    def __init__(self, sim: Sim, profile: Profile):
        self.sim, self.p = sim, profile
        self.rng = sim.world.rng

    def leader(self) -> Optional[str]:
        ls = [n for n in self.sim.ids if self.sim.alive(n) and self.sim.nodes[n]._isLeader()]
        return max(ls, key=lambda n: self.sim.nodes[n].raftCurrentTerm) if ls else None

    def start(self):
        self.sim.schedule(self.rng.uniform(*self.p.fault_gap), "call", self.fire)

    def fire(self):
        if self.sim.world.now >= self.p.duration:
            return
        kinds, weights = zip(*self.p.faults.items())
        getattr(self, "f_" + self.rng.choices(kinds, weights)[0])()
        self.sim.schedule(self.sim.world.now + self.rng.uniform(*self.p.fault_gap), "call", self.fire)

    def split(self):
        ids = self.sim.ids[:]
        self.rng.shuffle(ids)
        k = self.rng.choice([1, len(ids) // 2])
        return [ids[:k], ids[k:]]

    def f_partition(self):
        self.sim.partition(self.split(), silent=False)

    def f_silent_partition(self):
        self.sim.partition(self.split(), silent=True)

    def f_bridge(self):
        # two halves that can't see each other, and one node that sees both
        ids = self.sim.ids[:]
        self.rng.shuffle(ids)
        bridge, rest = ids[0], ids[1:]
        a, b = rest[: len(rest) // 2], rest[len(rest) // 2:]
        self.sim.cut([(x, y) for x in a for y in b], silent=self.rng.random() < .5, label={"bridge": bridge, "a": a, "b": b})

    def f_heal(self):
        self.sim.heal()

    def f_crash(self):
        alive = [n for n in self.sim.ids if self.sim.alive(n)]
        if alive:
            self.sim.crash(self.rng.choice(alive))

    def f_restart(self):
        dead = [n for n in self.sim.ids if not self.sim.alive(n)]
        if dead:
            self.sim.restart(self.rng.choice(dead))

    def f_pause(self):
        alive = [n for n in self.sim.ids if self.sim.alive(n)]
        if alive:
            self.sim.pause(self.rng.choice(alive), self.rng.uniform(0.5, 6.0))

    def f_pause_leader(self):
        ld = self.leader()
        if ld:
            self.sim.pause(ld, self.rng.uniform(1.0, 8.0))

    def f_transfer(self):
        ld = self.leader()
        if not ld:
            return
        target = self.rng.choice([None] + [n for n in self.sim.ids if n != ld])
        self.sim.log("transfer", ld, target)
        with self.sim.running(ld):
            self.sim.nodes[ld].transferLeadership(target, callback=lambda r, e: None)

    def f_compact(self):
        alive = [n for n in self.sim.ids if self.sim.alive(n)]
        if alive:
            nid = self.rng.choice(alive)
            self.sim.log("compact", nid)
            with self.sim.running(nid):
                self.sim.nodes[nid].forceLogCompaction()


def run(seed: int, profile: str = "default", trace: bool = False, keep_history: Optional[str] = None) -> dict:
    p = PROFILES[profile]
    t0 = time.time()
    sim = Sim(seed, n=p.nodes, conf_overrides=p.conf, trace=trace)
    h = History()
    wl, nem = Workload(sim, h, p), Nemesis(sim, p)
    result = {"seed": seed, "profile": profile}
    try:
        sim.boot()
        wl.start()
        nem.start()
        sim.run_until(p.duration)
        # settle: no more faults, everything back up, then read every key
        wl.stopped = True
        sim.heal()
        for nid in sim.ids:
            sim.restart(nid)
        sim.run_until(p.duration + p.settle)
        for i, key in enumerate(KEYS * 2):
            c = f"f{i}"
            sim.schedule(sim.world.now + 0.05 * i, "call", lambda c=c, key=key: wl.next(c, final_key=key))
        sim.run_until(p.duration + p.settle + p.op_timeout + 1)
        result["violation"] = None
    except Violation as v:
        result["violation"] = {"kind": v.kind, "detail": v.detail, "t": round(sim.world.now, 4)}
    except Exception as e:  # a crash inside the library is a finding too
        import traceback
        result["violation"] = {"kind": "exception", "detail": f"{type(e).__name__}: {e}",
                               "trace": traceback.format_exc()[-2000:], "t": round(sim.world.now, 4)}
    lin = check(h) if result["violation"] is None else None
    if lin is not None and not lin.ok:
        f = lin.failures[0]
        result["violation"] = {"kind": "linearizability", "detail": f"key {f.key}: no legal order; stuck on op {f.stuck_on}",
                               "prefix": f.prefix, "stuck_on": f.stuck_on}
    ops = h.ops
    result.update({
        "ok": result["violation"] is None,
        "ops": len(ops),
        "ok_ops": sum(o.status == "ok" for o in ops),
        "fail_ops": sum(o.status == "fail" for o in ops),
        "info_ops": sum(o.status == "info" for o in ops),
        "stats": sim.stats,
        "terms": max(sim.leader_of_term, default=0),
        "sim_time": round(sim.world.now, 3),
        "wall": round(time.time() - t0, 3),
    })
    if trace:
        result["trace"] = sim.trace
        result["trace_sha"] = hashlib.sha256(json.dumps(sim.trace).encode()).hexdigest()[:16]
    if keep_history:
        h.dump(keep_history)
    sim.cleanup()
    return result


def _campaign_one(args):
    seed, profile = args
    r = run(seed, profile)
    return {k: v for k, v in r.items() if k != "trace"}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", type=int)
    ap.add_argument("--seeds", help="start:end")
    ap.add_argument("--profile", default="default", choices=sorted(PROFILES))
    ap.add_argument("-j", type=int, default=os.cpu_count() or 1)
    ap.add_argument("--trace", help="write the full event trace of --seed to this file")
    ap.add_argument("--history", help="write the client history of --seed to this file (JSONL)")
    ap.add_argument("--out", default="runs", help="campaign output directory")
    a = ap.parse_args()
    if a.seed is not None:
        r = run(a.seed, a.profile, trace=bool(a.trace), keep_history=a.history)
        if a.trace:
            with open(a.trace, "w") as f:
                json.dump(r, f)
        print(json.dumps({k: v for k, v in r.items() if k != "trace"}, indent=2))
        sys.exit(0 if r["ok"] else 1)
    lo, hi = map(int, a.seeds.split(":"))
    os.makedirs(a.out, exist_ok=True)
    path = os.path.join(a.out, f"pysyncobj-{a.profile}-{lo}-{hi}.jsonl")
    bad = 0
    t0 = time.time()
    with Pool(a.j) as pool, open(path, "w") as out:
        for i, r in enumerate(pool.imap_unordered(_campaign_one, [(s, a.profile) for s in range(lo, hi)])):
            out.write(json.dumps(r) + "\n")
            out.flush()
            if not r["ok"]:
                bad += 1
                print(f"seed {r['seed']}: {r['violation']['kind']}: {r['violation']['detail'][:160]}", flush=True)
            if (i + 1) % 100 == 0:
                print(f"  {i + 1}/{hi - lo} runs, {bad} failing, {time.time() - t0:.0f}s", flush=True)
    print(f"done: {hi - lo} runs, {bad} failing -> {path}")


if __name__ == "__main__":
    main()

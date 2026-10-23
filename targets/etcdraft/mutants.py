"""Planted bugs for etcd-io/raft, to show the harness catches what it should.

    python3 mutants.py apply <name> <dir>     # used by prepare.sh (MUTANT=<name>)
    python -m targets.etcdraft.mutants --seeds 0:200 -j 4

Each mutant is one source edit to the seeded copy of the library. The driver
runs the same seeds on stock and on each mutant and credits a mutant only
with violation kinds the stock run of that seed didn't have.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from collections import Counter

MUTANTS = {
    "double-vote": {
        "what": "a node may grant a second vote in the same term",
        "file": "raft.go",
        "edit": ("			(r.Vote == None && r.lead == None) ||\n",
                 "			(r.lead == None) ||\n"),
    },
    "no-up-to-date-check": {
        "what": "voters ignore whether the candidate's log is up to date (election restriction removed)",
        "file": "log.go",
        "edit": ("	return their.term > our.term || their.term == our.term && their.index >= our.index\n",
                 "	_ = our\n	return true\n"),
    },
    "figure-8": {
        "what": "a leader commits by counting replicas even for entries from earlier terms (Raft paper, figure 8)",
        "file": "log.go",
        "edit": ("	if at.term != 0 && at.index > l.committed && l.matchTerm(at) {\n",
                 "	if at.term != 0 && at.index > l.committed && at.index <= l.lastIndex() {\n"),
    },
    "minority-commit": {
        "what": "the commit index is the median, not the majority, of match indexes (half the cluster suffices)",
        "file": "quorum/majority.go",
        "edit": ("	pos := n - (n/2 + 1)\n", "	pos := n - (n / 2)\n"),
    },
    "read-index-no-quorum": {
        "what": "ReadIndex answers at the leader's commit index without confirming leadership with a quorum",
        "file": "raft.go",
        "edit": ("		r.readOnly.addRequest(r.raftLog.committed, m)\n"
                 "		// The local node automatically acks the request.\n"
                 "		r.readOnly.recvAck(r.id, r.readOnly.heartbeatCtx())\n"
                 "		r.bcastHeartbeat()\n",
                 "		if resp := r.responseToReadIndexReq(m, r.raftLog.committed); resp.GetTo() != None {\n"
                 "			r.send(resp)\n"
                 "		}\n"),
    },
}


def apply(name: str, root: str) -> None:
    m = MUTANTS[name]
    path = os.path.join(root, m["file"])
    src = open(path).read()
    old, new = m["edit"]
    if src.count(old) != 1:
        sys.exit(f"mutant {name}: edit site found {src.count(old)} times in {m['file']}, expected 1")
    open(path, "w").write(src.replace(old, new))


def campaign(mutant: str | None, seeds: str, profile: str, jobs: int, out: str) -> dict[int, set]:
    env = dict(os.environ, MUTANT=mutant or "")
    subprocess.run([sys.executable, "-m", "targets.etcdraft.run", "--seeds", seeds, "--profile", profile,
                    "-j", str(jobs), "--out", out], env=env, check=True, stdout=subprocess.DEVNULL)
    lo, hi = seeds.split(":")
    tag = mutant or "stock"
    rows = [json.loads(l) for l in open(os.path.join(out, f"etcdraft-{tag}-{profile}-{lo}-{hi}.jsonl"))]
    return {r["seed"]: {v["kind"] for v in r["violations"]} for r in rows}


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "apply":
        apply(sys.argv[2], sys.argv[3])
        return
    ap = argparse.ArgumentParser()
    ap.add_argument("--seeds", default="0:200")
    ap.add_argument("--profile", default="default")
    ap.add_argument("-j", type=int, default=os.cpu_count() or 1)
    ap.add_argument("--only", nargs="*")
    ap.add_argument("--json")
    ap.add_argument("--out", default="runs/mutants")
    a = ap.parse_args()
    base = campaign(None, a.seeds, a.profile, a.j, a.out)
    print(f"stock etcd raft: {sum(bool(v) for v in base.values())} of {len(base)} runs flag something: "
          f"{dict(Counter(k for v in base.values() for k in v))}")
    results = {}
    for name in a.only or MUTANTS:
        got = campaign(name, a.seeds, a.profile, a.j, a.out)
        new = {s: k - base.get(s, set()) for s, k in got.items()}
        caught = sorted(s for s, k in new.items() if k)
        kinds = Counter(k for s in caught for k in new[s])
        results[name] = {"what": MUTANTS[name]["what"], "runs": len(got), "caught": len(caught),
                         "first_seed": caught[0] if caught else None, "kinds": dict(kinds)}
        print(f"{name:22} caught in {len(caught):3}/{len(got)} runs, first at seed {caught[0] if caught else '-'}: {dict(kinds)}")
    if a.json:
        with open(a.json, "w") as f:
            json.dump({"profile": a.profile, "seeds": a.seeds, "mutants": results}, f, indent=2)


if __name__ == "__main__":
    main()

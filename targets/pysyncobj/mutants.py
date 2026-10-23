"""Plant known Raft bugs in a copy of PySyncObj and measure whether, and how
fast, the harness catches each one. A harness that finds nothing in a real
library only means something if it does find these.

    python -m targets.pysyncobj.mutants --seeds 0:200 -j 4

Each mutant is one source edit in syncobj.py. For each, the campaign runs
the given seeds on the mutated copy and reports how many runs caught it, the
first seed that did, and which checks fired. Violations that the unmutated
library also produces (findings 001-003) are subtracted per seed, so a
mutant only gets credit for what it changed.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
from collections import Counter

import pysyncobj

MUTANTS = {
    "double-vote": {
        "what": "a node may vote twice in the same term (votedFor check removed)",
        "edit": ("                    if self.__votedForNodeId is not None:\n                        return\n", ""),
    },
    "forget-vote-on-restart": {
        "what": "votedFor isn't restored from the journal on restart (the bug fixed by PR #202)",
        "edit": ("        self.__votedForNodeId = self.__raftLog.getVotedForNodeId()\n",
                 "        self.__votedForNodeId = None\n"),
    },
    "figure-8": {
        "what": "a leader commits entries from earlier terms by counting replicas (Raft paper, figure 8)",
        "edit": ("                if commitTerm != self.__raftCurrentTerm:\n                    continue\n", ""),
    },
    "no-up-to-date-check": {
        "what": "voters ignore the candidate's last log term (election restriction removed)",
        "edit": ("                    if lastLogTerm < self.__getCurrentLogTerm():\n                        return\n", ""),
    },
    "minority-commit": {
        # (an earlier version changed <= to <, which is a no-op for odd cluster
        # sizes: count < 2.5 and count <= 2.5 agree on integers. It "survived"
        # every run because it planted nothing.)
        "what": "the leader commits with a minority: 2 of 5 nodes, or 1 of 3",
        "edit": ("                if count <= (len(self.__otherNodes) + 1) / 2:\n                    break\n",
                 "                if count <= (len(self.__otherNodes) + 1) // 2 - 1:\n                    break\n"),
    },
    "stale-term-append": {
        "what": "followers accept append_entries from a leader of an older term",
        "edit": ("        if message['type'] == 'append_entries' and message['term'] >= self.__raftCurrentTerm:\n",
                 "        if message['type'] == 'append_entries':\n"),
    },
}


def mutate(name: str, base: str | None) -> str:
    src_pkg = os.path.join(base, "pysyncobj") if base else os.path.dirname(pysyncobj.__file__)
    root = tempfile.mkdtemp(prefix=f"mutant-{name}-")
    shutil.copytree(src_pkg, os.path.join(root, "pysyncobj"))
    path = os.path.join(root, "pysyncobj", "syncobj.py")
    src = open(path).read()
    old, new = MUTANTS[name]["edit"]
    if src.count(old) != 1:
        raise SystemExit(f"mutant {name}: edit site found {src.count(old)} times, expected 1")
    open(path, "w").write(src.replace(old, new))
    return root


BASELINE = {"failed-but-committed"}   # finding 003, present in the unmutated library


def campaign(pythonpath: str | None, seeds: str, profile: str, jobs: int, out: str) -> dict[int, set]:
    env = dict(os.environ, PYTHONHASHSEED="0")
    if pythonpath:
        env["PYTHONPATH"] = pythonpath
    subprocess.run([sys.executable, "-m", "targets.pysyncobj.run", "--seeds", seeds, "--profile", profile,
                    "-j", str(jobs), "--out", out], env=env, check=True, stdout=subprocess.DEVNULL)
    lo, hi = seeds.split(":")
    rows = [json.loads(l) for l in open(os.path.join(out, f"pysyncobj-{profile}-{lo}-{hi}.jsonl"))]
    return {r["seed"]: {v["kind"] for v in r["violations"]} - BASELINE for r in rows}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seeds", default="0:200")
    ap.add_argument("--profile", default="crashy")
    ap.add_argument("-j", type=int, default=os.cpu_count() or 1)
    ap.add_argument("--only", nargs="*")
    ap.add_argument("--json", help="write results here")
    ap.add_argument("--base", help="directory containing a pysyncobj package to mutate (e.g. with findings 001/002 "
                                   "fixed, so their violations can't be credited to a mutant)")
    a = ap.parse_args()
    out = tempfile.mkdtemp(prefix="mutants-")
    base = campaign(a.base, a.seeds, a.profile, a.j, os.path.join(out, "baseline"))
    print(f"baseline (unmutated{' ' + a.base if a.base else ''}): {sum(bool(v) for v in base.values())} of {len(base)} runs flag something "
          f"besides finding 003: {dict(Counter(k for v in base.values() for k in v))}")
    results = {}
    for name in a.only or MUTANTS:
        root = mutate(name, a.base)
        got = campaign(root, a.seeds, a.profile, a.j, os.path.join(out, name))
        new = {s: kinds - base.get(s, set()) for s, kinds in got.items()}
        caught = sorted(s for s, k in new.items() if k)
        kinds = Counter(k for s in caught for k in new[s])
        results[name] = {"what": MUTANTS[name]["what"], "runs": len(got), "caught": len(caught),
                         "first_seed": caught[0] if caught else None, "kinds": dict(kinds)}
        print(f"{name:24} caught in {len(caught):3}/{len(got)} runs, first at seed {caught[0] if caught else '-'}: "
              f"{dict(kinds)}")
        shutil.rmtree(root, ignore_errors=True)
    if a.json:
        with open(a.json, "w") as f:
            json.dump({"profile": a.profile, "seeds": a.seeds, "mutants": results}, f, indent=2)


if __name__ == "__main__":
    main()

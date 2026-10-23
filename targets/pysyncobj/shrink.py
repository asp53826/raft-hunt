"""Shrink a failing run to the smallest fault plan that still fails the same way.

    python -m targets.pysyncobj.shrink --seed 1234 --profile default [--kind committed-entry-changed]
    python -m targets.pysyncobj.shrink --seed 1234 --out findings/xyz/shrunk.json

Delta debugging (Zeller's ddmin) over the list of planned faults: try removing
chunks, keep any smaller plan that still produces a violation of the same
kind, and repeat at finer granularity until no single fault can be removed.
The workload and network have their own random streams, so dropping a fault
changes as little else as possible; every candidate is still a full,
deterministic rerun.
"""
from __future__ import annotations

import argparse
import json
import os
import sys

if os.environ.get("PYTHONHASHSEED") != "0":
    os.environ["PYTHONHASHSEED"] = "0"
    os.execv(sys.executable, [sys.executable, "-m", "targets.pysyncobj.shrink", *sys.argv[1:]])

from targets.pysyncobj.run import run  # noqa: E402


def fails(seed, profile, kind, plan, cache):
    key = json.dumps(plan, sort_keys=True)
    if key not in cache:
        r = run(seed, profile, schedule=plan)
        cache[key] = any(v["kind"] == kind for v in r["violations"])
    return cache[key]


def ddmin(seed, profile, kind, plan, log=print):
    cache: dict = {}
    n = 2
    tries = 0
    while len(plan) >= 2:
        chunk = max(1, len(plan) // n)
        subsets = [plan[i:i + chunk] for i in range(0, len(plan), chunk)]
        reduced = False
        for i, sub in enumerate(subsets):
            complement = [e for j, s in enumerate(subsets) if j != i for e in s]
            for cand in (sub, complement):
                tries += 1
                if cand and len(cand) < len(plan) and fails(seed, profile, kind, cand, cache):
                    plan = cand
                    n = max(n - 1, 2) if cand is complement else 2
                    reduced = True
                    log(f"  {len(plan):3} faults still fail ({tries} reruns)")
                    break
            if reduced:
                break
        if not reduced:
            if n >= len(plan):
                break
            n = min(len(plan), n * 2)
    # the empty plan: is the fault needed at all?
    if fails(seed, profile, kind, [], cache):
        plan = []
    return plan, tries


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", type=int, required=True)
    ap.add_argument("--profile", default="default")
    ap.add_argument("--kind", help="violation kind to preserve (default: the run's first)")
    ap.add_argument("--out", help="write the shrunk plan, its summary and full trace here")
    a = ap.parse_args()
    r = run(a.seed, a.profile)
    if not r["violations"]:
        sys.exit(f"seed {a.seed} ({a.profile}) doesn't fail")
    kind = a.kind or r["violations"][0]["kind"]
    plan = r["schedule"]
    print(f"seed {a.seed}: {kind} with {len(plan)} planned faults; shrinking")
    small, tries = ddmin(a.seed, a.profile, kind, plan)
    final = run(a.seed, a.profile, schedule=small, trace=True)
    v = next(v for v in final["violations"] if v["kind"] == kind)
    print(f"shrunk {len(plan)} -> {len(small)} faults in {tries} reruns")
    for e in small:
        print(f"  t={e['t']:7.3f}  {e['kind']}")
    print(f"still fails: {v['kind']} at t={v.get('t')}: {v['detail']}")
    if a.out:
        os.makedirs(os.path.dirname(a.out) or ".", exist_ok=True)
        with open(a.out, "w") as f:
            json.dump({"seed": a.seed, "profile": a.profile, "kind": kind, "original_faults": len(plan),
                       "schedule": small, "violation": v, "trace": final["trace"]}, f)
        print("wrote", a.out)


if __name__ == "__main__":
    main()

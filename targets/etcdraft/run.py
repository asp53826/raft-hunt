"""Campaigns against etcd-io/raft. The Go simulator (main.go) does the run and
the per-step safety checks; this wrapper builds it, runs seeds in parallel,
and checks each client history for linearizability with hunt/.

    python -m targets.etcdraft.run --seed 1
    python -m targets.etcdraft.run --seeds 0:1000 -j 4 --profile lossy
    MUTANT=figure-8 python -m targets.etcdraft.run --seeds 0:200   # planted bug
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from multiprocessing import Pool

from hunt.history import INF, History, Op
from hunt.linearizability import check

HERE = os.path.dirname(os.path.abspath(__file__))


def build(mutant: str | None = None) -> str:
    """Prepare the seeded etcd raft copy (optionally with a planted bug) and build."""
    tag = mutant or "stock"
    third = os.path.join("third_party", f"raft-{tag}")
    env = dict(os.environ, DST=third, MUTANT=mutant or "")
    subprocess.run(["sh", "prepare.sh"], cwd=HERE, env=env, check=True, stdout=subprocess.DEVNULL)
    # point the module at this copy for the build only
    subprocess.run(["go", "mod", "edit", f"-replace=go.etcd.io/raft/v3=./{third}"], cwd=HERE, check=True)
    out = os.path.join(HERE, "bin", f"etcdsim-{tag}")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    try:
        subprocess.run(["go", "build", "-o", out, "."], cwd=HERE, check=True)
    finally:
        subprocess.run(["go", "mod", "edit", "-replace=go.etcd.io/raft/v3=./third_party/raft"], cwd=HERE, check=True)
    return out


def analyse(r: dict) -> dict:
    ops = []
    for o in r.get("history") or []:
        ops.append(Op(id=o["id"], client=o["client"], kind=o["kind"], key=o["key"],
                      value=o["value"], invoke=o["invoke"],
                      complete=INF if o["complete"] is None else o["complete"],
                      status=o["status"], output=o["output"], note=o["note"]))
    h = History(ops)
    found = list(r["violations"])
    agreed = {int(k): v for k, v in (r.get("agreed") or {}).items()}
    for o, raw in zip(ops, r.get("history") or []):
        if o.status == "ok" and o.kind in ("write", "cas") and raw["cmd"] not in agreed:
            found.append({"kind": "acked-not-committed", "t": o.complete,
                          "detail": f"op {o.id} ({o.kind} {o.key}) acknowledged but no majority applied command {raw['cmd']}"})
        if o.status == "fail" and raw["cmd"] in agreed:
            found.append({"kind": "failed-but-committed", "t": o.complete,
                          "detail": f"op {o.id} ({o.kind} {o.key}) failed with {o.note!r} but was committed at index "
                                    f"{agreed[raw['cmd']]}"})
    res = {k: v for k, v in r.items() if k not in ("history", "agreed", "trace")}
    try:
        lin = check(h)
        if not lin.ok:
            f = lin.failures[0]
            found.append({"kind": "linearizability", "t": None,
                          "detail": f"key {f.key}: no legal order; stuck on op {f.stuck_on}", "stuck_on": f.stuck_on})
    except RuntimeError as e:
        res["linearizability"] = f"undecided: {e}"
    res["violations"] = found
    res["ok"] = not found
    res["ops"] = len(ops)
    res["ok_ops"] = sum(o.status == "ok" for o in ops)
    res["info_ops"] = sum(o.status == "info" for o in ops)
    return res


def run_one(args):
    binary, seed, profile, extra = args
    t0 = time.time()
    p = subprocess.run([binary, "-seed", str(seed), "-profile", profile, *extra], capture_output=True, text=True)
    if p.returncode != 0:
        return {"seed": seed, "profile": profile, "ok": False,
                "violations": [{"kind": "exception", "detail": p.stderr[-2000:], "t": None}]}
    res = analyse(json.loads(p.stdout))
    res["wall"] = round(time.time() - t0, 3)
    return res


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", type=int)
    ap.add_argument("--seeds")
    ap.add_argument("--profile", default="default")
    ap.add_argument("-j", type=int, default=os.cpu_count() or 1)
    ap.add_argument("--out", default="runs")
    ap.add_argument("--trace", help="with --seed: write the full result with event trace here")
    a = ap.parse_args()
    mutant = os.environ.get("MUTANT") or None
    binary = build(mutant)
    tag = mutant or "stock"
    if a.seed is not None:
        if a.trace:
            p = subprocess.run([binary, "-seed", str(a.seed), "-profile", a.profile, "-trace"], capture_output=True, text=True, check=True)
            open(a.trace, "w").write(p.stdout)
        r = run_one((binary, a.seed, a.profile, []))
        print(json.dumps(r, indent=2))
        sys.exit(0 if r["ok"] else 1)
    lo, hi = map(int, a.seeds.split(":"))
    os.makedirs(a.out, exist_ok=True)
    path = os.path.join(a.out, f"etcdraft-{tag}-{a.profile}-{lo}-{hi}.jsonl")
    bad, t0 = 0, time.time()
    with Pool(a.j) as pool, open(path, "w") as out:
        for i, r in enumerate(pool.imap_unordered(run_one, [(binary, s, a.profile, []) for s in range(lo, hi)])):
            out.write(json.dumps(r) + "\n")
            out.flush()
            if not r["ok"]:
                bad += 1
                v = r["violations"][0]
                print(f"seed {r['seed']}: {v['kind']}: {v['detail'][:160]}", flush=True)
            if (i + 1) % 100 == 0:
                print(f"  {i + 1}/{hi - lo} runs, {bad} failing, {time.time() - t0:.0f}s", flush=True)
    print(f"done: {hi - lo} runs, {bad} failing -> {path}")


if __name__ == "__main__":
    main()

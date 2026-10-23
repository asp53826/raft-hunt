# raft-hunt

Deterministic simulation testing pointed at Raft libraries people actually ship.

Each target library runs inside a simulator that owns its clock, its network
and its randomness. A seed picks a fault schedule (partitions, message loss,
reordering, duplication, crashes and restarts) and a client workload. After
every step the harness checks Raft's safety properties, and at the end it
checks the client history for linearizability. A failing seed replays
exactly, and gets shrunk to the smallest schedule that still fails.

> **Status: in progress.** This README is updated as the work lands; the
> commit history is the lab notebook.

## Findings so far

| # | library | what breaks | evidence | fix |
|---|---|---|---|---|
| [001](findings/001-pysyncobj-commit-from-snapshot-chunk/) | PySyncObj 0.3.17 | A deposed leader adopts the new leader's commit index from a snapshot message with no log check, then **applies and acknowledges (SUCCESS) a write the cluster never committed** | standalone repro on stock pysyncobj (2 message shapes); scripted 3-node schedule; 13 of 400 random runs | [+9/−1](findings/001-pysyncobj-commit-from-snapshot-chunk/fix.diff), verified: 13 → 0 runs, same client throughput |
| [002](findings/002-pysyncobj-snapshot-none-storm/) | PySyncObj 0.3.17 | While a forked log compaction runs, the leader busy-loops resending `serialized=None` to lagging followers: **48k–82k messages in one 100 ms tick**, ~7k/s over real TCP | in-process measurement with real fork; 3 nodes over real TCP | [+5](findings/002-pysyncobj-snapshot-none-storm/fix.diff), verified: 1 message per tick |
| [003](findings/003-pysyncobj-leader-changed-but-committed/) | PySyncObj 0.3.17 | `LEADER_CHANGED` is documented as "without command commit" but **the command can still commit**, so retrying applies it twice | standalone repro on stock pysyncobj; 192 ops in 117 of 400 runs | [docs](findings/003-pysyncobj-leader-changed-but-committed/fix.diff) or return `UNKNOWN_OUTCOME` |

None of these has been reported upstream yet. Each folder has the analysis,
the reproductions, the patch and a draft issue.

### With fixes 001 and 002 applied

4,000 more runs (seeds 1000–1999 of each profile: default, crashy, transfer,
compaction), about 1.59 million acknowledged client operations: no safety
violation of any kind. The only flags are finding 003 (by design, 878 ops)
and 2 histories the linearizability checker couldn't decide within 60 million
search steps (transfer 1836, compaction 1244; both have long-overlapping
late-acknowledged ops). Those 2 are reported as undecided, not as passing.

### Mistakes the harness made along the way

Kept here because they're how the results got trustworthy:

- It treated `LEADER_CHANGED` as a definite failure, which produced 7 false
  linearizability failures in the first campaign. Reading why led to 003.
- Its virtual clock didn't advance inside an event, so a PySyncObj loop that
  stops after 0.1 s of wall time never stopped (14 hung seeds). Sends now
  cost time. Reading why the loop spun led to 002.

## Targets

| library | language | why | status |
|---|---|---|---|
| [PySyncObj](https://github.com/bakwc/PySyncObj) 0.3.17 | Python | Used by Patroni's Raft mode; lighter test suite; recent changes to persistence and leadership transfer | simulator done, 3 findings, campaigns continuing |
| [etcd-io/raft](https://github.com/etcd-io/raft) v3.7.0 | Go | The reference implementation. The control: if planted bugs aren't caught here, the harness is the problem | planned |
| [little_raft](https://github.com/andreev-io/little-raft) 0.2.0 | Rust | Small, rarely tested | planned |
| an npm Raft library | JavaScript | Lightly tested ecosystem | planned |

## What counts as a bug

A finding has to break something the library promises, reproduce from a seed,
and survive shrinking. The checks:

- **Election safety:** at most one leader per term.
- **Log matching / state machine safety:** no two nodes apply different
  commands at the same log index.
- **Durability:** a command acknowledged as committed is never lost, including
  across crash and restart with the library's own on-disk journal.
- **Linearizability:** the client history of a replicated key-value register
  (writes, compare-and-set and reads that go through the log) has a legal
  sequential order.

Reads that a library documents as possibly stale are not tested as
linearizable. Behaviour that only fails under a fault model the library
explicitly does not claim to handle (for example disk corruption) is reported
separately, not as a bug.

## Running it

```sh
python -m venv .venv && .venv/bin/pip install pytest pysyncobj==0.3.17
.venv/bin/python -m pytest                                    # the checker's own tests
.venv/bin/python -m targets.pysyncobj.run --seed 1            # one run, JSON summary
.venv/bin/python -m targets.pysyncobj.run --seeds 0:400 -j 4  # a campaign
.venv/bin/python -m targets.pysyncobj.run --seed 1 --trace t.json   # full event trace
```

Profiles (`--profile`): `default`, `crashy`, `transfer` (leadership transfers),
`compaction` (log compaction every 2 s / 20 entries).

## Layout

```
hunt/            shared: history format, linearizability checker, invariants
targets/<lib>/   one simulator adapter per library
findings/        one write-up per finding, with the seed and the shrunk schedule
docs/            method notes
```

## License

MIT. Each target library keeps its own license; none of their code is vendored
here, they're installed from their package registries at pinned versions.

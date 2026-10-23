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

## Targets

| library | language | why | status |
|---|---|---|---|
| [PySyncObj](https://github.com/bakwc/PySyncObj) 0.3.17 | Python | Used by Patroni's Raft mode; lighter test suite; recent changes to persistence and leadership transfer | simulator in progress |
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

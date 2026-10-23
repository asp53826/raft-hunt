# 001 · PySyncObj acknowledges a write the cluster never committed

| | |
|---|---|
| library | [PySyncObj](https://github.com/bakwc/PySyncObj) 0.3.17 (= `master` at 6962777) |
| affected | every release: the unchecked commit-index update is in the first commit (2016, da0d608); #166 (2022) changed it to be monotonic but kept it unconditional |
| property | state machine safety, durability of acknowledgements |
| found by | seed 1 of the first default-profile campaign, harness at 45250c4 (`committed-entry-changed` at t=42.068) |
| reproductions | [`unit_repro.py`](unit_repro.py) (pysyncobj only, no simulator), [`repro.py`](repro.py) (scripted 3-node schedule) |
| proposed fix | [`fix.diff`](fix.diff), applies cleanly to `master` |
| status | **not yet reported upstream** |

## What happens

In `SyncObj.__onMessageReceived`, an `append_entries` message is handled in one
of two branches: the regular one (it has `prevLogIdx`/`prevLogTerm`, and the
follower rejects it unless its log matches) or the snapshot one (it has
`serialized`). After both branches, unconditionally:

```python
if leaderCommitIndex > self.__raftCommitIndex:
    self.__raftCommitIndex = min(leaderCommitIndex, self.__getCurrentLogIndex())
self.__raftLog.setRaftCommitIndex(self.__raftCommitIndex)
```

The rejection paths `return` before this, but a snapshot message that doesn't
finish the transfer falls through to it. So does a message with
`serialized=None`, which is what the leader sends a lagging follower while
its forked log compaction is running (`Serializer.getTransmissionData`
returns `None` whenever `__pid != 0`). Neither carries anything that proves the
follower's log agrees with the leader's, but the follower adopts the
leader's commit index anyway, capped at the end of **its own** log.

If that follower is a deposed leader holding entries it appended but never
replicated, those entries are now "committed" on it. The next tick applies
them to the state machine, and `__commandsWaitingCommit` fires their
callbacks with `FAIL_REASON.SUCCESS` (the term matches, because they really
are the entries it appended). The cluster committed different entries at
those indices.

## When it can happen

1. A leader is partitioned from the majority but keeps accepting commands
   (until `leaderFallbackTimeout`, 30 s by default).
2. The majority elects a new leader, which commits its own entries at the same
   indices and then compacts its log past the old leader's last matching
   entry. Compaction runs by default every 300 s or 5,000 entries.
3. The partition heals and the new leader brings the old one up to date with a
   snapshot. Either the old leader ticks before the transfer completes (a
   dropped connection, or a snapshot spanning more than one send loop), or the
   new leader is mid-compaction and sends `serialized=None`.

The commit index is also persisted (`setRaftCommitIndex`), so a crash in that
window replays the stale entries on restart.

## Evidence

`unit_repro.py`, in a clean virtualenv with only `pysyncobj==0.3.17`, both
message shapes:

```
--- first chunk of a snapshot transfer
a's log (index, term): [(1, 0), (2, 1), (3, 1)] commit index: 1
after b's message: term 2 commit index 3 (covers the stale entry at index 3)
after one tick: applied up to 3 | a.data = {'x': 'stale'} | callback: ['SUCCESS']
--- serialized=None, sent while the leader's forked compaction runs
a's log (index, term): [(1, 0), (2, 1), (3, 1)] commit index: 1
after b's message: term 2 commit index 3 (covers the stale entry at index 3)
after one tick: applied up to 3 | a.data = {'x': 'stale'} | callback: ['SUCCESS']
REPRODUCED: an entry the term-2 leader never had was applied and acknowledged as SUCCESS (2 of 2 message shapes)
```

`repro.py`, the same thing end to end in the simulator (3 nodes, real
PySyncObj logic, TCP-faithful network):

```
[1] t=4.00 leader n2, committed x=1
[2] t=4.50 n2 cut off but still leader; accepted put(x, 'stale') at index (4, 1) (not committed)
[3] t=8.00 n3 elected in term 2; committed x='new'
[4] t=9.00 n3 compacted its log; first index is now 4; n2 still has [(1, 0), (2, 1), (3, 1), (4, 1)]
[5] t=9.596 n2 got snapshot chunk (first=True, last=False) with commit_index=5; connection drops now
[6] t=16.00 n2: commit 4, applied 4, x='stale'
    callbacks: put x=1 SUCCESS, put x='new' SUCCESS, put x='stale' SUCCESS at t=9.615
    cluster (leader n3) has x='new'; n2 has x='stale'
REPRODUCED
```

Random campaigns (5 nodes, 60 s of faults each):

- harness at 45250c4, seeds 0–399: 13 runs hit `committed-entry-changed`.
- harness with planned fault schedules (seeds have different meanings from
  here on), seeds 0–299: 9 runs hit `committed-entry-changed`, and in 2 of
  them (seeds 52 and 270) a node went on to **apply a different command than
  the rest of the cluster at the same index** (`state-machine-safety`).
  PySyncObj's own `logger.critical("truncating already-applied log entries
  ... state machine may have diverged")` fired 5 times in that campaign.
  With the fix, both seeds run clean with the same number of acknowledged
  operations (207 and 282).
- seed 52 shrunk from 19 planned faults to 13 that still diverge
  ([`shrunk-seed52.json`](shrunk-seed52.json); replay with
  `python -m targets.pysyncobj.run --seed 52 --schedule findings/001-pysyncobj-commit-from-snapshot-chunk/shrunk-seed52.json`):

  ```
  still fails: state-machine-safety at t=60.7739: index 50: n5 applied
  ('cas', 'b', 1019, 1050, 1050), n1 applied ('get', 'a', 1048)
  ```

## Proposed fix

Adopt the leader's commit index only after the message proved the logs agree:
a regular `append_entries` whose `prevLogIdx`/`prevLogTerm` matched, or a
snapshot that was fully installed. This is rule 5 of AppendEntries in figure 2
of the Raft paper. See [`fix.diff`](fix.diff) (+9/−1).

With the patch:

- `unit_repro.py`: not reproduced, both message shapes.
- `repro.py`: not reproduced; n2 keeps commit index 3 until the snapshot is
  installed.
- seeds 0–399: `committed-entry-changed` 13 → 0, no new violation kinds, and
  the exact same number of successful (146,636) and unknown-outcome (5,541)
  client operations, so the cluster is no slower to make progress.

## Draft issue

> **Follower adopts leader's commit index from snapshot messages without a log
> match, applies and acknowledges uncommitted entries**
>
> In `__onMessageReceived`, the `append_entries` handler runs
> `self.__raftCommitIndex = min(leaderCommitIndex, self.__getCurrentLogIndex())`
> after both the regular and the snapshot branch. A snapshot chunk that doesn't
> complete the transfer, or a `serialized=None` message (sent while the leader's
> forked compaction runs), reaches it without any `prevLogIdx`/`prevLogTerm`
> check. A deposed leader that still holds entries it appended while
> partitioned then marks them committed, applies them, and calls their
> callbacks with `SUCCESS`, although the cluster committed different entries at
> those indices.
>
> Minimal reproduction (only pysyncobj 0.3.17, no network): [unit_repro.py]
> Proposed fix: only adopt `commit_index` after a matching regular
> `append_entries` or a completed snapshot install: [fix.diff]
>
> Found with deterministic simulation testing:
> https://github.com/asp53826/raft-hunt

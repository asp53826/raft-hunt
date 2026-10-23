# 002 · PySyncObj floods a lagging follower while it compacts its log

| | |
|---|---|
| library | [PySyncObj](https://github.com/bakwc/PySyncObj) 0.3.17 (= `master` at 6962777) |
| property | performance / resource use (not a safety bug on its own; it amplifies [001](../001-pysyncobj-commit-from-snapshot-chunk/)) |
| found by | 14 of 400 compaction-profile seeds hung the simulator inside `__sendAppendEntries` |
| reproductions | [`storm.py`](storm.py) (one leader tick, real fork, real clock), [`storm_tcp.py`](storm_tcp.py) (three nodes over real TCP) |
| proposed fix | [`fix.diff`](fix.diff), +5/−0, applies cleanly to `master` |
| status | **not yet reported upstream** |

## What happens

For a follower that needs a snapshot, `__sendAppendEntries` loops:

```python
while nextNodeIndex <= self.__getCurrentLogIndex() or sendSingle or sendingSerialized:
    ...
    transmissionData = self.__serializer.getTransmissionData(node)
    self.__transport.send(node, {..., 'serialized': transmissionData})
    if transmissionData is not None:
        ...
    else:
        sendingSerialized = False
    nextNodeIndex = self.__raftNextIndex[node]
    ...
    if monotonicTime() - startTime > self.__conf.appendEntriesPeriod:
        break
```

`getTransmissionData` returns `None` while a forked log compaction is running
(`Serializer.__pid != 0`). Nothing in that branch advances `nextNodeIndex`, so
the loop condition stays true and the leader resends the same
`serialized=None` message until it has spent the whole `appendEntriesPeriod`
(0.1 s by default) doing so. That happens on every tick for as long as the
child process runs, for each follower that's behind.

## Measured

`storm.py`: one leader tick during a forked compaction, with an in-process
transport so only PySyncObj's own cost is counted:

```
one tick took 100 ms and sent b 82441 messages, 82441 of them serialized=None
```

(48,010 to 82,441 across runs, depending on the machine's load.)

`storm_tcp.py`: three real nodes over TCP on localhost. A node with an empty
log joins while the leader's compaction child writes an ~80 MB dump:

```
the compaction child ran for 0.36 s after b joined
in 4.0 s the joining node received 2,254 serialized=None messages and 3 snapshot data chunks
```

Three runs: 2,254, 2,788 and 0 messages (in the last, the node connected
after the child had finished). That's roughly 7,000 messages a second to one
follower, with the leader's tick thread busy the whole time. The window grows
with the state size, since the child writes the whole state.

Each of those messages also reaches the unchecked commit-index update from
[finding 001](../001-pysyncobj-commit-from-snapshot-chunk/).

## Proposed fix

When there's no snapshot data yet, send that one message (the follower still
needs it to reset its election timer) and move on to the next node:

```diff
                     else:
+                        # The snapshot isn't available yet (e.g. a forked compaction is
+                        # still writing it). One message per period keeps the follower's
+                        # election timer reset; looping would resend it until the time
+                        # budget runs out.
                         sendingSerialized = False
+                        break
```

With the patch, `storm.py` reports `one tick took 0 ms and sent b 1
messages`, and in `storm_tcp.py` the joining node still gets its snapshot
(3 chunks).

## Draft issue

> **Leader busy-loops resending `serialized=None` to lagging followers during forked log compaction**
>
> In `__sendAppendEntries`, when a follower needs a snapshot and
> `getTransmissionData()` returns `None` (it does whenever a forked compaction is
> running), nothing advances the loop, so the leader keeps sending the same
> message until `appendEntriesPeriod` has elapsed, on every tick. Measured: 48k
> to 82k messages to one follower in a single 100 ms tick (in-process
> transport); ~7k messages/s over TCP on localhost. Reproduction and a one-line
> fix (break after sending one message): [storm.py], [fix.diff].
>
> Found with deterministic simulation testing: https://github.com/asp53826/raft-hunt

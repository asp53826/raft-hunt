"""Finding 001, standalone: no simulator, only pysyncobj 0.3.17.

A node that was leader in term 1 holds an uncommitted entry. A new leader
(term 2) starts sending it a snapshot. The first snapshot chunk carries
commit_index but no prevLogIdx/prevLogTerm, and the follower still runs

    if leaderCommitIndex > self.__raftCommitIndex:
        self.__raftCommitIndex = min(leaderCommitIndex, self.__getCurrentLogIndex())

so its commit index now covers its own stale entry, which the new leader
never had. The next tick applies that entry and calls the writer's callback
with SUCCESS.

    pip install pysyncobj==0.3.17
    python unit_repro.py      # exit code 1 = reproduced
"""
import sys

import pysyncobj.syncobj as so
from pysyncobj import SyncObj, SyncObjConf, replicated
from pysyncobj.config import FAIL_REASON
from pysyncobj.node import Node
from pysyncobj.transport import Transport

clock = [100.0]
so.monotonicTime = lambda: clock[0]          # drive time by hand


class NullTransport(Transport):
    """Records what the node sends; the test delivers replies by hand."""
    def __init__(self, syncObj, selfNode, otherNodes):
        super().__init__(syncObj, selfNode, otherNodes)
        self.sent = []

    def send(self, node, message):
        self.sent.append((node.id, message))
        return True


class KV(SyncObj):
    def __init__(self):
        super().__init__("a", ["b", "c"], SyncObjConf(autoTick=False), nodeClass=Node, transportClass=NullTransport)
        self.data = {}

    @replicated
    def put(self, key, value):
        self.data[key] = value


a = KV()
tr = a._SyncObj__transport
recv = a._SyncObj__onMessageReceived
for peer in ("b", "c"):
    tr._onNodeConnected(Node(peer))

# 1. 'a' wins term 1 with b's vote
clock[0] += 5
a.doTick()
recv(Node("b"), {"type": "response_vote", "term": 1})
assert a._isLeader() and a.raftCurrentTerm == 1

# 2. a client writes through 'a'; 'a' appends it but is cut off before any follower acks it
results = []
a.put("x", "stale", callback=lambda res, err: results.append(err))
a.doTick()
log = [(e[1], e[2]) for e in a._SyncObj__raftLog]
print("a's log (index, term):", log, "commit index:", a.raftCommitIndex)
stale_index = log[-1][0]

# 3. b becomes leader of term 2 (its own entries at these indices), compacts, and
#    starts sending 'a' a snapshot. The first chunk: no prevLogIdx, commit_index 5.
recv(Node("b"), {"type": "append_entries", "term": 2, "commit_index": 5,
                 "serialized": (b"first chunk of the snapshot", True, False)})
print("after the first snapshot chunk: term", a.raftCurrentTerm, "commit index", a.raftCommitIndex,
      "(covers the stale entry at index %d)" % stale_index if a.raftCommitIndex >= stale_index else "")

# 4. the connection drops before the rest of the snapshot; 'a' keeps ticking
a.doTick()
print("after one tick: applied up to", a.raftLastApplied, "| a.data =", a.data, "| callback:",
      [k for e in results for k, v in vars(FAIL_REASON).items() if v == e])

if FAIL_REASON.SUCCESS in results and a.data.get("x") == "stale":
    print("REPRODUCED: an entry the term-2 leader never had was applied and acknowledged as SUCCESS")
    sys.exit(1)
print("not reproduced")

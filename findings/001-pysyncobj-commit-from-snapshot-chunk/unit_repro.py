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


def case(label, serialized):
    """serialized: the first chunk of a snapshot transfer, or None (what a leader
    sends a lagging follower while its forked log compaction is running)."""
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
    stale_index = log[-1][0]
    print(f"--- {label}")
    print("a's log (index, term):", log, "commit index:", a.raftCommitIndex)

    # 3. b is leader of term 2 with different entries at these indices and has
    #    compacted its log, so it talks to 'a' with snapshot messages: no prevLogIdx.
    recv(Node("b"), {"type": "append_entries", "term": 2, "commit_index": 5, "serialized": serialized})
    print("after b's message: term", a.raftCurrentTerm, "commit index", a.raftCommitIndex,
          "(covers the stale entry at index %d)" % stale_index if a.raftCommitIndex >= stale_index else "")

    # 4. no more messages arrive for a moment; 'a' keeps ticking
    a.doTick()
    names = [k for e in results for k, v in vars(FAIL_REASON).items() if v == e and not k.startswith("_")]
    print("after one tick: applied up to", a.raftLastApplied, "| a.data =", a.data, "| callback:", names)
    return FAIL_REASON.SUCCESS in results and a.data.get("x") == "stale"


hits = [case("first chunk of a snapshot transfer", (b"first chunk of the snapshot", True, False)),
        case("serialized=None, sent while the leader's forked compaction runs", None)]
if any(hits):
    print("REPRODUCED: an entry the term-2 leader never had was applied and acknowledged as SUCCESS",
          f"({sum(hits)} of 2 message shapes)")
    sys.exit(1)
print("not reproduced")

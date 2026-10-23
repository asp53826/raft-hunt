"""Finding 003, standalone: pysyncobj 0.3.17 reports LEADER_CHANGED ("leader
has changed without command commit", config.py) for a command that the
cluster then commits and applies.

A follower forwards a client's command to the leader. The leader appends it
and replicates it, but before its apply_command_response gets back, a new
leader takes over. On the leader change the follower fails every forwarded
command still waiting for a reply with LEADER_CHANGED. The new leader's log
contains the command (it was replicated to a majority before the old leader
went away), so the follower then applies it. The client was told it failed;
a client that retries applies it twice.

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
so.monotonicTime = lambda: clock[0]
NAMES = {v: k for k, v in vars(FAIL_REASON).items() if not k.startswith("_")}


class NullTransport(Transport):
    def __init__(self, syncObj, selfNode, otherNodes):
        super().__init__(syncObj, selfNode, otherNodes)
        self.sent = []

    def send(self, node, message):
        self.sent.append((node.id, message))
        return True


class Counter(SyncObj):
    def __init__(self):
        super().__init__("a", ["b", "c"], SyncObjConf(autoTick=False), nodeClass=Node, transportClass=NullTransport)
        self.value = 0

    @replicated
    def incr(self):
        self.value += 1
        return self.value


a = Counter()
tr = a._SyncObj__transport
recv = a._SyncObj__onMessageReceived
for peer in ("b", "c"):
    tr._onNodeConnected(Node(peer))

# 'b' is leader of term 1; 'a' is a follower with the same log
recv(Node("b"), {"type": "append_entries", "term": 1, "commit_index": 1, "prevLogIdx": 1, "prevLogTerm": 0,
                 "entries": [(b"\x01", 2, 1)]})   # leader b's no-op at index 2 (command type NO_OP)

# a client calls incr() on 'a'; 'a' forwards it to the leader
results = []
a.incr(callback=lambda res, err: results.append(NAMES[err]))
a.doTick()
fwd = [m for n, m in tr.sent if n == "b" and m["type"] == "apply_command"][-1]
print("a forwarded the command to b:", {k: v for k, v in fwd.items() if k != "command"})

# b appended it at index 3 and replicated it to c, then crashed before replying to a.
# c wins term 2; its first append_entries to a carries the command at index 3.
recv(Node("c"), {"type": "append_entries", "term": 2, "commit_index": 1, "prevLogIdx": 2, "prevLogTerm": 1,
                 "entries": [(fwd["command"], 3, 1), (b"\x01", 4, 2)]})
print("callback after the leader change:", results)

# c commits through index 4 (it holds the entries on a majority: c and a)
recv(Node("c"), {"type": "append_entries", "term": 2, "commit_index": 4, "prevLogIdx": 4, "prevLogTerm": 2,
                 "entries": []})
a.doTick()
print("a applied up to", a.raftLastApplied, "| counter =", a.value)

if results == ["LEADER_CHANGED"] and a.value == 1:
    print("REPRODUCED: incr() was reported LEADER_CHANGED (documented as 'without command commit') and committed anyway")
    sys.exit(1)
print("not reproduced")

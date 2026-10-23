"""How many messages does one leader tick send a lagging follower while the
leader's forked log compaction is running? Real pysyncobj, real fork, real clock."""
import os, sys, time, tempfile
from pysyncobj import SyncObj, SyncObjConf, replicated
from pysyncobj.node import Node
from pysyncobj.transport import Transport


class NullTransport(Transport):
    def __init__(self, syncObj, selfNode, otherNodes):
        super().__init__(syncObj, selfNode, otherNodes)
        self.sent = []

    def send(self, node, message):
        self.sent.append((node.id, message))
        return True


class KV(SyncObj):
    def __init__(self, dump):
        super().__init__("a", ["b", "c"], SyncObjConf(autoTick=False, fullDumpFile=dump, useFork=True,
                                                     logCompactionMinEntries=10 ** 9, logCompactionMinTime=10 ** 9),
                         nodeClass=Node, transportClass=NullTransport)
        self.data = {}

    @replicated
    def put(self, key, value):
        self.data[key] = value


d = tempfile.mkdtemp()
a = KV(os.path.join(d, "dump.bin"))
tr = a._SyncObj__transport
recv = a._SyncObj__onMessageReceived
for p in ("b", "c"):
    tr._onNodeConnected(Node(p))
time.sleep(1.5)                       # let the election timeout pass on the real clock
a.doTick()
recv(Node("b"), {"type": "response_vote", "term": 1})
assert a._isLeader()

# a few hundred entries, acknowledged by c so they commit and apply; b stays behind
big = "v" * int(sys.argv[1]) if len(sys.argv) > 1 else "v"
for i in range(300):
    a.put("k%d" % i, big)
a.doTick()
last = a._SyncObj__raftLog[-1][1]
recv(Node("c"), {"type": "next_node_idx", "next_node_idx": last + 1, "reset": False, "success": True})
a.doTick(); a.doTick()

# first compaction: wait for the forked child, then let the leader drop compacted entries
a.forceLogCompaction()
a.doTick()
for _ in range(200):
    time.sleep(0.02)
    a.doTick()
    if a._SyncObj__raftLog[0][1] > 2:
        break
print("leader log now starts at", a._SyncObj__raftLog[0][1], "of", a._SyncObj__raftLog[-1][1])

# b reports it is far behind: nextIndex <= first log index, so it needs a snapshot
recv(Node("b"), {"type": "next_node_idx", "next_node_idx": 2, "reset": True, "success": False})

# a second compaction starts (forked); tick once while the child is still writing
for i in range(300, 600):
    a.put("k%d" % i, big)
a.doTick()
last = a._SyncObj__raftLog[-1][1]
recv(Node("c"), {"type": "next_node_idx", "next_node_idx": last + 1, "reset": False, "success": True})
a.doTick(); a.doTick()
a.forceLogCompaction()
a.doTick()                      # compaction starts at the end of this tick: the child forks
time.sleep(0.12)                # next heartbeat is due
pid = a._SyncObj__serializer._Serializer__pid
print("serializer state while measuring: pid", pid, "(child running)" if pid > 0 else "")
tr.sent.clear()
t0 = time.monotonic()
a.doTick()
dt = time.monotonic() - t0
to_b = [m for n, m in tr.sent if n == "b"]
none_msgs = [m for m in to_b if m.get("type") == "append_entries" and "serialized" in m and m["serialized"] is None]
print(f"one tick took {dt * 1000:.0f} ms and sent b {len(to_b)} messages, {len(none_msgs)} of them serialized=None")

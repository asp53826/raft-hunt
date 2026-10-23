"""Finding 002 over real TCP: three PySyncObj 0.3.17 nodes on localhost with
their default autoTick threads, TCPTransport and forked log compaction. A
node that is far behind joins while the leader compacts, and we count the
append_entries messages it receives with serialized=None.

    pip install pysyncobj==0.3.17
    python storm_tcp.py
"""
import collections
import os
import socket
import sys
import tempfile
import threading
import time

import pysyncobj.syncobj as so
from pysyncobj import SyncObj, SyncObjConf, replicated

counts = collections.Counter()
lock = threading.Lock()
orig = so.SyncObj._SyncObj__onMessageReceived


def counting(self, node, message):
    if message.get("type") == "append_entries" and "serialized" in message:
        with lock:
            counts[(self.selfNode.id, "none" if message["serialized"] is None else "data")] += 1
    return orig(self, node, message)


so.SyncObj._SyncObj__onMessageReceived = counting


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


class KV(SyncObj):
    def __init__(self, me, others, d):
        super().__init__(me, others, SyncObjConf(fullDumpFile=os.path.join(d, me.replace(":", "_") + ".dump"),
                                                 useFork=True, logCompactionMinEntries=10 ** 9,
                                                 logCompactionMinTime=10 ** 9))
        self.data = {}

    @replicated
    def put(self, key, value):
        self.data[key] = value


d = tempfile.mkdtemp()
addrs = ["127.0.0.1:%d" % free_port() for _ in range(3)]
a_addr, b_addr, c_addr = addrs
a = KV(a_addr, [b_addr, c_addr], d)
c = KV(c_addr, [a_addr, b_addr], d)
while not (a._isLeader() or c._isLeader()):
    time.sleep(0.05)
leader = a if a._isLeader() else c
print("leader:", leader.selfNode.id)

payload = "v" * 200_000
for i in range(400):
    leader.put("k%d" % i, payload, sync=True)
print("wrote 400 x 200 KB")

leader.forceLogCompaction()
while leader._SyncObj__raftLog[0][1] <= 2:
    time.sleep(0.05)
print("leader compacted; log starts at", leader._SyncObj__raftLog[0][1])

# The leader starts another compaction (a forked child writes the ~80 MB dump),
# and while that child runs, b joins with an empty log and needs a snapshot.
for i in range(400, 420):                 # something new to compact (PySyncObj skips no-op compactions)
    leader.put("k%d" % i, payload, sync=True)
leader.forceLogCompaction()
ser = leader._SyncObj__serializer
deadline = time.time() + 10
while ser._Serializer__pid <= 0:
    if time.time() > deadline:
        sys.exit("compaction never forked")
    time.sleep(0.005)
print("forked compaction running in pid", ser._Serializer__pid)
b = KV(b_addr, [a_addr, c_addr], d)
t0 = time.time()
child_done = None
while time.time() - t0 < 4.0:
    if child_done is None and ser._Serializer__pid <= 0:
        child_done = time.time() - t0
    time.sleep(0.01)
elapsed = time.time() - t0
print(f"the compaction child ran for {child_done:.2f} s after b joined" if child_done else "child still running")
with lock:
    none_b, data_b = counts[(b_addr, "none")], counts[(b_addr, "data")]
print(f"in {elapsed:.1f} s the joining node received {none_b:,} serialized=None messages "
      f"and {data_b:,} snapshot data chunks")
for n in (a, b, c):
    n.destroy()
sys.exit(1 if none_b > 1000 else 0)

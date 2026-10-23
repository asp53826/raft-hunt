"""A deterministic world for PySyncObj.

PySyncObj is driven the way its own docs allow for embedding: ``autoTick``
off, ``doTick()`` called by us, and a custom ``Transport``. Everything that
could make two runs differ is owned here:

- **time:** ``monotonicTime`` in every PySyncObj module reads a virtual clock,
  per node, so pauses and clock offsets are just numbers.
- **randomness:** election timeouts come from a ``random.Random`` seeded per
  run.
- **network:** a TCP-faithful model. Each pair of nodes shares one
  connection; messages on it arrive in order, once. A connection can be cut
  (both sides notice), blackholed (nobody notices until PySyncObj's own
  ``connectionTimeout``), or lost with a crashed process. Data in flight on a
  dead connection is lost, as with TCP. Nothing here reorders or duplicates
  messages on a live connection, because TCP doesn't.
- **disks:** each node's journal and full-dump files live in a run directory.
  A crash drops the process (no shutdown hook runs) but keeps the files, as
  the OS page cache would. Power loss is a separate fault model.

Set ``PYTHONHASHSEED=0``: PySyncObj iterates sets of nodes, and string hashes
are randomized per process otherwise. ``run.py`` enforces it.
"""
from __future__ import annotations

import heapq
import itertools
import os
import pickle
import random
import shutil
import tempfile
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

import pysyncobj.dns_resolver
import pysyncobj.syncobj
import pysyncobj.transport
from pysyncobj import SyncObj, SyncObjConf, replicated
from pysyncobj.config import FAIL_REASON
from pysyncobj.node import Node
from pysyncobj.transport import Transport

FAIL_NAMES = {v: k for k, v in vars(FAIL_REASON).items() if not k.startswith("_")}


# --------------------------------------------------------------------------- clock

class World:
    """Global simulation state. One World per run; PySyncObj modules are
    patched to read from whichever World is active."""
    active: Optional["World"] = None

    def __init__(self, seed: int):
        self.seed = seed
        self.now = 0.0                       # true simulated time
        self.current: Optional[str] = None   # node whose code is running
        self.offset: dict[str, float] = {}   # per-node clock offset
        self.busy = 0.0                      # time the running node has spent inside this event
        self.rng = random.Random(seed)       # schedule decisions
        self.lib_rng = random.Random(seed ^ 0x5EED)  # PySyncObj's own randomness

    def local_time(self) -> float:
        return self.now + self.busy + self.offset.get(self.current, 0.0)


def _monotonic():
    return World.active.local_time()


def install(world: World) -> None:
    World.active = world
    for mod in (pysyncobj.syncobj, pysyncobj.transport, pysyncobj.dns_resolver):
        mod.monotonicTime = _monotonic
    pysyncobj.syncobj.random = world.lib_rng      # used for election timeouts
    pysyncobj.syncobj.time.sleep = lambda s: None  # never block the simulator


# ------------------------------------------------------------------------- network

@dataclass(order=True)
class Event:
    t: float
    seq: int
    kind: str = field(compare=False)
    data: Any = field(compare=False, default=None)


class Net:
    """Connections between nodes, TCP-style."""

    # Sending isn't free: pickling and a socket write cost the sender time. Without
    # this, PySyncObj loops that stop after appendEntriesPeriod of wall time
    # (__sendAppendEntries) would never stop, since the virtual clock doesn't move
    # inside an event. 30us + 1 GB/s is a cheap localhost send.
    SEND_COST = 30e-6
    SEND_BYTES_PER_S = 1e9

    def __init__(self, sim: "Sim", latency=(0.001, 0.012)):
        self.sim = sim
        self.latency = latency
        self.state: dict[frozenset, str] = {}    # pair -> "up" | "down" | "blackhole"
        self.epoch: dict[frozenset, int] = {}    # bumps whenever a connection dies
        self.last_delivery: dict[tuple, float] = {}
        self.blackhole_since: dict[frozenset, float] = {}

    def pair(self, a, b):
        return frozenset((a, b))

    def link(self, a, b) -> str:
        return self.state.get(self.pair(a, b), "down")

    def send(self, src: str, dst: str, message: Any) -> bool:
        p = self.pair(src, dst)
        st = self.state.get(p, "down")
        if st == "down":
            return False
        self.sim.stats["msgs_sent"] += 1
        w = self.sim.world
        if st == "blackhole":
            w.busy += self.SEND_COST
            self.sim.stats["msgs_lost"] += 1
            return True                           # TCP accepted it; it will never arrive
        # serialize like the real transport, so no object is shared between nodes
        payload = pickle.dumps(message, protocol=pickle.HIGHEST_PROTOCOL)
        w.busy += self.SEND_COST + len(payload) / self.SEND_BYTES_PER_S
        if isinstance(message, dict) and message.get("type") == "append_entries" and "serialized" in message:
            self.sim.stats["snapshot_msgs"] += 1
            if message["serialized"] is None:
                self.sim.stats["snapshot_none_msgs"] += 1
        lo, hi = self.latency
        t = w.now + w.busy + w.rng.uniform(lo, hi)
        ch = (src, dst)
        t = max(t, self.last_delivery.get(ch, 0.0) + 1e-6)   # FIFO per direction
        self.last_delivery[ch] = t
        self.sim.schedule(t, "deliver", (src, dst, self.epoch.get(p, 0), payload, message.get("type") if isinstance(message, dict) else None))
        return True

    def connect(self, a, b):
        p = self.pair(a, b)
        if self.state.get(p) == "up":
            return
        self.state[p] = "up"
        self.blackhole_since.pop(p, None)
        self.sim.notify_connected(a, b)
        self.sim.notify_connected(b, a)

    def disconnect(self, a, b, notify=True):
        p = self.pair(a, b)
        was = self.state.get(p, "down")
        self.state[p] = "down"
        self.epoch[p] = self.epoch.get(p, 0) + 1      # in-flight data on the old connection is gone
        self.blackhole_since.pop(p, None)
        self.last_delivery.pop((a, b), None); self.last_delivery.pop((b, a), None)
        if notify and was != "down":
            self.sim.notify_disconnected(a, b)
            self.sim.notify_disconnected(b, a)

    def blackhole(self, a, b):
        p = self.pair(a, b)
        if self.state.get(p) == "up":
            self.state[p] = "blackhole"
            self.epoch[p] = self.epoch.get(p, 0) + 1
            self.blackhole_since[p] = self.sim.world.now


class SimTransport(Transport):
    def __init__(self, syncObj, selfNode, otherNodes, sim: "Sim" = None):
        super().__init__(syncObj, selfNode, otherNodes)
        self.sim = sim
        self.me = selfNode.id
        self.nodes = {n.id: n for n in otherNodes}

    @property
    def ready(self):
        return True

    def tryGetReady(self):
        pass

    def waitReady(self):
        pass

    def addNode(self, node):
        self.nodes[node.id] = node

    def dropNode(self, node):
        self.nodes.pop(node.id, None)

    def send(self, node, message):
        return self.sim.net.send(self.me, node.id, message)

    def destroy(self):
        pass


# ----------------------------------------------------------------------------- app

class KV(SyncObj):
    """A replicated register map. Every command carries the client op id, so
    the safety checker can tell commands apart exactly."""

    def __init__(self, me, others, conf, sim):
        super().__init__(me, others, conf, nodeClass=Node,
                         transportClass=lambda so, sn, on: SimTransport(so, sn, on, sim=sim))
        self._d = {}   # set after super().__init__ so log compaction serializes it

    @replicated
    def put(self, key, value, opid):
        self._d[key] = value
        Sim.observe(self, ("put", key, value, opid))
        return value

    @replicated
    def get(self, key, opid):
        v = self._d.get(key)
        Sim.observe(self, ("get", key, opid))
        return v

    @replicated
    def cas(self, key, expected, new, opid):
        ok = self._d.get(key) == expected
        if ok:
            self._d[key] = new
        Sim.observe(self, ("cas", key, expected, new, opid))
        return ok


# ------------------------------------------------------------------------ the sim

class Violation(Exception):
    def __init__(self, kind: str, detail: str):
        super().__init__(f"{kind}: {detail}")
        self.kind, self.detail = kind, detail


class Sim:
    current: Optional["Sim"] = None

    def __init__(self, seed: int, n: int = 5, conf_overrides: Optional[dict] = None,
                 tick=0.02, workdir: Optional[str] = None, trace: bool = False):
        self.world = World(seed)
        install(self.world)
        Sim.current = self
        self.n = n
        self.ids = [f"n{i}" for i in range(1, n + 1)]
        self.tick_period = tick
        self.conf_overrides = conf_overrides or {}
        self.events: list[Event] = []
        self.seq = itertools.count()
        self.net = Net(self)
        self.nodes: dict[str, Optional[KV]] = {}
        self.paused_until: dict[str, float] = {}
        self.generation: dict[str, int] = {i: 0 for i in self.ids}
        self.dir = workdir or tempfile.mkdtemp(prefix=f"raft-hunt-{seed}-")
        self.stats = {"msgs_sent": 0, "msgs_lost": 0, "delivered": 0, "ticks": 0, "crashes": 0,
                      "restarts": 0, "elections": 0, "snapshot_msgs": 0, "snapshot_none_msgs": 0}
        # safety bookkeeping
        self.leader_of_term: dict[int, str] = {}
        self.applied: dict[int, Any] = {}               # log index -> command, first seen
        self.applied_by: dict[int, str] = {}
        self.apps: dict[int, list] = {}                 # log index -> [(node, command, term)] for every application
        self.committed: dict[int, tuple] = {}           # index -> (term, command) seen as committed
        self.blocked: set = set()                       # pairs a partition currently forbids
        self.trace: Optional[list] = [] if trace else None
        self.violations: list[Violation] = []
        self.stop_on_violation = False                  # record and keep going, to see what clients see
        self._reported: set = set()

    # ---- lifecycle -------------------------------------------------------

    def conf(self, nid: str) -> SyncObjConf:
        d = os.path.join(self.dir, nid)
        os.makedirs(d, exist_ok=True)
        kw = dict(autoTick=False, useFork=False, journalFile=os.path.join(d, "journal.bin"),
                  fullDumpFile=os.path.join(d, "dump.bin"), appendEntriesPeriod=0.1,
                  raftMinTimeout=0.4, raftMaxTimeout=1.4, connectionTimeout=3.5,
                  connectionRetryTime=1.0, leaderFallbackTimeout=5.0,
                  logCompactionMinEntries=1_000_000, logCompactionMinTime=1e9)
        kw.update(self.conf_overrides)
        return SyncObjConf(**kw)

    def start(self, nid: str):
        others = [i for i in self.ids if i != nid]
        with self.running(nid):
            node = KV(nid, others, self.conf(nid), self)
        self.nodes[nid] = node
        self.generation[nid] += 1
        self.schedule(self.world.now + self.world.rng.uniform(0, self.tick_period), "tick", (nid, self.generation[nid]))
        self.log("start", nid)

    def boot(self):
        for nid in self.ids:
            self.start(nid)
        for a, b in itertools.combinations(self.ids, 2):
            self.net.connect(a, b)

    def crash(self, nid: str):
        if self.nodes.get(nid) is None:
            return
        self.stats["crashes"] += 1
        self.nodes[nid] = None            # no destroy(): a crash runs no shutdown code
        for other in self.ids:
            if other != nid:
                self.net.disconnect(nid, other)
        self.log("crash", nid)

    def restart(self, nid: str):
        if self.nodes.get(nid) is not None:
            return
        self.stats["restarts"] += 1
        self.start(nid)
        self.log("restart", nid)
        # peers reconnect after their retry interval
        self.schedule(self.world.now + self.conf(nid).connectionRetryTime, "reconnect", nid)

    def alive(self, nid) -> bool:
        return self.nodes.get(nid) is not None

    # ---- context: which node's clock and code is running ----------------

    class _Running:
        def __init__(self, sim, nid):
            self.sim, self.nid = sim, nid

        def __enter__(self):
            self.prev = self.sim.world.current
            self.sim.world.current = self.nid

        def __exit__(self, *a):
            self.sim.world.current = self.prev

    def running(self, nid):
        return Sim._Running(self, nid)

    # ---- events ----------------------------------------------------------

    def schedule(self, t: float, kind: str, data=None):
        heapq.heappush(self.events, Event(t, next(self.seq), kind, data))

    def log(self, kind, *data):
        if self.trace is not None:
            self.trace.append([round(self.world.now, 4), kind, *data])

    def notify_connected(self, at: str, peer: str):
        node = self.nodes.get(at)
        if node is not None:
            tr = node._SyncObj__transport
            with self.running(at):
                tr._onNodeConnected(tr.nodes.get(peer) or Node(peer))

    def notify_disconnected(self, at: str, peer: str):
        node = self.nodes.get(at)
        if node is not None:
            tr = node._SyncObj__transport
            with self.running(at):
                tr._onNodeDisconnected(tr.nodes.get(peer) or Node(peer))

    def step(self) -> bool:
        if not self.events:
            return False
        ev = heapq.heappop(self.events)
        self.world.now = max(self.world.now, ev.t)
        self.world.busy = 0.0
        getattr(self, "_on_" + ev.kind)(ev.data)
        return True

    def run_until(self, t_end: float, after_step: Optional[Callable] = None):
        while self.events and self.events[0].t <= t_end:
            self.step()
            if after_step:
                after_step()
        self.world.now = max(self.world.now, t_end)

    def _on_tick(self, data):
        nid, gen = data
        node = self.nodes.get(nid)
        if node is None or self.generation[nid] != gen:
            return
        if self.paused_until.get(nid, 0) > self.world.now:
            self.schedule(self.paused_until[nid], "tick", data)
            return
        with self.running(nid):
            node.doTick(0.0)
        self.stats["ticks"] += 1
        self.check_node(nid)
        self.schedule(self.world.now + self.tick_period * self.world.rng.uniform(0.8, 1.2), "tick", data)

    def _on_deliver(self, data):
        src, dst, epoch, payload, mtype = data
        p = self.net.pair(src, dst)
        if self.net.epoch.get(p, 0) != epoch or self.net.state.get(p) != "up":
            self.stats["msgs_lost"] += 1
            return
        node = self.nodes.get(dst)
        if node is None:
            return
        if self.paused_until.get(dst, 0) > self.world.now:
            # a paused process doesn't read its socket; TCP holds the bytes
            self.schedule(self.paused_until[dst], "deliver", data)
            return
        tr = node._SyncObj__transport
        with self.running(dst):
            tr._onMessageReceived(tr.nodes.get(src) or Node(src), pickle.loads(payload))
        self.stats["delivered"] += 1
        self.check_node(dst)

    def _on_reconnect(self, nid):
        for other in self.ids:
            if other != nid and self.alive(nid) and self.alive(other) and self.net.link(nid, other) == "down" \
                    and not self.partitioned(nid, other):
                self.net.connect(nid, other)

    def _on_detect(self, pair):
        # a blackholed connection that has been silent for connectionTimeout is declared dead
        a, b = tuple(pair)
        if self.net.state.get(pair) == "blackhole":
            self.net.disconnect(a, b)

    def _on_call(self, fn):
        fn()

    # ---- partitions --------------------------------------------------------

    def partitioned(self, a, b) -> bool:
        return frozenset((a, b)) in self.blocked

    def partition(self, groups: list[list[str]], silent: bool):
        """Cut every link between groups."""
        where = {nid: gi for gi, g in enumerate(groups) for nid in g}
        pairs = [(a, b) for a, b in itertools.combinations(self.ids, 2) if where.get(a) != where.get(b)]
        self.cut(pairs, silent, label=[list(g) for g in groups])

    def cut(self, pairs, silent: bool, label=None):
        """Forbid the given links. A silent cut blackholes the connection, and
        PySyncObj only notices through its own connectionTimeout; a loud cut
        closes it, and both sides see the disconnect at once."""
        self.heal(reconnect=False)
        for a, b in pairs:
            p = frozenset((a, b))
            self.blocked.add(p)
            if silent:
                self.net.blackhole(a, b)
                self.schedule(self.world.now + self.conf(a).connectionTimeout, "detect", p)
            else:
                self.net.disconnect(a, b)
        self.log("partition", label if label is not None else sorted(sorted(p) for p in map(tuple, pairs)),
                 "silent" if silent else "cut")

    def heal(self, reconnect=True):
        had = bool(self.blocked)
        self.blocked = set()
        for a, b in itertools.combinations(self.ids, 2):
            if self.net.link(a, b) == "blackhole":
                self.net.disconnect(a, b)
        if reconnect:
            retry = self.conf(self.ids[0]).connectionRetryTime
            for nid in self.ids:
                self.schedule(self.world.now + retry * self.world.rng.uniform(0.2, 1.0), "reconnect", nid)
            if had:
                self.log("heal")

    def pause(self, nid: str, dur: float):
        self.paused_until[nid] = self.world.now + dur
        self.log("pause", nid, round(dur, 3))

    # ---- observation and safety checks -------------------------------------

    @staticmethod
    def observe(node: KV, command):
        sim = Sim.current
        nid = node.selfNode.id
        idx = node.raftLastApplied + 1     # incremented by PySyncObj right after the call returns
        entry = node._SyncObj__getEntries(idx, 1)
        term = entry[0][2] if entry else None
        sim.apps.setdefault(idx, []).append((nid, command, term))
        sim.log("apply", nid, idx, term, list(command))
        first = sim.applied.get(idx)
        if first is None:
            sim.applied[idx] = command
            sim.applied_by[idx] = nid
        elif first != command:
            sim.fail("state-machine-safety",
                     f"index {idx}: {sim.applied_by[idx]} applied {first!r}, {nid} applied {command!r}")

    def fail(self, kind, detail):
        v = Violation(kind, detail)
        if kind not in self._reported:          # first occurrence of each kind is the interesting one
            self._reported.add(kind)
            v.t = round(self.world.now, 4)
            self.violations.append(v)
        self.log("VIOLATION", kind, detail)
        if self.stop_on_violation:
            raise v

    def agreed(self) -> dict[int, Any]:
        """The command a majority of the nodes that applied an index agree on."""
        out = {}
        for idx, rows in self.apps.items():
            by_node = {}
            for nid, cmd, term in rows:
                by_node[nid] = cmd          # a node's latest application of the index
            counts: dict = {}
            for cmd in by_node.values():
                counts[cmd] = counts.get(cmd, 0) + 1
            cmd, c = max(counts.items(), key=lambda kv: kv[1])
            if c > self.n // 2:
                out[idx] = cmd
        return out

    def check_node(self, nid: str):
        node = self.nodes.get(nid)
        if node is None:
            return
        term = node.raftCurrentTerm
        if node._isLeader():
            prev = self.leader_of_term.get(term)
            if prev is None:
                self.leader_of_term[term] = nid
                self.stats["elections"] += 1
                self.log("leader", nid, term)
            elif prev != nid:
                self.fail("election-safety", f"term {term} has two leaders: {prev} and {nid}")
        # every committed entry this node can see is recorded; a different entry
        # at a recorded index anywhere later is a lost commit
        log = node._SyncObj__raftLog
        commit = node.raftCommitIndex
        if len(log) == 0:
            return
        first_idx = log[0][1]
        for idx in range(max(first_idx, commit - 3), commit + 1):
            k = idx - first_idx
            if 0 <= k < len(log):
                cmd, i, t = log[k]
                seen = self.committed.get(idx)
                if seen is None:
                    self.committed[idx] = (t, bytes(cmd))
                elif seen != (t, bytes(cmd)):
                    self.fail("committed-entry-changed",
                              f"index {idx} was committed as term {seen[0]}, {nid} now has term {t} there "
                              f"(commit index {commit})")

    def cleanup(self):
        shutil.rmtree(self.dir, ignore_errors=True)

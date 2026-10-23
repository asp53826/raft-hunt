// Command etcdraft runs one seeded fault-injection simulation of etcd-io/raft
// (v3.7.0, RawNode API) and prints a JSON result: safety violations seen during
// the run, stats, the fault plan, and the client history for the Python
// linearizability checker (hunt/).
//
// etcd's raft package is a deterministic state machine: the application owns
// time (Tick), the network (Ready.Messages) and storage (Ready.Entries,
// HardState). That is exactly what a simulator needs, so this file is the
// whole adapter. The network here is a datagram network, not TCP: etcd raft
// must tolerate dropped, duplicated and reordered messages, so all three are
// injected. Storage is raft.MemoryStorage, treated as the disk: it survives a
// crash, everything else in the process does not.
//
//	go run . -seed 1 -profile default
package main

import (
	"container/heap"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"sort"
	"strings"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// ---------------------------------------------------------------- profiles

type Profile struct {
	Nodes        int
	Duration     float64 // seconds of faults and traffic
	Settle       float64
	Clients      int
	OpTimeout    float64
	FaultGap     [2]float64
	Faults       map[string]int
	Drop, Dup    float64 // baseline per-message probabilities
	Latency      [2]float64
	CompactEvery uint64 // snapshot + compact after this many applied entries (0 = never)
	ReadIndex    float64
}

var profiles = map[string]Profile{
	"default": {Nodes: 5, Duration: 60, Settle: 15, Clients: 4, OpTimeout: 3, FaultGap: [2]float64{1, 5},
		Faults: map[string]int{"partition": 3, "bridge": 1, "heal": 4, "crash": 2, "restart": 3, "pause": 2,
			"pause_leader": 2, "lossy": 2, "transfer": 2},
		Drop: 0.01, Dup: 0.01, Latency: [2]float64{0.001, 0.02}, CompactEvery: 200, ReadIndex: 0.3},
	"lossy": {Nodes: 5, Duration: 60, Settle: 15, Clients: 4, OpTimeout: 3, FaultGap: [2]float64{1, 4},
		Faults: map[string]int{"partition": 2, "heal": 3, "crash": 1, "restart": 2, "lossy": 4, "pause_leader": 1},
		Drop:   0.1, Dup: 0.05, Latency: [2]float64{0.001, 0.08}, CompactEvery: 100, ReadIndex: 0.3},
	"snap": {Nodes: 5, Duration: 60, Settle: 15, Clients: 4, OpTimeout: 3, FaultGap: [2]float64{1, 4},
		Faults: map[string]int{"partition": 3, "heal": 3, "crash": 3, "restart": 4, "pause_leader": 1, "lossy": 1},
		Drop:   0.02, Dup: 0.02, Latency: [2]float64{0.001, 0.03}, CompactEvery: 15, ReadIndex: 0.3},
}

// ------------------------------------------------------------------- events

type event struct {
	t    float64
	seq  int64
	kind string
	fn   func()
}
type eventQ []*event

func (q eventQ) Len() int { return len(q) }
func (q eventQ) Less(i, j int) bool {
	if q[i].t != q[j].t {
		return q[i].t < q[j].t
	}
	return q[i].seq < q[j].seq
}
func (q eventQ) Swap(i, j int) { q[i], q[j] = q[j], q[i] }
func (q *eventQ) Push(x any)   { *q = append(*q, x.(*event)) }
func (q *eventQ) Pop() any     { o := *q; n := len(o); x := o[n-1]; *q = o[:n-1]; return x }

// ------------------------------------------------------------------ the app

type Cmd struct {
	ID  int64  `json:"id"`
	Op  string `json:"op"` // put | get | cas
	Key string `json:"k"`
	Val int64  `json:"v,omitempty"`
	Exp *int64 `json:"e,omitempty"`
}

type Op struct {
	ID       int      `json:"id"`
	Client   string   `json:"client"`
	Kind     string   `json:"kind"` // write | read | cas
	Key      string   `json:"key"`
	Value    any      `json:"value"`
	Invoke   float64  `json:"invoke"`
	Complete *float64 `json:"complete"`
	Status   string   `json:"status"` // ok | fail | info
	Output   any      `json:"output"`
	Note     string   `json:"note"`
	Cmd      int64    `json:"cmd"`
	home     uint64
	done     bool
}

type snapState struct {
	KV   map[string]int64 `json:"kv"`
	Seen map[int64]bool   `json:"seen"`
}

type node struct {
	id          uint64
	alive       bool
	gen         int
	rn          *raft.RawNode
	disk        *raft.MemoryStorage
	kv          map[string]int64
	seen        map[int64]bool
	applied     uint64
	pausedUntil float64
	reads       map[string]*Op
	sinceSnap   uint64
}

type violation struct {
	Kind   string  `json:"kind"`
	Detail string  `json:"detail"`
	T      float64 `json:"t"`
}

type Fault struct {
	T    float64 `json:"t"`
	Kind string  `json:"kind"`
	R    int64   `json:"r"`
}

type Sim struct {
	p        Profile
	now      float64
	seq      int64
	q        eventQ
	rng      *rand.Rand // network and ticks
	wl       *rand.Rand // workload
	nodes    []*node
	ids      []uint64
	blocked  map[[2]uint64]bool
	drop     float64
	dup      float64
	lossyEnd float64
	ops      []*Op
	pending  map[int64]*Op // command id -> op
	nextCmd  int64
	stopped  bool
	// safety bookkeeping
	leaderOf  map[uint64]uint64
	apps      map[uint64]map[uint64]string // index -> node -> command key
	committed map[uint64]string            // index -> term:data
	viol      []violation
	reported  map[string]bool
	stats     map[string]int
	trace     [][]any
	traceOn   bool
}

const tick = 0.01

func (s *Sim) at(t float64, kind string, fn func()) {
	s.seq++
	heap.Push(&s.q, &event{t: t, seq: s.seq, kind: kind, fn: fn})
}

func (s *Sim) log(v ...any) {
	if s.traceOn {
		s.trace = append(s.trace, append([]any{round(s.now)}, v...))
	}
}

func round(x float64) float64 { return float64(int64(x*10000)) / 10000 }

func (s *Sim) fail(kind, detail string) {
	s.log("VIOLATION", kind, detail)
	if s.reported[kind] {
		return
	}
	s.reported[kind] = true
	s.viol = append(s.viol, violation{kind, detail, round(s.now)})
}

func (s *Sim) node(id uint64) *node { return s.nodes[id-1] }

var quiet = &raft.DefaultLogger{Logger: log.New(io.Discard, "", 0)}

func (s *Sim) config(n *node) *raft.Config {
	return &raft.Config{ID: n.id, ElectionTick: 10, HeartbeatTick: 1, Storage: n.disk, Applied: n.applied,
		MaxSizePerMsg: 1 << 20, MaxInflightMsgs: 256, CheckQuorum: true, PreVote: true,
		ReadOnlyOption: raft.ReadOnlySafe, Logger: quiet}
}

func (s *Sim) start(n *node, bootstrap bool) {
	rn, err := raft.NewRawNode(s.config(n))
	if err != nil {
		panic(err)
	}
	if bootstrap {
		peers := make([]raft.Peer, len(s.ids))
		for i, id := range s.ids {
			peers[i] = raft.Peer{ID: id}
		}
		if err := rn.Bootstrap(peers); err != nil {
			panic(err)
		}
	}
	n.rn, n.alive = rn, true
	n.gen++
	n.reads = map[string]*Op{}
	gen := n.gen
	var tickFn func()
	tickFn = func() {
		if !n.alive || n.gen != gen {
			return
		}
		if n.pausedUntil > s.now {
			s.at(n.pausedUntil, "tick", tickFn)
			return
		}
		n.rn.Tick()
		s.stats["ticks"]++
		s.process(n)
		s.at(s.now+tick*(0.9+0.2*s.rng.Float64()), "tick", tickFn)
	}
	s.at(s.now+tick*s.rng.Float64(), "tick", tickFn)
}

func (s *Sim) crash(n *node) {
	if !n.alive {
		return
	}
	s.stats["crashes"]++
	n.alive, n.rn = false, nil
	// the state machine is volatile; on restart it comes back from the snapshot
	// and the committed log in storage
	for _, op := range n.reads {
		s.info(op, "crash")
	}
	s.log("crash", n.id)
}

func (s *Sim) restart(n *node) {
	if n.alive {
		return
	}
	s.stats["restarts"]++
	snap, _ := n.disk.Snapshot()
	n.kv, n.seen, n.applied = map[string]int64{}, map[int64]bool{}, snap.GetMetadata().GetIndex()
	if len(snap.GetData()) > 0 {
		var st snapState
		_ = json.Unmarshal(snap.GetData(), &st)
		n.kv, n.seen = st.KV, st.Seen
		if n.kv == nil {
			n.kv = map[string]int64{}
		}
		if n.seen == nil {
			n.seen = map[int64]bool{}
		}
	}
	s.start(n, false)
	s.log("restart", n.id)
}

// process drains Ready: persist, send, apply, advance. Messages go out only
// after the entries and hard state they depend on are on "disk".
func (s *Sim) process(n *node) {
	for n.alive && n.rn.HasReady() {
		rd := n.rn.Ready()
		if !raft.IsEmptySnap(rd.Snapshot) {
			if err := n.disk.ApplySnapshot(rd.Snapshot); err != nil {
				panic(err)
			}
			var st snapState
			_ = json.Unmarshal(rd.Snapshot.GetData(), &st)
			n.kv, n.seen = st.KV, st.Seen
			if n.kv == nil {
				n.kv = map[string]int64{}
			}
			if n.seen == nil {
				n.seen = map[int64]bool{}
			}
			n.applied = rd.Snapshot.GetMetadata().GetIndex()
			s.log("install-snapshot", n.id, n.applied)
		}
		if err := n.disk.Append(rd.Entries); err != nil {
			panic(err)
		}
		if !raft.IsEmptyHardState(rd.HardState) {
			if err := n.disk.SetHardState(rd.HardState); err != nil {
				panic(err)
			}
		}
		for _, m := range rd.Messages {
			s.send(n.id, m)
		}
		for _, e := range rd.CommittedEntries {
			s.apply(n, e)
		}
		for _, rs := range rd.ReadStates {
			if op := n.reads[string(rs.RequestCtx)]; op != nil {
				idx := rs.Index
				s.waitApplied(n, idx, op)
			}
		}
		n.rn.Advance(rd)
		s.maybeSnapshot(n)
	}
	s.check(n)
}

func (s *Sim) waitApplied(n *node, idx uint64, op *Op) {
	gen := n.gen
	var try func()
	try = func() {
		if !n.alive || n.gen != gen || op.done {
			return
		}
		if n.applied >= idx {
			delete(n.reads, fmt.Sprint(op.Cmd))
			s.ok(op, kvGet(n.kv, op.Key))
			return
		}
		s.at(s.now+tick, "read-wait", try)
	}
	try()
}

func kvGet(kv map[string]int64, k string) any {
	if v, ok := kv[k]; ok {
		return v
	}
	return nil
}

func (s *Sim) apply(n *node, e *pb.Entry) {
	if e.GetIndex() <= n.applied {
		return
	}
	n.applied = e.GetIndex()
	n.sinceSnap++
	if e.GetType() != pb.EntryNormal || len(e.GetData()) == 0 {
		return
	}
	var c Cmd
	if err := json.Unmarshal(e.GetData(), &c); err != nil {
		s.fail("bad-entry", fmt.Sprintf("index %d on n%d: %v", e.GetIndex(), n.id, err))
		return
	}
	key := string(e.GetData())
	byNode := s.apps[e.GetIndex()]
	if byNode == nil {
		byNode = map[uint64]string{}
		s.apps[e.GetIndex()] = byNode
	}
	for other, k := range byNode {
		if k != key && other != n.id {
			s.fail("state-machine-safety", fmt.Sprintf("index %d: n%d applied %s, n%d applied %s", e.GetIndex(), other, k, n.id, key))
			break
		}
	}
	byNode[n.id] = key
	s.log("apply", n.id, e.GetIndex(), e.GetTerm(), c.ID)
	// the state machine dedupes by command id, as etcd's apply layer does:
	// a duplicated MsgProp can legitimately be appended twice
	var out any
	if !n.seen[c.ID] {
		n.seen[c.ID] = true
		switch c.Op {
		case "put":
			n.kv[c.Key] = c.Val
		case "get":
			out = kvGet(n.kv, c.Key)
		case "cas":
			cur, has := n.kv[c.Key]
			okk := (c.Exp == nil && !has) || (c.Exp != nil && has && cur == *c.Exp)
			if okk {
				n.kv[c.Key] = c.Val
			}
			out = okk
		}
	} else {
		return
	}
	if op := s.pending[c.ID]; op != nil && op.home == n.id && !op.done {
		if op.Kind == "write" {
			out = nil
		}
		s.ok(op, out)
	}
}

func (s *Sim) maybeSnapshot(n *node) {
	if s.p.CompactEvery == 0 || n.sinceSnap < s.p.CompactEvery {
		return
	}
	s.compact(n)
}

func (s *Sim) compact(n *node) {
	data, _ := json.Marshal(snapState{n.kv, n.seen})
	cs := &pb.ConfState{Voters: s.ids}
	if _, err := n.disk.CreateSnapshot(n.applied, cs, data); err != nil {
		return
	}
	keep := uint64(5)
	if n.applied > keep {
		_ = n.disk.Compact(n.applied - keep)
	}
	n.sinceSnap = 0
	s.stats["snapshots"]++
}

// ------------------------------------------------------------------ network

func pair(a, b uint64) [2]uint64 {
	if a > b {
		a, b = b, a
	}
	return [2]uint64{a, b}
}

func (s *Sim) send(from uint64, m *pb.Message) {
	s.stats["msgs"]++
	to := m.GetTo()
	dst := s.node(to)
	lost := s.blocked[pair(from, to)] || s.rng.Float64() < s.drop
	copies := 1
	if s.rng.Float64() < s.dup {
		copies = 2
		s.stats["dups"]++
	}
	isSnap := m.GetType() == pb.MsgSnap
	if isSnap {
		s.stats["snap_msgs"]++
	}
	if lost {
		s.stats["dropped"]++
		if isSnap {
			s.at(s.now+0.05, "snap-report", func() { s.reportSnap(from, to, raft.SnapshotFailure) })
		}
		return
	}
	data, err := proto.Marshal(m) // a real wire copy: no shared slices between nodes
	if err != nil {
		panic(err)
	}
	for i := 0; i < copies; i++ {
		d := s.p.Latency[0] + s.rng.Float64()*(s.p.Latency[1]-s.p.Latency[0])
		if isSnap {
			d += float64(len(data)) / 50e6
		}
		s.at(s.now+d, "deliver", func() {
			if !dst.alive || s.blocked[pair(from, to)] {
				s.stats["dropped"]++
				if isSnap {
					s.reportSnap(from, to, raft.SnapshotFailure)
				}
				return
			}
			s.deliver(dst, data, isSnap, from)
		})
	}
}

func (s *Sim) deliver(dst *node, data []byte, isSnap bool, from uint64) {
	if dst.pausedUntil > s.now {
		s.at(dst.pausedUntil, "deliver", func() {
			if dst.alive {
				s.deliver(dst, data, isSnap, from)
			}
		})
		return
	}
	m := &pb.Message{}
	if err := proto.Unmarshal(data, m); err != nil {
		panic(err)
	}
	_ = dst.rn.Step(m)
	s.stats["delivered"]++
	if isSnap {
		s.reportSnap(from, dst.id, raft.SnapshotFinish)
	}
	s.process(dst)
}

func (s *Sim) reportSnap(from, to uint64, st raft.SnapshotStatus) {
	src := s.node(from)
	if src.alive {
		src.rn.ReportSnapshot(to, st)
		s.process(src)
	}
}

// ------------------------------------------------------------------- checks

func (s *Sim) check(n *node) {
	if !n.alive {
		return
	}
	st := n.rn.BasicStatus()
	if st.RaftState == raft.StateLeader {
		if prev, ok := s.leaderOf[st.GetTerm()]; !ok {
			s.leaderOf[st.GetTerm()] = n.id
			s.stats["elections"]++
			s.log("leader", n.id, st.GetTerm())
		} else if prev != n.id {
			s.fail("election-safety", fmt.Sprintf("term %d has two leaders: n%d and n%d", st.GetTerm(), prev, n.id))
		}
	}
	commit := st.GetCommit()
	first, _ := n.disk.FirstIndex()
	lo := commit
	if lo > 3 {
		lo -= 3
	}
	if lo < first {
		lo = first
	}
	if commit < lo {
		return
	}
	ents, err := n.disk.Entries(lo, commit+1, 1<<30)
	if err != nil {
		return
	}
	for _, e := range ents {
		k := fmt.Sprintf("%d:%x", e.GetTerm(), e.GetData())
		if prev, ok := s.committed[e.GetIndex()]; !ok {
			s.committed[e.GetIndex()] = k
		} else if prev != k {
			s.fail("committed-entry-changed", fmt.Sprintf("index %d committed as %s, n%d has %s (commit %d)",
				e.GetIndex(), short(prev), n.id, short(k), commit))
		}
	}
}

func short(s string) string {
	if len(s) > 40 {
		return s[:40] + "…"
	}
	return s
}

// ----------------------------------------------------------------- workload

func (s *Sim) ok(op *Op, out any) {
	if op.done {
		return
	}
	op.done = true
	t := round(s.now)
	op.Complete, op.Status, op.Output = &t, "ok", out
	s.log("op", op.ID, op.Client, op.home, op.Kind, op.Key, "ok")
	s.afterOp(op)
}

func (s *Sim) failOp(op *Op, note string) {
	if op.done {
		return
	}
	op.done = true
	t := round(s.now)
	op.Complete, op.Status, op.Note = &t, "fail", note
	s.afterOp(op)
}

func (s *Sim) info(op *Op, note string) {
	if op.done {
		return
	}
	op.done = true
	op.Status, op.Note = "info", note
	s.afterOp(op)
}

func (s *Sim) afterOp(op *Op) {
	if strings.HasPrefix(op.Client, "f") || s.stopped {
		return
	}
	c := op.Client
	s.at(s.now+s.wl.Float64()*0.3, "client", func() { s.issue(c, "") })
}

var keys = []string{"a", "b", "c"}

func (s *Sim) issue(client, finalKey string) {
	if s.stopped && finalKey == "" {
		return
	}
	var alive []*node
	for _, n := range s.nodes {
		if n.alive {
			alive = append(alive, n)
		}
	}
	if len(alive) == 0 {
		s.at(s.now+0.5, "client", func() { s.issue(client, finalKey) })
		return
	}
	n := alive[s.wl.Intn(len(alive))]
	s.nextCmd++
	id := s.nextCmd
	op := &Op{ID: len(s.ops), Client: client, Invoke: round(s.now), Status: "info", Cmd: id, home: n.id,
		Note: fmt.Sprintf("n%d", n.id)}
	s.ops = append(s.ops, op)
	s.pending[id] = op
	var c Cmd
	r := s.wl.Float64()
	switch {
	case finalKey != "":
		op.Kind, op.Key = "read", finalKey
		c = Cmd{ID: id, Op: "get", Key: finalKey}
	case r < 0.45:
		op.Kind, op.Key, op.Value = "write", keys[s.wl.Intn(len(keys))], id
		c = Cmd{ID: id, Op: "put", Key: op.Key, Val: id}
	case r < 0.85:
		op.Kind, op.Key = "read", keys[s.wl.Intn(len(keys))]
		c = Cmd{ID: id, Op: "get", Key: op.Key}
	default:
		op.Kind, op.Key = "cas", keys[s.wl.Intn(len(keys))]
		var exp *int64
		var prior []int64
		for _, o := range s.ops {
			if o.Key == op.Key && o.Kind == "write" {
				prior = append(prior, o.Value.(int64))
			}
		}
		if len(prior) > 0 && s.wl.Float64() < 0.7 {
			v := prior[s.wl.Intn(len(prior))]
			exp = &v
		}
		op.Value = []any{exp, id}
		c = Cmd{ID: id, Op: "cas", Key: op.Key, Val: id, Exp: exp}
	}
	timeout := s.p.OpTimeout
	if op.Kind == "read" && finalKey == "" && s.wl.Float64() < s.p.ReadIndex {
		// a linearizable read through ReadIndex instead of the log
		op.Note += " readindex"
		ctx := fmt.Sprint(id)
		n.reads[ctx] = op
		n.rn.ReadIndex([]byte(ctx))
		s.process(n)
	} else {
		data, _ := json.Marshal(c)
		if err := n.rn.Propose(data); err != nil {
			// ErrProposalDropped: no leader known, nothing was appended anywhere
			s.failOp(op, err.Error())
			return
		}
		s.process(n)
	}
	s.at(s.now+timeout, "timeout", func() { s.info(op, "timeout") })
}

// --------------------------------------------------------------------- faults

func (s *Sim) plan(r *rand.Rand) []Fault {
	var kinds []string
	for k := range s.p.Faults {
		kinds = append(kinds, k)
	}
	sort.Strings(kinds)
	total := 0
	for _, k := range kinds {
		total += s.p.Faults[k]
	}
	var out []Fault
	t := 0.0
	for {
		t += s.p.FaultGap[0] + r.Float64()*(s.p.FaultGap[1]-s.p.FaultGap[0])
		if t >= s.p.Duration {
			return out
		}
		x, k := r.Intn(total), ""
		for _, kk := range kinds {
			if x < s.p.Faults[kk] {
				k = kk
				break
			}
			x -= s.p.Faults[kk]
		}
		out = append(out, Fault{T: round(t), Kind: k, R: r.Int63()})
	}
}

func (s *Sim) leader() *node {
	var best *node
	for _, n := range s.nodes {
		if n.alive {
			st := n.rn.BasicStatus()
			if st.RaftState == raft.StateLeader {
				if best == nil {
					best = n
				} else if bst := best.rn.BasicStatus(); st.GetTerm() > bst.GetTerm() {
					best = n
				}
			}
		}
	}
	return best
}

func (s *Sim) cut(pairs [][2]uint64) {
	s.blocked = map[[2]uint64]bool{}
	for _, p := range pairs {
		s.blocked[pair(p[0], p[1])] = true
	}
}

func (s *Sim) fire(f Fault) {
	r := rand.New(rand.NewSource(f.R))
	s.log("fault", f.Kind)
	ids := append([]uint64(nil), s.ids...)
	r.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
	switch f.Kind {
	case "partition":
		k := 1 + r.Intn(len(ids)/2)
		var ps [][2]uint64
		for _, a := range ids[:k] {
			for _, b := range ids[k:] {
				ps = append(ps, [2]uint64{a, b})
			}
		}
		s.cut(ps)
		s.log("partition", ids[:k], ids[k:])
	case "bridge":
		rest := ids[1:]
		a, b := rest[:len(rest)/2], rest[len(rest)/2:]
		var ps [][2]uint64
		for _, x := range a {
			for _, y := range b {
				ps = append(ps, [2]uint64{x, y})
			}
		}
		s.cut(ps)
		s.log("bridge", ids[0], a, b)
	case "heal":
		s.blocked = map[[2]uint64]bool{}
		s.log("heal")
	case "crash":
		for _, id := range ids {
			if s.node(id).alive {
				s.crash(s.node(id))
				break
			}
		}
	case "restart":
		for _, id := range ids {
			if !s.node(id).alive {
				s.restart(s.node(id))
				break
			}
		}
	case "pause":
		n := s.node(ids[0])
		n.pausedUntil = s.now + 0.5 + r.Float64()*5
		s.log("pause", n.id)
	case "pause_leader":
		if l := s.leader(); l != nil {
			l.pausedUntil = s.now + 1 + r.Float64()*6
			s.log("pause", l.id)
		}
	case "lossy":
		s.drop, s.dup = 0.2+r.Float64()*0.3, 0.05+r.Float64()*0.1
		end := s.now + 1 + r.Float64()*4
		s.lossyEnd = end
		s.log("lossy", round(s.drop), round(s.dup))
		s.at(end, "lossy-end", func() {
			if s.lossyEnd == end {
				s.drop, s.dup = s.p.Drop, s.p.Dup
			}
		})
	case "transfer":
		if l := s.leader(); l != nil {
			t := ids[0]
			if t == l.id {
				t = ids[1]
			}
			l.rn.TransferLeader(t)
			s.log("transfer", l.id, t)
			s.process(l)
		}
	case "compact":
		n := s.node(ids[0])
		if n.alive {
			s.compact(n)
		}
	}
}

// ---------------------------------------------------------------------- main

type Result struct {
	Seed       int64            `json:"seed"`
	Profile    string           `json:"profile"`
	Violations []violation      `json:"violations"`
	Stats      map[string]int   `json:"stats"`
	Terms      uint64           `json:"terms"`
	Schedule   []Fault          `json:"schedule"`
	History    []*Op            `json:"history"`
	Agreed     map[int64]uint64 `json:"agreed"` // command id -> index where a majority applied it
	Trace      [][]any          `json:"trace,omitempty"`
}

func main() {
	seed := flag.Int64("seed", 1, "")
	prof := flag.String("profile", "default", "")
	schedFile := flag.String("schedule", "", "replay this fault plan (JSON) instead of the seed's own")
	traceOn := flag.Bool("trace", false, "include the event trace")
	noHist := flag.Bool("no-history", false, "omit the history (summary only)")
	flag.Parse()
	p, ok := profiles[*prof]
	if !ok {
		fmt.Fprintln(os.Stderr, "unknown profile")
		os.Exit(2)
	}
	raft.HuntSeed(*seed ^ 0x5EED)
	s := &Sim{p: p, rng: rand.New(rand.NewSource(*seed)), wl: rand.New(rand.NewSource(*seed<<1 ^ 0xC11E47)),
		blocked: map[[2]uint64]bool{}, drop: p.Drop, dup: p.Dup, pending: map[int64]*Op{},
		leaderOf: map[uint64]uint64{}, apps: map[uint64]map[uint64]string{}, committed: map[uint64]string{},
		reported: map[string]bool{}, stats: map[string]int{}, traceOn: *traceOn, nextCmd: 1000}
	for i := 1; i <= p.Nodes; i++ {
		s.ids = append(s.ids, uint64(i))
	}
	for _, id := range s.ids {
		n := &node{id: id, disk: raft.NewMemoryStorage(), kv: map[string]int64{}, seen: map[int64]bool{}}
		s.nodes = append(s.nodes, n)
		s.start(n, true)
	}
	var plan []Fault
	if *schedFile != "" {
		b, err := os.ReadFile(*schedFile)
		if err != nil {
			panic(err)
		}
		var wrapped struct {
			Schedule []Fault `json:"schedule"`
		}
		if json.Unmarshal(b, &wrapped) == nil && wrapped.Schedule != nil {
			plan = wrapped.Schedule
		} else if err := json.Unmarshal(b, &plan); err != nil {
			panic(err)
		}
	} else {
		plan = s.plan(rand.New(rand.NewSource(*seed<<2 ^ 0xFA017)))
	}
	for _, f := range plan {
		f := f
		s.at(f.T, "fault", func() { s.fire(f) })
	}
	for i := 0; i < p.Clients; i++ {
		c := fmt.Sprintf("c%d", i+1)
		s.at(0.5+s.wl.Float64(), "client", func() { s.issue(c, "") })
	}
	run := func(until float64) {
		for s.q.Len() > 0 && s.q[0].t <= until {
			e := heap.Pop(&s.q).(*event)
			if e.t > s.now {
				s.now = e.t
			}
			e.fn()
		}
		if until > s.now {
			s.now = until
		}
	}
	run(p.Duration)
	s.stopped = true
	s.blocked = map[[2]uint64]bool{}
	s.drop, s.dup = p.Drop, p.Dup
	for _, n := range s.nodes {
		n.pausedUntil = 0
		s.restart(n)
	}
	run(p.Duration + p.Settle)
	for i, k := range append(append([]string{}, keys...), keys...) {
		c, k := fmt.Sprintf("f%d", i), k
		s.at(s.now+0.05*float64(i), "client", func() { s.issue(c, k) })
	}
	run(p.Duration + p.Settle + p.OpTimeout + 1)

	// Commands a majority of nodes hold in their final state, after every fault
	// has healed and the cluster has settled. Each node's set of applied command
	// ids travels inside snapshots, so a node that caught up by snapshot still
	// counts. (Counting per-index applications instead, as an earlier version
	// did, misses commands that most nodes received only through a snapshot.)
	agreed := map[int64]uint64{}
	holders := map[int64]int{}
	for _, n := range s.nodes {
		if n.alive {
			for id := range n.seen {
				holders[id]++
			}
		}
	}
	for id, c := range holders {
		if c > p.Nodes/2 {
			agreed[id] = 0
		}
	}
	for idx, byNode := range s.apps {
		for _, k := range byNode {
			var cmd Cmd
			_ = json.Unmarshal([]byte(k), &cmd)
			if _, ok := agreed[cmd.ID]; ok && (agreed[cmd.ID] == 0 || idx < agreed[cmd.ID]) {
				agreed[cmd.ID] = idx
			}
		}
	}
	var maxTerm uint64
	for t := range s.leaderOf {
		if t > maxTerm {
			maxTerm = t
		}
	}
	res := Result{Seed: *seed, Profile: *prof, Violations: s.viol, Stats: s.stats, Terms: maxTerm,
		Schedule: plan, Agreed: agreed}
	if res.Violations == nil {
		res.Violations = []violation{}
	}
	if !*noHist {
		res.History = s.ops
	}
	if *traceOn {
		res.Trace = s.trace
	}
	enc := json.NewEncoder(os.Stdout)
	_ = enc.Encode(res)
}

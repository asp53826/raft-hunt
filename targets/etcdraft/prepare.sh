#!/bin/sh
# Make a local copy of etcd-io/raft v3.7.0 whose only change is the source of
# randomness: election timeouts come from a seeded math/rand instead of
# crypto/rand, so a seed replays exactly. Nothing else in the library changes;
# `diff -r` against the module cache shows the two edits.
#
# Optional: MUTANT=<name> applies one planted bug from mutants/ on top.
set -e
cd "$(dirname "$0")"
SRC="$(go env GOMODCACHE)/go.etcd.io/raft/v3@v3.7.0"
[ -d "$SRC" ] || GOFLAGS=-mod=mod go mod download go.etcd.io/raft/v3@v3.7.0
DST="${DST:-third_party/raft}"
rm -rf "$DST"
mkdir -p "$(dirname "$DST")"
cp -r "$SRC" "$DST"
chmod -R u+w "$DST"
python3 - "$DST/raft.go" <<'PY'
import sys
p = sys.argv[1]
s = open(p).read()
old = """func (r *lockedRand) Intn(n int) int {
	r.mu.Lock()
	v, _ := rand.Int(rand.Reader, big.NewInt(int64(n)))
	r.mu.Unlock()
	return int(v.Int64())
}"""
new = """func (r *lockedRand) Intn(n int) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	if huntRand != nil { // raft-hunt: seeded, so runs replay exactly
		return huntRand.Intn(n)
	}
	v, _ := rand.Int(rand.Reader, big.NewInt(int64(n)))
	return int(v.Int64())
}"""
assert s.count(old) == 1, "etcd raft changed: lockedRand.Intn not found"
open(p, "w").write(s.replace(old, new))
PY
cat > "$DST/hunt_seed.go" <<'GO'
package raft

import mrand "math/rand"

// raft-hunt: the only addition to the library. HuntSeed makes election
// timeouts deterministic for one simulated run.
var huntRand *mrand.Rand

func HuntSeed(seed int64) { huntRand = mrand.New(mrand.NewSource(seed)) }
GO
if [ -n "$MUTANT" ]; then
	python3 mutants.py apply "$MUTANT" "$DST"
fi
echo "prepared $DST${MUTANT:+ (mutant $MUTANT)}"

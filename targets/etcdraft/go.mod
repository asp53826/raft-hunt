module github.com/asp53826/raft-hunt/targets/etcdraft

go 1.26

require go.etcd.io/raft/v3 v3.7.0

require google.golang.org/protobuf v1.36.11

replace go.etcd.io/raft/v3 => ./third_party/raft

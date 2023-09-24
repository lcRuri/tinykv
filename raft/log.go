// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	// storage存储了所有自从上次快照以来稳定的日志
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	// commited是已知处于的最高日志位置 节点仲裁上的稳定存储。
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	// 已经用于状态机 applied<=commited
	// 大部分peer已经commited之后，applied
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	// 日志小于这个字段将会被持久化到storage
	// 它用于记录还没有被持久化的日志
	// 每次处理Ready，不稳定的logs将会被包括
	stabled uint64

	// all entries that have not yet compact.
	// 所有尚未压缩的条目
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	//从storage中取出消息
	state, _, _ := storage.InitialState()
	firstIndex, _ := storage.FirstIndex()
	lastIndex, _ := storage.LastIndex()
	entries, _ := storage.Entries(firstIndex, lastIndex+1)

	raftLog := &RaftLog{
		storage:   storage,
		committed: state.Commit,
		// todo why applied = firstIndex - 1
		applied: firstIndex - 1,
		stabled: lastIndex,
		entries: entries,
	}

	return raftLog
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).
	return l.entries
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	return l.entries[l.stabled+1:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	//todo
	return l.entries[l.applied : l.committed-1]
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).

	return uint64(len(l.entries))
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	term, err := l.storage.Term(i)
	if err != nil {
		return 0, err
	}
	return term, nil
}

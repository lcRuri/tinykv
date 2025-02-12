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
	firstIndex      uint64

	// Your Data Here (2A).
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	//从storage中取出消息
	firstIndex, _ := storage.FirstIndex()
	lastIndex, _ := storage.LastIndex()
	entries, _ := storage.Entries(firstIndex, lastIndex+1)
	hardState, _, _ := storage.InitialState()

	raftLog := &RaftLog{
		storage:   storage,
		committed: hardState.Commit,

		applied:         firstIndex - 1,
		stabled:         lastIndex,
		entries:         entries,
		pendingSnapshot: nil,
		firstIndex:      firstIndex,
	}

	return raftLog
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
// 我们需要在某个时间点压缩日志条目，例如
// Storage Compact 稳定日志条目阻止日志条目
// 在内存中无限增长
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
	truncateIndex, _ := l.storage.FirstIndex()
	if len(l.entries) > 0 {
		firstIndex := l.entries[0].Index
		//entries的第一条日志都比storage中小，说明已经被压缩成为快照了
		if firstIndex < truncateIndex {
			l.entries = l.entries[truncateIndex-firstIndex:]
		}
	}
}

func (l *RaftLog) Commited() uint64 {
	// Your Code Here (2C).
	return l.committed
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).
	// l.storage+l.entries
	// 判断firstIndex和stabled大小
	Entries := []pb.Entry{}

	firstIndex, err := l.storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	lastIndex, err := l.storage.LastIndex()
	if err != nil {
		panic(err)
	}

	if firstIndex <= lastIndex && firstIndex <= l.stabled+1 {
		Entries, err = l.storage.Entries(firstIndex, l.stabled+1)
	}

	if err != nil {
		panic(err)
	}

	if len(l.entries) > 0 {
		for _, entry := range l.entries {
			if entry.Index > l.stabled {
				Entries = append(Entries, entry)
			}
		}
	}

	return Entries
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).

	//stabled稳定的是指下标
	ents := make([]pb.Entry, 0)
	for _, entry := range l.entries {
		if entry.Index > l.stabled {
			ents = append(ents, entry)
		}
	}
	return ents
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	//todo
	if len(l.entries) > 0 {
		if l.committed-l.FirstIndex()+1 < 0 || l.applied-l.FirstIndex()+1 > l.LastIndex() {
			return nil
		}
		if l.applied-l.FirstIndex()+1 >= 0 && l.committed-l.FirstIndex()+1 <= uint64(len(l.entries)) {
			return l.entries[l.applied-l.FirstIndex()+1 : l.committed-l.FirstIndex()+1]
		}
	}
	return nil

}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) != 0 {
		return l.entries[len(l.entries)-1].Index
	}

	return l.stabled
}

// Term return the term of the entry in the given index
// 获取给定index的term
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	// 判断index的范围
	if i < 1 {
		return 0, nil
	}

	lastIndex := l.LastIndex()
	//fmt.Println("i:", i, "lastIndex", lastIndex, "entries:", len(l.entries), "l.stabled:", l.stabled)
	if i > lastIndex {
		return 0, nil
	}

	if len(l.entries) > 0 {
		if i >= l.FirstIndex() {
			index := i - l.FirstIndex()
			if index >= uint64(len(l.entries)) {
				return 0, ErrUnavailable
			}
			return l.entries[index].Term, nil
		}
	}

	term, err := l.storage.Term(i)
	if err != nil {
		if err == ErrUnavailable {
			if !IsEmptySnap(l.pendingSnapshot) {
				return l.pendingSnapshot.Metadata.Term, nil
			}
		}
		//panic(err)
	}

	return term, nil

}

func (l *RaftLog) hasNextEnts() bool {
	index, _ := l.storage.FirstIndex()

	off := max(l.applied+1, index)
	return l.committed+1 > off
}

func (l *RaftLog) FirstIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		i, _ := l.storage.FirstIndex()
		return i - 1
	}
	return l.entries[0].Index
}

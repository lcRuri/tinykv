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
	"errors"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"math/rand"
	"sync"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	mu *sync.Mutex

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	raft := &Raft{
		id:               c.ID,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		State:            StateFollower,
		mu:               new(sync.Mutex),
		electionElapsed:  0,
		heartbeatElapsed: 0,
	}

	go raft.tick()

	return raft
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
// sendAppend发送一个携带新的日志的RPC和目前已经提交的日志索引给发送的peer，返回为真如果消息发送了
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).

	return false
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
// sendHeartbeat发送一个心跳RPC给发送的peer
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	msg := pb.Message{
		To:   to,
		From: r.id,
		Term: r.Term,
	}

	//根据角色的不同设置msg的type
	if r.State == StateCandidate {
		msg.MsgType = pb.MessageType_MsgRequestVote
	}
	if r.State == StateLeader {
		msg.MsgType = pb.MessageType_MsgBeat
	}

	//发送消息，只需将其推送到 raft.Raft.msgs
	r.msgs = append(r.msgs, msg)
}

// tick advances the internal logical clock by a single tick.
// 时钟周期使内部逻辑时钟提前一个时钟周期,用的都是逻辑时钟
func (r *Raft) tick() {
	random := rand.Intn(5) + 5
	// Your Code Here (2A).
	for {
		switch r.State {
		//维护electionElapsed
		case StateFollower:
			r.electionElapsed++
			if r.electionElapsed > r.electionTimeout+random {
				msg := pb.Message{MsgType: pb.MessageType_MsgHup}
				//将msg发送到本地的msgs,msgHup表示start a new election.
				r.msgs = append(r.msgs, msg)
				r.becomeCandidate()
				r.electionElapsed = 0
			}

		case StateCandidate:
			////控制选举时间，如果在规定时间内都没有完成选举，退回为follower
			//r.electionElapsed++
			//声明消息的类型
			msg := pb.Message{MsgType: pb.MessageType_MsgHup, From: r.id, Term: r.Term}
			r.msgs = append(r.msgs, msg)
		//维护heartElapsed
		case StateLeader:
			r.heartbeatElapsed++
			//当heartbeatElapsed超过heartbeatTimeout，发送心跳
			if r.heartbeatElapsed > r.heartbeatTimeout {
				msg := pb.Message{MsgType: pb.MessageType_MsgBeat}
				r.msgs = append(r.msgs, msg)
				r.electionElapsed = 0
			}
		}
	}

}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.mu.Lock()
	defer r.mu.Unlock()

	r.State = StateFollower
	r.Term = term
	r.Lead = lead

}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.mu.Lock()
	defer r.mu.Unlock()

	r.State = StateCandidate
	r.Term += 1
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.mu.Lock()
	defer r.mu.Unlock()

	r.State = StateLeader
	r.Term += 1
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
// Raft 收到的所有消息将被传递到 raft.Raft.Step()
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		//处理来自candidate和leader的请求
		r.handleHeartbeat(m)
	case StateCandidate:
		//发起投票
		if m.MsgType == pb.MessageType_MsgHup {
			r.sendHeartbeat(m.To)
		}

		if m.MsgType == pb.MessageType_MsgRequestVote {
			r.sendHeartbeat(m.To)
		}

		//收到响应
		if m.MsgType == pb.MessageType_MsgRequestVoteResponse {
			//todo
		}
	case StateLeader:
		//发送心跳
		if m.MsgType == pb.MessageType_MsgHeartbeat {
			r.sendHeartbeat(m.To)
		}
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
// handleAppendEntries处理添加日志的RPC请求
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).

}

// handleHeartbeat handle Heartbeat RPC request
// handleHeartbeat处理心跳的RPC请求
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.becomeCandidate()
	case pb.MessageType_MsgRequestVote:
		if m.Term > r.Term {
			r.becomeFollower(m.Term, m.From)
			r.Term = m.Term
			r.Vote = m.From
			r.Lead = m.From
			r.electionElapsed = 0

			//返回响应
			msg := pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse, From: r.id, To: m.From}
			r.msgs = append(r.msgs, msg)
		}
	case pb.MessageType_MsgBeat:
		if m.Term >= r.Term {
			r.becomeFollower(m.Term, m.From)
			r.Term = m.Term
			r.Lead = m.From
			r.heartbeatElapsed = 0
		}
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

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
	"sort"
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

	mu    *sync.Mutex
	peers []uint64
	resp  int

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	// 记录每个对等方的复制进度 匹配的日志索引和下一个日志索引
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
		//2AA
		id:               c.ID,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		State:            StateFollower,
		mu:               new(sync.Mutex),
		electionElapsed:  0,
		heartbeatElapsed: 0,
		peers:            c.peers,
		votes:            map[uint64]bool{},
		//2AB
		RaftLog: newLog(c.Storage),
		Prs:     map[uint64]*Progress{},
	}

	return raft
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
// sendAppend发送一个携带新的日志的RPC和目前已经提交的日志索引给发送的peer，返回为真如果消息发送了
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	if r.State != StateLeader {
		return false
	}

	if r.Prs[to].Match < uint64(len(r.RaftLog.entries)) {
		//起始是leader的Prs里面存储的当前peer的下一条日志的位置
		entries := make([]*pb.Entry, 0)
		for i := r.Prs[to].Next - 1; i < uint64(len(r.RaftLog.entries)); i++ {
			entries = append(entries, &r.RaftLog.entries[i])
		}

		term, err := r.RaftLog.Term(r.Prs[to].Next - 1)
		if err != nil {
			return false
		}
		msg := pb.Message{
			MsgType: pb.MessageType_MsgAppend,
			To:      to,
			From:    r.id,
			Term:    r.Term,
			LogTerm: term,
			Index:   r.Prs[to].Next - 1,
			Entries: entries,
			Commit:  r.RaftLog.committed,
		}

		r.msgs = append(r.msgs, msg)
	}

	return true
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
		msg.MsgType = pb.MessageType_MsgHeartbeat
	}

	//发送消息，只需将其推送到 raft.Raft.msgs
	r.msgs = append(r.msgs, msg)
}

// tick advances the internal logical clock by a single tick.
// 时钟周期使内部逻辑时钟提前一个时钟周期,用的都是逻辑时钟。tick是否要作为协程启动
// 逻辑时钟
func (r *Raft) tick() {
	if len(r.peers) == 1 && r.State == StateFollower {
		r.becomeCandidate()
		r.becomeLeader()
		return
	}
	random := rand.Intn(20)
	// Your Code Here (2A).
	switch r.State {
	//维护electionElapsed
	case StateFollower:
		r.electionElapsed++
		if r.electionElapsed >= r.electionTimeout+random {
			msg := pb.Message{MsgType: pb.MessageType_MsgHup, From: r.id, To: r.id}
			//将msg发送到本地的msgs,msgHup表示start a new election.
			r.Step(msg)

			r.electionElapsed = 0
		}
	case StateCandidate:
		r.electionElapsed++
		if r.electionElapsed >= r.electionTimeout+random {
			msg := pb.Message{MsgType: pb.MessageType_MsgHup, From: r.id, To: r.id}
			//将msg发送到本地的msgs,msgHup表示start a new election.
			r.Step(msg)

			r.electionElapsed = 0
		}

	//维护heartElapsed
	case StateLeader:
		r.heartbeatElapsed++
		//当heartbeatElapsed超过heartbeatTimeout，发送心跳
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			msg := pb.Message{MsgType: pb.MessageType_MsgBeat}
			r.Step(msg)
			r.electionElapsed = 0
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
	//给自己投票
	r.votes[r.id] = true
	r.electionElapsed = 0

}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.mu.Lock()
	defer r.mu.Unlock()

	//变更状态
	r.State = StateLeader

	//设置nextInts和matchInts
	for _, peer := range r.peers {
		p := &Progress{
			Match: 0,
			Next:  uint64(len(r.RaftLog.entries)) + 1,
		}
		r.Prs[peer] = p
	}

	//发送一条空的ents消息 截断之前的消息 leader只能处理自己任期的消息
	r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{Term: r.Term, Index: uint64(len(r.RaftLog.entries) + 1)})
	if len(r.peers) == 1 {
		r.RaftLog.committed++
	}
	r.Prs[r.id].Match = uint64(len(r.RaftLog.entries))
	r.Prs[r.id].Next = uint64(len(r.RaftLog.entries)) + 1
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
// Raft 收到的所有消息将被传递到 raft.Raft.Step()
// 查看eraftpb.proto来确定什么消息应该被处理
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	if len(r.peers) == 1 && r.State == StateFollower {
		r.becomeCandidate()
		r.becomeLeader()
		return nil
	}

	switch r.State {
	case StateFollower:
		//本地消息，需要发起一次election
		if m.MsgType == pb.MessageType_MsgHup {
			r.becomeCandidate()
			for _, peer := range r.peers {
				if peer == r.id {
					continue
				} else {
					r.sendHeartbeat(peer)
				}
			}
		}

		//处理心跳或者请求投票
		if m.MsgType == pb.MessageType_MsgBeat || m.MsgType == pb.MessageType_MsgRequestVote {
			r.handleHeartbeat(m)
		}

		if m.MsgType == pb.MessageType_MsgAppend {
			r.handleAppendEntries(m)
		}
	case StateCandidate:
		//发起投票
		if m.MsgType == pb.MessageType_MsgHup {
			r.becomeCandidate()
			for _, peer := range r.peers {
				if peer == r.id {
					continue
				} else {
					r.sendHeartbeat(peer)
				}
			}
		}

		if m.MsgType == pb.MessageType_MsgRequestVote {
			r.handleHeartbeat(m)
		}

		//收到响应
		if m.MsgType == pb.MessageType_MsgRequestVoteResponse && m.Reject == false {

			r.votes[m.From] = true
			if len(r.votes) > len(r.peers)/2 {
				r.becomeLeader()
			}
		}

		if m.MsgType == pb.MessageType_MsgAppend {
			r.handleAppendEntries(m)
		}

		if m.MsgType == pb.MessageType_MsgHeartbeat {
			r.handleHeartbeat(m)
		}
	case StateLeader:
		if m.MsgType == pb.MessageType_MsgBeat {
			for _, peer := range r.peers {
				if peer == r.id {
					continue
				} else {
					r.sendHeartbeat(peer)
				}
			}
		}

		if m.MsgType == pb.MessageType_MsgRequestVote {
			r.handleHeartbeat(m)
		}

		if m.MsgType == pb.MessageType_MsgAppend {
			r.handleAppendEntries(m)
		}

		if m.MsgType == pb.MessageType_MsgPropose {
			//将msg保存到leader本地
			for _, e := range m.Entries {
				entry := pb.Entry{
					Term:  r.Term,
					Index: r.RaftLog.LastIndex() + 1,
					Data:  e.Data,
				}

				r.RaftLog.entries = append(r.RaftLog.entries, entry)
				r.Prs[r.id].Match = uint64(len(r.RaftLog.entries))
				r.Prs[r.id].Next = uint64(len(r.RaftLog.entries)) + 1
				if len(r.peers) == 1 {
					r.RaftLog.committed++
				}
			}

			//向其他的节点发送添加日志请求
			for _, peer := range r.peers {
				if peer == r.id {
					continue
				}

				r.sendAppend(peer)
			}
		}

		if m.MsgType == pb.MessageType_MsgAppendResponse {
			r.handleAppendEntriesResponse(m)
		}

	}

	return nil
}

func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	if m.Term > r.Term {
		r.State = StateFollower
	} else if m.Reject == true {
		r.Prs[m.From].Match--
		r.Prs[m.From].Next--
	} else if m.Reject == false {
		if m.LogTerm == 0 {
			r.Prs[m.From].Match = m.Index
			r.Prs[m.From].Next = m.Index + 1

			//找到match的中位数更新commit
			matchInts := make([]uint64, 0)
			for _, progress := range r.Prs {
				matchInts = append(matchInts, progress.Match)
			}

			sort.Slice(matchInts, func(i, j int) bool {
				return matchInts[i] < matchInts[j]
			})

			r.RaftLog.committed = matchInts[len(matchInts)/2]
		} else {
			r.Prs[m.From].Match = m.Index
			r.Prs[m.From].Next = r.Prs[m.From].Match + 1

			//找到match的中位数更新commit
			matchInts := make([]uint64, 0)
			for _, progress := range r.Prs {
				matchInts = append(matchInts, progress.Match)
			}

			sort.Slice(matchInts, func(i, j int) bool {
				return matchInts[i] < matchInts[j]
			})

			r.RaftLog.committed = matchInts[len(matchInts)/2]
		}

	}
}

// handleAppendEntries handle AppendEntries RPC request
// handleAppendEntries处理添加日志的RPC请求 todo
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	//处理请求添加日志消息
	//If AppendEntries RPC received from new leader: convert to follower

}

// handleHeartbeat handle Heartbeat RPC request
// handleHeartbeat处理心跳的RPC请求
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	switch m.MsgType {

	case pb.MessageType_MsgRequestVote:
		//消息的任期大于节点本身
		if m.Term > r.Term || r.Vote == m.From {
			r.becomeFollower(m.Term, m.From)
			r.Term = m.Term
			r.Vote = m.From
			r.Lead = m.From
			r.electionElapsed = 0

			//返回响应
			msg := pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse, From: r.id, To: m.From, Term: m.Term, Reject: m.Reject}
			r.msgs = append(r.msgs, msg)
		}
		//如果任期相同并且没有投过票或者已经为请求投票的透过票
		if m.Term == r.Term && r.Vote == 0 {
			r.becomeFollower(m.Term, m.From)
			r.Term = m.Term
			r.Vote = m.From
			r.Lead = m.From
			r.electionElapsed = 0

			//返回响应
			msg := pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse, From: r.id, To: m.From, Term: m.Term, Reject: m.Reject}
			r.msgs = append(r.msgs, msg)
		}
		//如果任期相同但是已经投过票并且还不是为请求投票投的票
		if m.Term == r.Term && r.Vote != m.From {
			msg := pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse, From: r.id, To: m.From, Term: m.Term, Reject: true}
			r.msgs = append(r.msgs, msg)
		}

		//任期小于节点本身的任期
	case pb.MessageType_MsgHeartbeat:
		if m.Term >= r.Term {
			r.becomeFollower(m.Term, m.From)
			r.Term = m.Term
			r.Lead = m.From
			r.electionElapsed = 0
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

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
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"math/rand"
	"sort"
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

func (r *Raft) Id() uint64 {
	return r.id
}

func (r *Raft) softState() *SoftState {
	return &SoftState{
		Lead:      r.Lead,
		RaftState: r.State,
	}
}

func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	state, confstate, err := c.Storage.InitialState()
	if err != nil {
		log.Error(err.Error())
		//panic(err)
	}

	if c.peers == nil {
		c.peers = confstate.Nodes
	}
	raft := &Raft{
		//2AA
		id:               c.ID,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		State:            StateFollower,
		electionElapsed:  0,
		heartbeatElapsed: 0,
		peers:            c.peers,
		votes:            map[uint64]bool{},
		Vote:             state.Vote,
		Term:             state.Term,
		Lead:             None,
		//2AB
		RaftLog: newLog(c.Storage),
		Prs:     make(map[uint64]*Progress), // map[uint64]*Progress{}
	}

	lastIndex := raft.RaftLog.LastIndex()
	for _, peer := range c.peers {
		raft.Prs[peer] = &Progress{Next: lastIndex + 1, Match: 0}
	}

	if c.Applied > 0 {
		raft.RaftLog.applied = c.Applied
	}

	return raft
}

func (r *Raft) sendSnapshot(to uint64) {
	var snap pb.Snapshot
	var err error
	if !IsEmptySnap(r.RaftLog.pendingSnapshot) {
		snap = *r.RaftLog.pendingSnapshot
	} else {
		snap, err = r.RaftLog.storage.Snapshot()

	}
	if err != nil {
		return
	}
	msg := pb.Message{
		MsgType:  pb.MessageType_MsgSnapshot,
		From:     r.id,
		To:       to,
		Term:     r.Term,
		Snapshot: &snap,
	}

	r.msgs = append(r.msgs, msg)
	r.Prs[to].Next = snap.Metadata.Index + 1
	return

}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
// sendAppend发送一个携带新的日志的RPC和目前已经提交的日志索引给发送的peer，返回为真如果消息发送了
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	if r.State != StateLeader {
		return false
	}

	firstIndex := r.RaftLog.FirstIndex()
	//log.Infof("to:%d prevIndex:%d firstIndex:%d", to, r.Prs[to].Next-1, firstIndex)
	if r.Prs[to].Next-1 < firstIndex-1 {
		r.sendSnapshot(to)
		return true
	}

	entries := make([]*pb.Entry, 0)

	//起始是leader的Prs里面存储的当前peer的下一条日志的位置
	if r.Prs[to].Next <= r.RaftLog.stabled {
		lastIndex, _ := r.RaftLog.storage.LastIndex()

		//说明日志在快照里面
		if lastIndex >= r.Prs[to].Next {
			e, _ := r.RaftLog.storage.Entries(r.Prs[to].Next, lastIndex+1)
			for i, entry := range e {
				if entry.Index >= r.Prs[to].Next {
					entries = append(entries, &e[i])
				}

			}
		}
	}

	for i := 0; i < len(r.RaftLog.entries); i++ {
		if r.Prs[to].Next <= r.RaftLog.entries[i].Index {
			entries = append(entries, &r.RaftLog.entries[i])
		}
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
	//log.Infof("%d send append to %d len:%d match:%d next:%d", r.id, to, len(msg.Entries), r.Prs[to].Match, r.Prs[to].Next)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
// sendHeartbeat发送一个心跳RPC给发送的peer
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).

	//todo 关于含有日志的候选者要成为leader 关于LogTerm和Index设置
	msg := pb.Message{
		To:     to,
		From:   r.id,
		Term:   r.Term,
		Commit: r.RaftLog.committed,
	}

	//根据角色的不同设置msg的type
	if r.State == StateCandidate {
		msg.MsgType = pb.MessageType_MsgRequestVote
		//获取peer的匹配日志
		msg.Index = r.Prs[to].Match
		lastIndex := r.RaftLog.LastIndex()
		lastLogTerm, _ := r.RaftLog.Term(lastIndex)

		msg.Index = lastIndex
		msg.LogTerm = lastLogTerm

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

	// Your Code Here (2A).
	switch r.State {
	//维护electionElapsed
	case StateFollower:
		r.electionElapsed++
		if r.electionElapsed >= r.electionTimeout {
			r.electionElapsed = 0 - rand.Intn(r.electionTimeout)

			msg := pb.Message{MsgType: pb.MessageType_MsgHup, From: r.id, To: r.id}
			//将msg发送到本地的msgs,msgHup表示start a new election.
			r.Step(msg)
		}
	case StateCandidate:
		r.electionElapsed++
		if r.electionElapsed >= r.electionTimeout {
			r.electionElapsed = 0 - rand.Intn(r.electionTimeout)
			msg := pb.Message{MsgType: pb.MessageType_MsgHup, From: r.id, To: r.id}
			//将msg发送到本地的msgs,msgHup表示start a new election.
			r.Step(msg)

		}

	//维护heartElapsed
	case StateLeader:

		r.heartbeatElapsed++
		//当heartbeatElapsed超过heartbeatTimeout，发送心跳
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.electionElapsed = 0
			msg := pb.Message{MsgType: pb.MessageType_MsgBeat}
			r.Step(msg)
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	if r.State == StateLeader {
		//log.Infof("raft:%d become follower at term:%d", r.id, r.Term)
	}
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
	r.Vote = None
	r.electionElapsed = 0 - rand.Intn(r.electionTimeout)
	//log.Infof("raft:%d become follower at term:%d", r.id, r.Term)

}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.Term += 1
	r.votes = make(map[uint64]bool)
	//给自己投票
	r.votes[r.id] = true
	r.Vote = r.id
	r.electionElapsed = 0 - rand.Intn(r.electionTimeout)

	//log.Infof("raft:%d become candidate at term:%d", r.id, r.Term)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term

	//变更状态
	r.State = StateLeader
	r.Lead = r.id
	//设置nextInts和matchInts
	// todo match是entries的下标 next为啥这么设置
	index := r.RaftLog.LastIndex()
	for _, peer := range r.peers {
		p := &Progress{
			Match: 0,
			Next:  index + 1,
		}
		r.Prs[peer] = p
	}

	//发送一条空的ents消息 截断之前的消息 leader只能处理自己任期的消息
	r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{Term: r.Term, Index: r.RaftLog.LastIndex() + 1})
	if len(r.peers) == 1 {
		r.RaftLog.committed++
	}
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1

	r.bcastAppend()
	//log.Infof("lastIndex:%d", r.RaftLog.LastIndex())

	//log.Infof("raft:%d become leader at term:%d", r.id, r.Term)
	//log.Info("raft log stabled:", r.RaftLog.stabled, "raft log commited:", r.RaftLog.committed, "raft log len entries", len(r.RaftLog.entries), "lastIndex", r.RaftLog.LastIndex())
	//for i := 0; i < len(r.peers); i++ {
	//	log.Info("id:", r.peers[i], "match:", r.Prs[r.peers[i]].Match, "next:", r.Prs[r.peers[i]].Next)
	//}
}

func (r *Raft) bcastAppend() {
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.sendAppend(id)
	}
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
					//todo 区别是否已经有了日志
					r.sendHeartbeat(peer)
					//log.Infof("raft:%d send msg to raft:%d", r.id, peer)
				}
			}
		}

		//处理心跳或者请求投票
		if m.MsgType == pb.MessageType_MsgHeartbeat || m.MsgType == pb.MessageType_MsgRequestVote {
			r.handleHeartbeat(m)
		}

		if m.MsgType == pb.MessageType_MsgAppend {
			//fmt.Println("append follower", "r.id", r.id)
			r.handleAppendEntries(m)
		}

		if m.MsgType == pb.MessageType_MsgSnapshot {
			r.handleSnapshot(m)
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
		if m.MsgType == pb.MessageType_MsgRequestVoteResponse {
			//log.Infof("raft:%d receive resp from %d reject:%v", r.id, m.From, m.Reject)
			if r.State != StateCandidate {
				return nil
			}
			if m.Reject == false {
				r.votes[m.From] = true

			} else if m.Reject == true {
				r.votes[m.From] = false
			}

			granted := 0
			denials := 0
			for _, vote := range r.votes {
				if vote {
					granted++
				} else {
					denials++
				}
			}

			if granted > len(r.peers)/2 {
				r.becomeLeader()
			} else if denials > len(r.peers)/2 {
				r.becomeFollower(r.Term, m.From)
			}
		}

		if m.MsgType == pb.MessageType_MsgAppend {
			//fmt.Println("append candidate")
			r.handleAppendEntries(m)
		}

		if m.MsgType == pb.MessageType_MsgHeartbeat {
			r.handleHeartbeat(m)
		}

		if m.MsgType == pb.MessageType_MsgSnapshot {
			r.handleSnapshot(m)
		}
	case StateLeader:
		if m.MsgType == pb.MessageType_MsgBeat {
			for _, peer := range r.peers {
				if peer == r.id {
					continue
				} else {
					//fmt.Println("send heart")
					r.sendHeartbeat(peer)
					//fmt.Println("done")
				}
			}
		}

		if m.MsgType == pb.MessageType_MsgRequestVote {
			r.handleHeartbeat(m)
		}

		if m.MsgType == pb.MessageType_MsgHeartbeatResponse {
			r.handleHeartbeat(m)
		}

		if m.MsgType == pb.MessageType_MsgAppend {
			//fmt.Println("append")
			r.handleAppendEntries(m)
		}

		if m.MsgType == pb.MessageType_MsgPropose {
			//fmt.Println("propose", "len m.entries", len(m.Entries))
			//log.Infof("raft:%d propose len m.entries:%d", r.id, len(m.Entries))
			//将msg保存到leader本地
			for _, e := range m.Entries {
				entry := pb.Entry{
					Term:  r.Term,
					Index: r.RaftLog.LastIndex() + 1,
					Data:  e.Data,
				}

				r.RaftLog.entries = append(r.RaftLog.entries, entry)
				r.Prs[r.id].Match = r.RaftLog.LastIndex()
				r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
				if len(r.peers) == 1 {
					//fmt.Println("aaa")
					r.RaftLog.committed++
				}
			}

			//向其他的节点发送添加日志请求
			for id := range r.Prs {
				if id == r.id {
					continue
				}

				r.sendAppend(id)
			}
		}

		if m.MsgType == pb.MessageType_MsgAppendResponse {
			r.handleAppendEntriesResponse(m)
		}

		if m.MsgType == pb.MessageType_MsgSnapshot {
			r.handleSnapshot(m)
		}

	}

	return nil
}

func (r *Raft) handleAppendEntriesResponse(m pb.Message) {

	if m.Term > r.Term {
		r.State = StateFollower
		r.Term = m.Term
	} else if m.Reject == true {
		if m.Term <= r.Term {
			next := r.Prs[m.From].Next - 1
			r.Prs[m.From].Next = min(m.Index, next)
			r.sendAppend(m.From)
			return
		}

	} else if m.Reject == false {

		r.Prs[m.From].Match = m.Index
		r.Prs[m.From].Next = m.Index + 1
		//todo 只能提交自己任期内的日志

		//找到match的中位数更新commit
		matchInts := make([]uint64, 0)

		for i := 0; i < len(r.peers); i++ {
			matchInts = append(matchInts, r.Prs[r.peers[i]].Match)
		}

		sort.Slice(matchInts, func(i, j int) bool {
			return matchInts[i] < matchInts[j]
		})

		if r.maybeCommit(matchInts[(len(matchInts)-1)/2]) {
			r.bcastAppend()
		}

	}
}

func (r *Raft) maybeCommit(newCommited uint64) bool {
	if newCommited-1 >= uint64(len(r.RaftLog.entries)) {
		//fmt.Println("new ", newCommited, len(r.RaftLog.entries), "r.id", r.id, "state", r.State, "commited", r.RaftLog.committed, "stabled", r.RaftLog.stabled)
	}

	term, _ := r.RaftLog.Term(newCommited)

	if newCommited > r.RaftLog.committed && term == r.Term {
		//fmt.Println("change", "newCommited", newCommited, " r.RaftLog.committed", r.RaftLog.committed)
		r.changeCommited(newCommited)
		return true
	}

	return false
}

func (r *Raft) changeCommited(commited uint64) {
	//log.Infof("r.id:%d committed change to %d", r.id, commited)
	r.RaftLog.committed = commited
}

// handleAppendEntries handle AppendEntries RPC request
// handleAppendEntries处理添加日志的RPC请求
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	//处理请求添加日志消息
	//If AppendEntries RPC received from new leader: convert to follower
	r.heartbeatTimeout = 0
	msg := pb.Message{MsgType: pb.MessageType_MsgAppendResponse, From: r.id, To: m.From, Index: m.Index, Term: r.Term}

	term, err := r.RaftLog.Term(m.Index)
	if err != nil {
		msg.Reject = true
		r.msgs = append(r.msgs, msg)
		return
	}
	//处理日志 日志任期大于节点的本地日志并且前面的日志的term能够匹配
	if m.Term >= r.Term && term == m.LogTerm {
		r.Term = m.Term
		r.Lead = m.From
		r.State = StateFollower

		for _, entry := range m.Entries {

			lastIndex := r.RaftLog.LastIndex()

			if entry.Index <= lastIndex {
				//PreTerm, err = r.RaftLog.Term(entry.Index)
				if err != nil {
					log.Error(err)
					panic(err)
				}
				Term, _ := r.RaftLog.Term(entry.Index)
				//删除index相同但是term不同的日志 并且更改stabled
				if Term != entry.Term {
					firstIndex := r.RaftLog.FirstIndex()
					//截断
					if len(r.RaftLog.entries) != 0 && int64(m.Index-firstIndex+1) >= 0 {
						r.RaftLog.entries = r.RaftLog.entries[0 : m.Index-firstIndex+1]
					}
					lastIndex = r.RaftLog.LastIndex()
					r.RaftLog.entries = append(r.RaftLog.entries, *entry)
					r.RaftLog.stabled = m.Index
				}
			} else {
				// 超出了节点匹配的日志
				r.RaftLog.entries = append(r.RaftLog.entries, *entry)

			}

		}

		//更新commited
		if m.Commit > r.RaftLog.committed {
			// commitIndex = min(leaderCommit, index of last new entry)
			lastnewi := uint64(len(m.Entries)) + m.Index //index of last new entry
			//fmt.Println("lastnewi:", lastnewi, "len(m.Entries):", len(m.Entries), "m.commit:", m.Commit, "r.id:", r.id, "r.state:", r.State)
			r.RaftLog.committed = min(lastnewi, m.Commit)
			//log.Infof("r.id:%d committed change to %d", r.id, min(lastnewi, m.Commit))
		}
	} else {
		//Println("id:", r.id, "reject ", "m.term:", m.Term, "r.term:", r.Term, "term:", term, "m.logTerm:", m.LogTerm, "m.index", m.Index)
		//log.Infof("msg.Reject = true")
		msg.Reject = true
	}

	//没有日志 就处理心跳
	if m.Term >= r.Term && r.Vote == m.From {
		r.becomeFollower(m.Term, m.From)
		r.heartbeatTimeout = 0
	}

	//todo 可能需要follower的commit
	if len(r.RaftLog.entries) > 0 {
		msg.Index = r.RaftLog.entries[len(r.RaftLog.entries)-1].Index
	} else {
		msg.Index = r.RaftLog.stabled
	}

	r.msgs = append(r.msgs, msg)

	//fmt.Println("id:", r.id, "raft log commit", r.RaftLog.committed, "raft log stabled", r.RaftLog.stabled, "msg index", msg.Index, "raft log lastIndex", r.RaftLog.LastIndex(), "len entries", len(r.RaftLog.entries))

}

// handleHeartbeat handle Heartbeat RPC request
// handleHeartbeat处理心跳的RPC请求
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	switch m.MsgType {

	case pb.MessageType_MsgRequestVote:

		//消息的任期大于节点本身
		if m.Term < r.Term {
			msg := pb.Message{
				MsgType: pb.MessageType_MsgRequestVoteResponse,
				From:    r.id,
				To:      m.From,
				Term:    r.Term,
				Reject:  true,
			}
			r.msgs = append(r.msgs, msg)
			return
		}
		if r.State != StateFollower && m.Term > r.Term {
			r.becomeFollower(m.Term, None)
		}
		if r.Term < m.Term {
			r.Vote = None
			r.Term = m.Term
		}
		// If votedFor is null or candidateId
		if r.Vote == None || r.Vote == m.From {
			// when sender's last term is greater than receiver's term
			// or sender's last term is equal to receiver's term
			// but sender's last index is greater than or equal to follower's.
			lastIndex := r.RaftLog.LastIndex()
			lastTerm, _ := r.RaftLog.Term(lastIndex)
			if m.LogTerm > lastTerm ||
				(m.LogTerm == lastTerm && m.Index >= lastIndex) {
				r.Vote = m.From
				if r.Term < m.Term {
					r.Term = m.Term
				}
				msg := pb.Message{
					MsgType: pb.MessageType_MsgRequestVoteResponse,
					From:    r.id,
					To:      m.From,
					Term:    r.Term,
					Reject:  false,
				}
				r.msgs = append(r.msgs, msg)
				return
			}
		}
		msg := pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			From:    r.id,
			To:      m.From,
			Term:    r.Term,
			Reject:  true,
		}
		r.msgs = append(r.msgs, msg)
		//任期小于节点本身的任期
	case pb.MessageType_MsgHeartbeat:
		if m.Term < r.Term {
			msg := pb.Message{
				MsgType: pb.MessageType_MsgHeartbeatResponse,
				From:    r.id,
				To:      m.From,
				Term:    r.Term,
				Reject:  true,
				Commit:  r.RaftLog.committed,
				Index:   r.RaftLog.stabled,
			}
			r.msgs = append(r.msgs, msg)
			return
		}

		if m.Term > r.Term {
			r.Term = m.Term
		}

		r.becomeFollower(m.Term, m.From)

		r.Lead = m.From
		r.electionElapsed -= r.heartbeatTimeout
		if m.Commit > r.RaftLog.committed {
			msg := pb.Message{
				MsgType: pb.MessageType_MsgHeartbeatResponse,
				To:      m.From,
				From:    r.id,
				Term:    r.Term,
				Commit:  r.RaftLog.committed,
				Index:   r.RaftLog.stabled,
				Reject:  false,
			}

			r.msgs = append(r.msgs, msg)
		}

	case pb.MessageType_MsgHeartbeatResponse:
		if r.State != StateLeader {
			return
		}

		if m.Reject || m.Commit < r.RaftLog.committed {
			r.sendAppend(m.From)
		}
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	meta := m.Snapshot.Metadata

	if meta.Index <= r.RaftLog.committed || m.Term < r.Term {
		return
	}

	r.Lead = m.From
	r.RaftLog.applied = m.Snapshot.Metadata.Index
	r.RaftLog.committed = m.Snapshot.Metadata.Index
	r.RaftLog.stabled = m.Snapshot.Metadata.Index
	r.Term = m.Term

	//发送过来的快照可能只有当前entries的一部分，将已经存储为快照的一部分截掉
	if len(r.RaftLog.entries) > 0 {
		if meta.Index > r.RaftLog.LastIndex() {
			r.RaftLog.entries = nil
		} else if meta.Index >= r.RaftLog.FirstIndex() {
			r.RaftLog.entries = r.RaftLog.entries[meta.Index-r.RaftLog.FirstIndex():]
		}
	}

	nds := m.Snapshot.Metadata.ConfState.Nodes

	r.Prs = make(map[uint64]*Progress)
	for _, nd := range nds {
		r.Prs[nd] = &Progress{Match: 0, Next: r.RaftLog.LastIndex() + 1}
	}

	r.RaftLog.pendingSnapshot = m.Snapshot
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

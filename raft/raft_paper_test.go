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

/*
This file contains tests which verify that the scenarios described
in the raft paper (https://raft.github.io/raft.pdf) are handled by the
raft implementation correctly. Each test focuses on several sentences
written in the paper.

Each test is composed of three parts: init, test and check.
Init part uses simple and understandable way to simulate the init state.
Test part uses Step function to generate the scenario. Check part checks
outgoing messages and state.
*/
package raft

import (
	"fmt"
	"reflect"
	"sort"
	"testing"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// 测试follower从消息中更新任期
func TestFollowerUpdateTermFromMessage2AA(t *testing.T) {
	testUpdateTermFromMessage(t, StateFollower)
}
func TestCandidateUpdateTermFromMessage2AA(t *testing.T) {
	testUpdateTermFromMessage(t, StateCandidate)
}
func TestLeaderUpdateTermFromMessage2AA(t *testing.T) {
	testUpdateTermFromMessage(t, StateLeader)
}

// testUpdateTermFromMessage tests that if one server’s current term is
// smaller than the other’s, then it updates its current term to the larger
// value. If a candidate or leader discovers that its term is out of date,
// it immediately reverts to follower state.
// Reference: section 5.1
// 更新从消息更新任期测试 如果一个服务器的当前任期小于其他的，接着它更新自己的目前任期变为更大的值
// 如果一个候选者或者领导者发送它的任期已经过期，那么它就会恢复到follower状态
func testUpdateTermFromMessage(t *testing.T, state StateType) {
	r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewMemoryStorage())
	switch state {
	case StateFollower:
		r.becomeFollower(1, 2)
	case StateCandidate:
		r.becomeCandidate()
	case StateLeader:
		r.becomeCandidate()
		r.becomeLeader()
	}

	r.Step(pb.Message{MsgType: pb.MessageType_MsgAppend, Term: 2})

	if r.Term != 2 {
		t.Errorf("term = %d, want %d", r.Term, 2)
	}
	if r.State != StateFollower {
		t.Errorf("state = %v, want %v", r.State, StateFollower)
	}
}

// TestStartAsFollower tests that when servers start up, they begin as followers.
// Reference: section 5.2
func TestStartAsFollower2AA(t *testing.T) {
	r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewMemoryStorage())
	if r.State != StateFollower {
		t.Errorf("state = %s, want %s", r.State, StateFollower)
	}
}

// TestLeaderBcastBeat tests that if the leader receives a heartbeat tick,
// it will send a MessageType_MsgHeartbeat with m.Index = 0, m.LogTerm=0 and empty entries
// as heartbeat to all followers.
// Reference: section 5.2
// 测试领导者广播心跳测试的是如果leader收到了一个心跳的tick，它将会发送一个消息类型为心跳并且index和term为0并且空日志作为心跳给其他的follower
func TestLeaderBcastBeat2AA(t *testing.T) {
	// heartbeat interval
	//心跳间隔
	hi := 1
	//让id为1的称为领导者
	r := newTestRaft(1, []uint64{1, 2, 3}, 10, hi, NewMemoryStorage())
	r.becomeCandidate()
	r.becomeLeader()

	//消息的目的是为了append日志给leader
	r.Step(pb.Message{MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{}}})
	r.readMessages() // clear message

	for i := 0; i < hi; i++ {
		r.tick()
	}

	msgs := r.readMessages()
	sort.Sort(messageSlice(msgs))
	wmsgs := []pb.Message{
		{From: 1, To: 2, Term: 1, MsgType: pb.MessageType_MsgHeartbeat},
		{From: 1, To: 3, Term: 1, MsgType: pb.MessageType_MsgHeartbeat},
	}
	if !reflect.DeepEqual(msgs, wmsgs) {
		t.Errorf("msgs = %v, \n                      want %v", msgs, wmsgs)
	}
}

func TestFollowerStartElection2AA(t *testing.T) {
	testNonleaderStartElection(t, StateFollower)
}
func TestCandidateStartNewElection2AA(t *testing.T) {
	testNonleaderStartElection(t, StateCandidate)
}

// testNonleaderStartElection tests that if a follower receives no communication
// over election timeout, it begins an election to choose a new leader. It
// increments its current term and transitions to candidate state. It then
// votes for itself and issues RequestVote RPCs in parallel to each of the
// other servers in the cluster.
// Reference: section 5.2
// Also if a candidate fails to obtain a majority, it will time out and
// start a new election by incrementing its term and initiating another
// round of RequestVote RPCs.
// Reference: section 5.2
// 没有leader开始选举测试 如果一个follower没有收到心跳超过了选举时间，那么它就会开始选举并且成为一个新的leadeer。
// 它会自增自己的任期并且转换自己为候选者。它将会为自己投票并且并行发布请求投票rpc给集群中的其他server
// 同意它也有可能失败获取大部分选票，它将会超时并且重新开启一轮新的选举通过增加它的任期、重新初始新的一轮请求投票rpc
func testNonleaderStartElection(t *testing.T, state StateType) {
	// election timeout
	et := 10
	r := newTestRaft(1, []uint64{1, 2, 3}, et, 1, NewMemoryStorage())
	switch state {
	case StateFollower:
		r.becomeFollower(1, 2)
	case StateCandidate:
		r.becomeCandidate()
	}

	for i := 1; i < 2*et; i++ {
		r.tick()
	}

	if r.Term != 2 {
		t.Errorf("term = %d, want 2", r.Term)
	}
	if r.State != StateCandidate {
		t.Errorf("state = %s, want %s", r.State, StateCandidate)
	}
	if !r.votes[r.id] {
		t.Errorf("vote for self = false, want true")
	}
	msgs := r.readMessages()
	sort.Sort(messageSlice(msgs))
	wmsgs := []pb.Message{
		{From: 1, To: 2, Term: 2, MsgType: pb.MessageType_MsgRequestVote},
		{From: 1, To: 3, Term: 2, MsgType: pb.MessageType_MsgRequestVote},
	}
	if !reflect.DeepEqual(msgs, wmsgs) {
		t.Errorf("msgs = %v, want %v", msgs, wmsgs)
	}
}

// TestLeaderElectionInOneRoundRPC tests all cases that may happen in
// leader election during one round of RequestVote RPC:
// a) it wins the election
// b) it loses the election
// c) it is unclear about the result
// Reference: section 5.2
// 测试领导者选举在一轮rpc中，测试所有可能会发送在一轮leader election的情况
// a) 赢得选举
// b）失败
// c）对结果不清楚
func TestLeaderElectionInOneRoundRPC2AA(t *testing.T) {
	tests := []struct {
		size  int
		votes map[uint64]bool
		state StateType
	}{
		// win the election when receiving votes from a majority of the servers
		//{1, map[uint64]bool{}, StateLeader},
		//{3, map[uint64]bool{2: true, 3: true}, StateLeader},
		//{3, map[uint64]bool{2: true}, StateLeader},
		//{5, map[uint64]bool{2: true, 3: true, 4: true, 5: true}, StateLeader},
		//{5, map[uint64]bool{2: true, 3: true, 4: true}, StateLeader},
		//{5, map[uint64]bool{2: true, 3: true}, StateLeader},

		// stay in candidate if it does not obtain the majority
		//{3, map[uint64]bool{}, StateCandidate},
		//{5, map[uint64]bool{2: true}, StateCandidate},
		{5, map[uint64]bool{2: false, 3: false}, StateCandidate},
		//{5, map[uint64]bool{}, StateCandidate},
	}
	for i, tt := range tests {
		r := newTestRaft(1, idsBySize(tt.size), 10, 1, NewMemoryStorage())

		r.Step(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})
		for id, vote := range tt.votes {
			r.Step(pb.Message{From: id, To: 1, Term: r.Term, MsgType: pb.MessageType_MsgRequestVoteResponse, Reject: !vote})
		}

		if r.State != tt.state {
			t.Errorf("#%d: state = %s, want %s", i, r.State, tt.state)
		}
		if g := r.Term; g != 1 {
			t.Errorf("#%d: term = %d, want %d", i, g, 1)
		}
	}
}

// TestFollowerVote tests that each follower will vote for at most one
// candidate in a given term, on a first-come-first-served basis.
// Reference: section 5.2
// 测试follower投票 每个follower将会投票给至多一个候选者在一个给定的任期中,先到先得
func TestFollowerVote2AA(t *testing.T) {
	tests := []struct {
		vote    uint64
		nvote   uint64
		wreject bool
	}{
		{None, 1, false},
		{None, 2, false},
		{1, 1, false},
		{2, 2, false},
		{1, 2, true},
		{2, 1, true},
	}
	for i, tt := range tests {
		r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewMemoryStorage())
		r.Term = 1
		r.Vote = tt.vote

		r.Step(pb.Message{From: tt.nvote, To: 1, Term: 1, MsgType: pb.MessageType_MsgRequestVote})

		msgs := r.readMessages()
		wmsgs := []pb.Message{
			{From: 1, To: tt.nvote, Term: 1, MsgType: pb.MessageType_MsgRequestVoteResponse, Reject: tt.wreject},
		}
		if !reflect.DeepEqual(msgs, wmsgs) {
			t.Errorf("#%d: msgs = %v, want %v", i, msgs, wmsgs)
		}
	}
}

// TestCandidateFallback tests that while waiting for votes,
// if a candidate receives an AppendEntries RPC from another server claiming
// to be leader whose term is at least as large as the candidate's current term,
// it recognizes the leader as legitimate and returns to follower state.
// Reference: section 5.2
func TestCandidateFallback2AA(t *testing.T) {
	tests := []pb.Message{
		{From: 2, To: 1, Term: 1, MsgType: pb.MessageType_MsgAppend},
		{From: 2, To: 1, Term: 2, MsgType: pb.MessageType_MsgAppend},
	}
	for i, tt := range tests {
		r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewMemoryStorage())
		r.Step(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})
		if r.State != StateCandidate {
			t.Fatalf("unexpected state = %s, want %s", r.State, StateCandidate)
		}

		r.Step(tt)

		if g := r.State; g != StateFollower {
			t.Errorf("#%d: state = %s, want %s", i, g, StateFollower)
		}
		if g := r.Term; g != tt.Term {
			t.Errorf("#%d: term = %d, want %d", i, g, tt.Term)
		}
	}
}

func TestFollowerElectionTimeoutRandomized2AA(t *testing.T) {
	testNonleaderElectionTimeoutRandomized(t, StateFollower)
}
func TestCandidateElectionTimeoutRandomized2AA(t *testing.T) {
	testNonleaderElectionTimeoutRandomized(t, StateCandidate)
}

// testNonleaderElectionTimeoutRandomized tests that election timeout for
// follower or candidate is randomized.
// Reference: section 5.2
// 测试无领导者选举超时随机 测试随机对follower和candidate超时
func testNonleaderElectionTimeoutRandomized(t *testing.T, state StateType) {
	et := 10
	r := newTestRaft(1, []uint64{1, 2, 3}, et, 1, NewMemoryStorage())
	timeouts := make(map[int]bool)
	for round := 0; round < 50*et; round++ {
		switch state {
		case StateFollower:
			r.becomeFollower(r.Term+1, 2)
		case StateCandidate:
			r.becomeCandidate()
		}

		time := 0
		//没有收到消息，意味着没有心跳
		for len(r.readMessages()) == 0 {
			r.tick()
			time++
		}
		timeouts[time] = true
	}

	for d := et + 1; d < 2*et; d++ {
		if !timeouts[d] {
			t.Errorf("timeout in %d ticks should happen", d)
		}
	}
}

func TestFollowersElectionTimeoutNonconflict2AA(t *testing.T) {
	testNonleadersElectionTimeoutNonconflict(t, StateFollower)
}
func TestCandidatesElectionTimeoutNonconflict2AA(t *testing.T) {
	testNonleadersElectionTimeoutNonconflict(t, StateCandidate)
}

// testNonleadersElectionTimeoutNonconflict tests that in most cases only a
// single server(follower or candidate) will time out, which reduces the
// likelihood of split vote in the new election.
// Reference: section 5.2
// 测试无领导者选举超时没有冲突 测试在大多数情况下只有一个server(候选者或者follower)将会超时，降低了在新的选举分裂投票的可能性
func testNonleadersElectionTimeoutNonconflict(t *testing.T, state StateType) {
	et := 10
	size := 5
	rs := make([]*Raft, size)
	ids := idsBySize(size)
	for k := range rs {
		rs[k] = newTestRaft(ids[k], ids, et, 1, NewMemoryStorage())
	}
	conflicts := 0
	for round := 0; round < 1000; round++ {
		for _, r := range rs {
			switch state {
			case StateFollower:
				r.becomeFollower(r.Term+1, None)
			case StateCandidate:
				r.becomeCandidate()
			}
		}

		timeoutNum := 0
		for timeoutNum == 0 {
			for _, r := range rs {
				r.tick()
				if len(r.readMessages()) > 0 {
					timeoutNum++
				}
			}
		}
		// several rafts time out at the same tick
		if timeoutNum > 1 {
			conflicts++
		}
	}

	if g := float64(conflicts) / 1000; g > 0.3 {
		t.Errorf("probability of conflicts = %v, want <= 0.3", g)
	}
}

// TestLeaderStartReplication tests that when receiving client proposals,
// the leader appends the proposal to its log as a new entry, then issues
// AppendEntries RPCs in parallel to each of the other servers to replicate
// the entry. Also, when sending an AppendEntries RPC, the leader includes
// the index and term of the entry in its log that immediately precedes
// the new entries.
// Also, it writes the new entry into stable storage.
// Reference: section 5.3
// TestLeaderStartReplication 测试当收到client的提议的时候，leader将提议append到它的log作为新的entry
// 然后向其他每个服务器并行发出追加条目 RPC 以复制条目
// 当然 当发送一个AppendEntries RPC包括紧接在其日志之前的条目的索引和任期
// 同时也要写到稳定的存储中
func TestLeaderStartReplication2AB(t *testing.T) {
	s := NewMemoryStorage()
	r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, s)
	r.becomeCandidate()
	r.becomeLeader()
	commitNoopEntry(r, s)

	li := r.RaftLog.LastIndex()

	ents := []*pb.Entry{{Data: []byte("some data")}}
	r.Step(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: ents})

	if g := r.RaftLog.LastIndex(); g != li+1 {
		t.Errorf("lastIndex = %d, want %d", g, li+1)
	}
	if g := r.RaftLog.committed; g != li {
		t.Errorf("committed = %d, want %d", g, li)
	}
	msgs := r.readMessages()
	sort.Sort(messageSlice(msgs))
	ent := pb.Entry{Index: li + 1, Term: 1, Data: []byte("some data")}
	wents := []pb.Entry{ent}
	wmsgs := []pb.Message{
		{From: 1, To: 2, Term: 1, MsgType: pb.MessageType_MsgAppend, Index: li, LogTerm: 1, Entries: []*pb.Entry{&ent}, Commit: li},
		{From: 1, To: 3, Term: 1, MsgType: pb.MessageType_MsgAppend, Index: li, LogTerm: 1, Entries: []*pb.Entry{&ent}, Commit: li},
	}
	if !reflect.DeepEqual(msgs, wmsgs) {
		t.Errorf("msgs = %+v\n,                     want %+v", msgs, wmsgs)
	}
	if g := r.RaftLog.unstableEntries(); !reflect.DeepEqual(g, wents) {
		t.Errorf("ents = %+v\n,                     want %+v", g, wents)
	}
}

// TestLeaderCommitEntry tests that when the entry has been safely replicated,
// the leader gives out the applied entries, which can be applied to its state
// machine.
// Also, the leader keeps track of the highest index it knows to be committed,
// and it includes that index in future AppendEntries RPCs so that the other
// servers eventually find out.
// Reference: section 5.3
// TestLeaderCommitEntry 测试当条目被安全复制时，
// 领导者给出应用的条目，这些条目可以应用于其状态
// 机器。
// 此外，领导者会跟踪它所知道的要承诺的最高指数，
// 它将该索引包含在将来的 AppendEntries RPC 中，以便其他
// 服务器最终会发现。
func TestLeaderCommitEntry2AB(t *testing.T) {
	s := NewMemoryStorage()
	r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, s)
	r.becomeCandidate()
	r.becomeLeader()
	commitNoopEntry(r, s)
	li := r.RaftLog.LastIndex()
	r.Step(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{Data: []byte("some data")}}})

	for _, m := range r.readMessages() {
		r.Step(acceptAndReply(m))
	}

	if g := r.RaftLog.committed; g != li+1 {
		t.Errorf("committed = %d, want %d", g, li+1)
	}
	wents := []pb.Entry{{Index: li + 1, Term: 1, Data: []byte("some data")}}
	if g := r.RaftLog.nextEnts(); !reflect.DeepEqual(g, wents) {
		t.Errorf("nextEnts = %+v\n,                         want %+v", g, wents)
	}
	msgs := r.readMessages()
	sort.Sort(messageSlice(msgs))
	for i, m := range msgs {
		if w := uint64(i + 2); m.To != w {
			t.Errorf("to = %d, want %d", m.To, w)
		}
		if m.MsgType != pb.MessageType_MsgAppend {
			t.Errorf("type = %v, want %v", m.MsgType, pb.MessageType_MsgAppend)
		}
		if m.Commit != li+1 {
			t.Errorf("commit = %d, want %d", m.Commit, li+1)
		}
	}
}

// TestLeaderAcknowledgeCommit tests that a log entry is committed once the
// leader that created the entry has replicated it on a majority of the servers.
// Reference: section 5.3
func TestLeaderAcknowledgeCommit2AB(t *testing.T) {
	tests := []struct {
		size      int
		acceptors map[uint64]bool
		wack      bool
	}{
		{1, nil, true},
		{3, nil, false},
		{3, map[uint64]bool{2: true}, true},
		{3, map[uint64]bool{2: true, 3: true}, true},
		{5, nil, false},
		{5, map[uint64]bool{2: true}, false},
		{5, map[uint64]bool{2: true, 3: true}, true},
		{5, map[uint64]bool{2: true, 3: true, 4: true}, true},
		{5, map[uint64]bool{2: true, 3: true, 4: true, 5: true}, true},
	}
	for i, tt := range tests {
		s := NewMemoryStorage()
		r := newTestRaft(1, idsBySize(tt.size), 10, 1, s)
		r.becomeCandidate()
		r.becomeLeader()
		commitNoopEntry(r, s)
		li := r.RaftLog.LastIndex()
		r.Step(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{Data: []byte("some data")}}})

		for _, m := range r.readMessages() {
			if tt.acceptors[m.To] {
				r.Step(acceptAndReply(m))
			}
		}

		if g := r.RaftLog.committed > li; g != tt.wack {
			t.Errorf("#%d: ack commit = %v, want %v", i, g, tt.wack)
		}
	}
}

// TestLeaderCommitPrecedingEntries tests that when leader commits a log entry,
// it also commits all preceding entries in the leader’s log, including
// entries created by previous leaders.
// Also, it applies the entry to its local state machine (in log order).
// Reference: section 5.3
// 当leader提交一个日志 它也提交了所有前面的条目，包括先前leader创建的
func TestLeaderCommitPrecedingEntries2AB(t *testing.T) {
	tests := [][]pb.Entry{
		{},
		{{Term: 2, Index: 1}},
		{{Term: 1, Index: 1}, {Term: 2, Index: 2}},
		{{Term: 1, Index: 1}},
	}
	for i, tt := range tests {
		storage := NewMemoryStorage()
		storage.Append(tt)
		r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, storage)
		r.Term = 2
		r.becomeCandidate()
		r.becomeLeader()
		r.Step(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{Data: []byte("some data")}}})

		for _, m := range r.readMessages() {
			r.Step(acceptAndReply(m))
		}

		li := uint64(len(tt))
		wents := append(tt, pb.Entry{Term: 3, Index: li + 1}, pb.Entry{Term: 3, Index: li + 2, Data: []byte("some data")})
		if g := r.RaftLog.nextEnts(); !reflect.DeepEqual(g, wents) {
			t.Errorf("#%d: ents = %+v\n,                         want %+v", i, g, wents)
		}
	}
}

// TestFollowerCommitEntry tests that once a follower learns that a log entry
// is committed, it applies the entry to its local state machine (in log order).
// Reference: section 5.3
// TestFollowerCommitEntry 测试一旦一个follower知道了一个日志条目已经被提交了，它会将条目应用到它本地的状态机器
func TestFollowerCommitEntry2AB(t *testing.T) {
	tests := []struct {
		ents   []*pb.Entry
		commit uint64
	}{
		{
			[]*pb.Entry{
				{Term: 1, Index: 1, Data: []byte("some data")},
			},
			1,
		},
		{
			[]*pb.Entry{
				{Term: 1, Index: 1, Data: []byte("some data")},
				{Term: 1, Index: 2, Data: []byte("some data2")},
			},
			2,
		},
		{
			[]*pb.Entry{
				{Term: 1, Index: 1, Data: []byte("some data2")},
				{Term: 1, Index: 2, Data: []byte("some data")},
			},
			2,
		},
		{
			[]*pb.Entry{
				{Term: 1, Index: 1, Data: []byte("some data")},
				{Term: 1, Index: 2, Data: []byte("some data2")},
			},
			1,
		},
	}
	for i, tt := range tests {
		r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewMemoryStorage())
		r.becomeFollower(1, 2)

		r.Step(pb.Message{From: 2, To: 1, MsgType: pb.MessageType_MsgAppend, Term: 1, Entries: tt.ents, Commit: tt.commit})

		if g := r.RaftLog.committed; g != tt.commit {
			t.Errorf("#%d: committed = %d, want %d", i, g, tt.commit)
		}
		wents := make([]pb.Entry, 0, tt.commit)
		for _, ent := range tt.ents[:int(tt.commit)] {
			wents = append(wents, *ent)
		}
		if g := r.RaftLog.nextEnts(); !reflect.DeepEqual(g, wents) {
			t.Errorf("#%d: nextEnts = %v,                          \n want %v", i, g, wents)
		}
	}
}

// TestFollowerCheckMessageType_MsgAppend tests that if the follower does not find an
// entry in its log with the same index and term as the one in AppendEntries RPC,
// then it refuses the new entries. Otherwise it replies that it accepts the
// append entries.
// Reference: section 5.3
// TestFollowerCheckMessageType_MsgAppend 测试follower是否在AppendEntries RPC没有找到相同索引和任期的log
// 接着它拒绝新的日志 否则回答它并且接受
func TestFollowerCheckMessageType_MsgAppend2AB(t *testing.T) {
	ents := []pb.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}}
	tests := []struct {
		term    uint64
		index   uint64
		wreject bool
	}{
		// match with committed entries
		// 和提交的日志匹配
		{0, 0, false},
		{ents[0].Term, ents[0].Index, false},

		// match with uncommitted entries
		//和未提交的日志匹配
		{ents[1].Term, ents[1].Index, false},

		// unmatch with existing entry
		// 和存在的日志不匹配
		{ents[0].Term, ents[1].Index, true},

		// unexisting entry
		// 日志不存在
		{ents[1].Term + 1, ents[1].Index + 1, true},
	}
	for i, tt := range tests {
		storage := NewMemoryStorage()
		storage.Append(ents)
		r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, storage)
		r.RaftLog.committed = 1
		r.becomeFollower(2, 2)
		msgs := r.readMessages() // clear message

		r.Step(pb.Message{From: 2, To: 1, MsgType: pb.MessageType_MsgAppend, Term: 2, LogTerm: tt.term, Index: tt.index})

		msgs = r.readMessages()
		if len(msgs) != 1 {
			t.Errorf("#%d: len(msgs) = %+v, want %+v", i, len(msgs), 1)
		}
		if msgs[0].Term != 2 {
			t.Errorf("#%d: term = %+v, want %+v", i, msgs[0].Term, 2)
		}
		if msgs[0].Reject != tt.wreject {
			t.Errorf("#%d: reject = %+v, want %+v", i, msgs[0].Reject, tt.wreject)
		}
	}
}

// TestFollowerAppendEntries tests that when AppendEntries RPC is valid,
// the follower will delete the existing conflict entry and all that follow it,
// and append any new entries not already in the log.
// Also, it writes the new entry into stable storage.
// Reference: section 5.3
// TestFollowerAppendEntries 测试当添加日志的rpc是无效的 follower将会删除存在冲突的日志并且添加不存在的日志
// 同时 也会写入新的日志到稳定的存储中
func TestFollowerAppendEntries2AB(t *testing.T) {
	tests := []struct {
		index, term uint64
		lterm       uint64
		ents        []*pb.Entry
		wents       []*pb.Entry
		wunstable   []*pb.Entry
	}{
		{
			2, 2, 3,
			[]*pb.Entry{{Term: 3, Index: 3}},
			[]*pb.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}, {Term: 3, Index: 3}},
			[]*pb.Entry{{Term: 3, Index: 3}},
		},
		{
			1, 1, 4,
			[]*pb.Entry{{Term: 3, Index: 2}, {Term: 4, Index: 3}},
			[]*pb.Entry{{Term: 1, Index: 1}, {Term: 3, Index: 2}, {Term: 4, Index: 3}},
			[]*pb.Entry{{Term: 3, Index: 2}, {Term: 4, Index: 3}},
		},
		{
			0, 0, 2,
			[]*pb.Entry{{Term: 1, Index: 1}},
			[]*pb.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}},
			[]*pb.Entry{},
		},
		{
			0, 0, 3,
			[]*pb.Entry{{Term: 3, Index: 1}},
			[]*pb.Entry{{Term: 3, Index: 1}},
			[]*pb.Entry{{Term: 3, Index: 1}},
		},
	}
	for i, tt := range tests {
		storage := NewMemoryStorage()
		storage.Append([]pb.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}})
		r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, storage)
		r.becomeFollower(2, 2)

		r.Step(pb.Message{From: 2, To: 1, MsgType: pb.MessageType_MsgAppend, Term: tt.lterm, LogTerm: tt.term, Index: tt.index, Entries: tt.ents})

		wents := make([]pb.Entry, 0, len(tt.wents))
		for _, ent := range tt.wents {
			wents = append(wents, *ent)
		}
		if g := r.RaftLog.entries; !reflect.DeepEqual(g, wents) {
			t.Errorf("#%d: ents = %+v,\n                          want %+v", i, g, wents)
		}
		var wunstable []pb.Entry
		if tt.wunstable != nil {
			wunstable = make([]pb.Entry, 0, len(tt.wunstable))
		}
		for _, ent := range tt.wunstable {
			wunstable = append(wunstable, *ent)
		}
		if g := r.RaftLog.unstableEntries(); !reflect.DeepEqual(g, wunstable) {
			t.Errorf("#%d: unstableEnts = %+v,\n                                  want %+v", i, g, wunstable)
		}
	}
}

// TestLeaderSyncFollowerLog tests that the leader could bring a follower's log
// into consistency with its own.
// Reference: section 5.3, figure 7
// TestLeaderSyncFollowerLog 测试 可以带来关注者的日志与自己保持一致。
func TestLeaderSyncFollowerLog2AB(t *testing.T) {
	ents := []pb.Entry{
		{},
		{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3},
		{Term: 4, Index: 4}, {Term: 4, Index: 5},
		{Term: 5, Index: 6}, {Term: 5, Index: 7},
		{Term: 6, Index: 8}, {Term: 6, Index: 9}, {Term: 6, Index: 10},
	}
	term := uint64(8)
	tests := [][]pb.Entry{
		{
			{},
			{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3},
			{Term: 4, Index: 4}, {Term: 4, Index: 5},
			{Term: 5, Index: 6}, {Term: 5, Index: 7},
			{Term: 6, Index: 8}, {Term: 6, Index: 9},
		},
		{
			{},
			{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3},
			{Term: 4, Index: 4},
		},
		{
			{},
			{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3},
			{Term: 4, Index: 4}, {Term: 4, Index: 5},
			{Term: 5, Index: 6}, {Term: 5, Index: 7},
			{Term: 6, Index: 8}, {Term: 6, Index: 9}, {Term: 6, Index: 10}, {Term: 6, Index: 11},
		},
		{
			{},
			{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3},
			{Term: 4, Index: 4}, {Term: 4, Index: 5},
			{Term: 5, Index: 6}, {Term: 5, Index: 7},
			{Term: 6, Index: 8}, {Term: 6, Index: 9}, {Term: 6, Index: 10},
			{Term: 7, Index: 11}, {Term: 7, Index: 12},
		},
		{
			{},
			{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3},
			{Term: 4, Index: 4}, {Term: 4, Index: 5}, {Term: 4, Index: 6}, {Term: 4, Index: 7},
		},
		{
			{},
			{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3},
			{Term: 2, Index: 4}, {Term: 2, Index: 5}, {Term: 2, Index: 6},
			{Term: 3, Index: 7}, {Term: 3, Index: 8}, {Term: 3, Index: 9}, {Term: 3, Index: 10}, {Term: 3, Index: 11},
		},
	}

	for i, tt := range tests {
		//创建一个包括ents日志的leader节点
		leadStorage := NewMemoryStorage()
		leadStorage.Append(ents)
		lead := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, leadStorage)
		lead.Term = term
		lead.RaftLog.committed = lead.RaftLog.LastIndex()

		//创建一个具有test日志的follower节点 并且任期小于leader节点
		followerStorage := NewMemoryStorage()
		followerStorage.Append(tt)
		follower := newTestRaft(2, []uint64{1, 2, 3}, 10, 1, followerStorage)
		follower.Term = term - 1

		// It is necessary to have a three-node cluster.
		// The second may have more up-to-date log than the first one, so the
		// first node needs the vote from the third node to become the leader.
		//必须有一个三节点群集。
		//第二个可能比第一个具有更多的最新日志，因此
		//第一个节点需要第三个节点的投票才能成为领导者
		// 一个leader 一个follower 一个blackhole
		// 本质上创建了一个包含三个节点的map 然后通过共用一个类似于管道的东西来发送 将每个node的msg抽象出来 可以读取消息 设计
		n := newNetwork(lead, follower, nopStepper)

		//发起选举请求成为leader
		n.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgHup})

		// The election occurs in the term after the one we loaded with
		// lead's term and committed index setted up above.
		// 选举发生在我们加载的leader的任期和上面设置的committed index之后的任期中
		n.send(pb.Message{From: 3, To: 1, MsgType: pb.MessageType_MsgRequestVoteResponse, Term: term + 1})

		n.send(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{}}})
		//fmt.Println(lead.RaftLog)
		//fmt.Println(follower.RaftLog)
		//ltoa加载所有的entry 并判断差异 没有差异才正确？
		if g := diffu(ltoa(lead.RaftLog), ltoa(follower.RaftLog)); g != "" {
			t.Errorf("#%d: log diff:\n%s", i, g)
		}
	}
}

// TestVoteRequest tests that the vote request includes information about the candidate’s log
// and are sent to all of the other nodes.
// Reference: section 5.4.1
func TestVoteRequest2AB(t *testing.T) {
	tests := []struct {
		ents  []*pb.Entry
		wterm uint64
	}{
		{[]*pb.Entry{{Term: 1, Index: 1}}, 2},
		{[]*pb.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}}, 3},
	}
	for j, tt := range tests {
		r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewMemoryStorage())
		r.Step(pb.Message{
			From: 2, To: 1, MsgType: pb.MessageType_MsgAppend, Term: tt.wterm - 1, LogTerm: 0, Index: 0, Entries: tt.ents,
		})
		r.readMessages()

		// 成为StateCandidate的过程
		for r.State != StateCandidate {
			r.tick()
		}

		msgs := r.readMessages()
		sort.Sort(messageSlice(msgs))
		if len(msgs) != 2 {
			t.Fatalf("#%d: len(msg) = %d, want %d", j, len(msgs), 2)
		}
		for i, m := range msgs {
			if m.MsgType != pb.MessageType_MsgRequestVote {
				t.Errorf("#%d: msgType = %d, want %d", i, m.MsgType, pb.MessageType_MsgRequestVote)
			}
			if m.To != uint64(i+2) {
				t.Errorf("#%d: to = %d, want %d", i, m.To, i+2)
			}
			if m.Term != tt.wterm {
				t.Errorf("#%d: term = %d, want %d", i, m.Term, tt.wterm)
			}

			windex, wlogterm := tt.ents[len(tt.ents)-1].Index, tt.ents[len(tt.ents)-1].Term
			if m.Index != windex {
				t.Errorf("#%d: index = %d, want %d", i, m.Index, windex)
			}
			if m.LogTerm != wlogterm {
				t.Errorf("#%d: logterm = %d, want %d", i, m.LogTerm, wlogterm)
			}
		}
	}
}

// TestVoter tests the voter denies its vote if its own log is more up-to-date
// than that of the candidate.
// Reference: section 5.4.1
func TestVoter2AB(t *testing.T) {
	tests := []struct {
		ents    []pb.Entry
		logterm uint64
		index   uint64

		wreject bool
	}{
		// same logterm
		{[]pb.Entry{{Term: 1, Index: 1}}, 1, 1, false},
		{[]pb.Entry{{Term: 1, Index: 1}}, 1, 2, false},
		{[]pb.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}}, 1, 1, true},
		// candidate higher logterm
		{[]pb.Entry{{Term: 1, Index: 1}}, 2, 1, false},
		{[]pb.Entry{{Term: 1, Index: 1}}, 2, 2, false},
		{[]pb.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}}, 2, 1, false},
		// voter higher logterm
		{[]pb.Entry{{Term: 2, Index: 1}}, 1, 1, true},
		{[]pb.Entry{{Term: 2, Index: 1}}, 1, 2, true},
		{[]pb.Entry{{Term: 2, Index: 1}, {Term: 1, Index: 2}}, 1, 1, true},
	}
	for i, tt := range tests {
		storage := NewMemoryStorage()
		storage.Append(tt.ents)
		r := newTestRaft(1, []uint64{1, 2}, 10, 1, storage)

		r.Step(pb.Message{From: 2, To: 1, MsgType: pb.MessageType_MsgRequestVote, Term: 3, LogTerm: tt.logterm, Index: tt.index})

		msgs := r.readMessages()
		if len(msgs) != 1 {
			t.Fatalf("#%d: len(msg) = %d, want %d", i, len(msgs), 1)
		}
		m := msgs[0]
		if m.MsgType != pb.MessageType_MsgRequestVoteResponse {
			t.Errorf("#%d: msgType = %d, want %d", i, m.MsgType, pb.MessageType_MsgRequestVoteResponse)
		}
		if m.Reject != tt.wreject {
			t.Errorf("#%d: reject = %t, want %t", i, m.Reject, tt.wreject)
		}
	}
}

// TestLeaderOnlyCommitsLogFromCurrentTerm tests that only log entries from the leader’s
// current term are committed by counting replicas.
// Reference: section 5.4.2
// 只有从leader的目前的任期被提交
func TestLeaderOnlyCommitsLogFromCurrentTerm2AB(t *testing.T) {
	ents := []pb.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}}
	tests := []struct {
		index   uint64
		wcommit uint64
	}{
		// do not commit log entries in previous terms
		{1, 0},
		{2, 0},
		// commit log in current term
		{3, 3},
	}
	for i, tt := range tests {
		storage := NewMemoryStorage()
		storage.Append(ents)
		r := newTestRaft(1, []uint64{1, 2}, 10, 1, storage)
		r.Term = 2
		// become leader at term 3
		r.becomeCandidate()
		r.becomeLeader()
		r.readMessages()
		// propose a entry to current term
		r.Step(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{}}})

		r.Step(pb.Message{From: 2, To: 1, MsgType: pb.MessageType_MsgAppendResponse, Term: r.Term, Index: tt.index})
		if r.RaftLog.committed != tt.wcommit {
			t.Errorf("#%d: commit = %d, want %d", i, r.RaftLog.committed, tt.wcommit)
		}
	}
}

type messageSlice []pb.Message

func (s messageSlice) Len() int           { return len(s) }
func (s messageSlice) Less(i, j int) bool { return fmt.Sprint(s[i]) < fmt.Sprint(s[j]) }
func (s messageSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// 提交日志
func commitNoopEntry(r *Raft, s *MemoryStorage) {
	if r.State != StateLeader {
		panic("it should only be used when it is the leader")
	}

	for id := range r.Prs {
		if id == r.id {
			continue
		}

		r.sendAppend(id)
	}
	// simulate the response of MessageType_MsgAppend
	// 模拟MessageType_MsgAppend的响应
	msgs := r.readMessages()
	for _, m := range msgs {
		//
		if m.MsgType != pb.MessageType_MsgAppend || len(m.Entries) != 1 || m.Entries[0].Data != nil {
			panic("not a message to append noop entry")
		}
		r.Step(acceptAndReply(m))
	}
	// ignore further messages to refresh followers' commit index
	// 通过忽略以后的消息来刷新follower的提交的index
	r.readMessages()
	//将不稳定的日志添加到storage中
	s.Append(r.RaftLog.unstableEntries())
	//更新applied和lastindex
	r.RaftLog.applied = r.RaftLog.committed
	r.RaftLog.stabled = r.RaftLog.LastIndex()
}

// 接受消息并且响应
func acceptAndReply(m pb.Message) pb.Message {
	if m.MsgType != pb.MessageType_MsgAppend {
		panic("type should be MessageType_MsgAppend")
	}
	// Note: reply message don't contain LogTerm
	return pb.Message{
		From:    m.To,
		To:      m.From,
		Term:    m.Term,
		MsgType: pb.MessageType_MsgAppendResponse,
		Index:   m.Index + uint64(len(m.Entries)),
	}
}

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
	"log"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// ErrStepLocalMsg is returned when try to step a local raft message
var ErrStepLocalMsg = errors.New("raft: cannot step raft local message")

// ErrStepPeerNotFound is returned when try to step a response message
// but there is no peer found in raft.Prs for that node.
var ErrStepPeerNotFound = errors.New("raft: cannot step as peer not found")

// SoftState provides state that is volatile and does not need to be persisted to the WAL.
type SoftState struct {
	Lead      uint64
	RaftState StateType
}

// Ready encapsulates the entries and messages that are ready to read,
// be saved to stable storage, committed or sent to other peers.
// All fields in Ready are read-only.
// Ready封装了需要发送的entries和messages，被保存到稳定的存储，提交或者发送给peers，所有的filed在Ready中是仅读的
type Ready struct {
	// The current volatile state of a Node.
	// SoftState will be nil if there is no update.
	// It is not required to consume or store SoftState.
	// 节点的当前易失状态，SoftState将会是nil如果没有更新，它不需要使用或存储
	*SoftState

	// The current state of a Node to be saved to stable storage BEFORE
	// Messages are sent.
	// HardState will be equal to empty state if there is no update.
	// 要保存到稳定存储之前的节点的当前状态
	// 发送消息。
	// 如果没有更新，hardState将等于空状态
	pb.HardState

	// Entries specifies entries to be saved to stable storage BEFORE
	// Messages are sent.
	Entries []pb.Entry

	// Snapshot specifies the snapshot to be saved to stable storage.
	Snapshot pb.Snapshot

	// CommittedEntries specifies entries to be committed to a
	// store/state-machine. These have previously been committed to stable
	// store.
	CommittedEntries []pb.Entry

	// Messages specifies outbound messages to be sent AFTER Entries are
	// committed to stable storage.
	// If it contains a MessageType_MsgSnapshot message, the application MUST report back to raft
	// when the snapshot has been received or has failed by calling ReportSnapshot.
	Messages []pb.Message
}

// RawNode is a wrapper of Raft.
// RawNode是Raft的封装
type RawNode struct {
	Raft *Raft
	// Your Data Here (2A).
	preSoftState *SoftState
	preHardState *pb.HardState
}

// NewRawNode returns a new RawNode given configuration and a list of raft peers.
// NewRawNode返回一个新的RawNode给定配置和raft peers列表
func NewRawNode(config *Config) (*RawNode, error) {
	// Your Code Here (2A).
	raft := newRaft(config)
	softstate := raft.SoftState()
	hardState := raft.HardState()
	return &RawNode{Raft: raft, preHardState: hardState, preSoftState: softstate}, nil
}

// Tick advances the internal logical clock by a single tick.
func (rn *RawNode) Tick() {
	rn.Raft.tick()
}

// Campaign causes this RawNode to transition to candidate state.
func (rn *RawNode) Campaign() error {
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgHup,
	})
}

// Propose proposes data be appended to the raft log.
func (rn *RawNode) Propose(data []byte) error {
	ent := pb.Entry{Data: data}
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		From:    rn.Raft.id,
		Entries: []*pb.Entry{&ent}})
}

// ProposeConfChange proposes a config change.
func (rn *RawNode) ProposeConfChange(cc pb.ConfChange) error {
	data, err := cc.Marshal()
	if err != nil {
		return err
	}
	ent := pb.Entry{EntryType: pb.EntryType_EntryConfChange, Data: data}
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{&ent},
	})
}

// ApplyConfChange applies a config change to the local node.
func (rn *RawNode) ApplyConfChange(cc pb.ConfChange) *pb.ConfState {
	if cc.NodeId == None {
		return &pb.ConfState{Nodes: nodes(rn.Raft)}
	}
	switch cc.ChangeType {
	case pb.ConfChangeType_AddNode:
		rn.Raft.addNode(cc.NodeId)
	case pb.ConfChangeType_RemoveNode:
		rn.Raft.removeNode(cc.NodeId)
	default:
		panic("unexpected conf type")
	}
	return &pb.ConfState{Nodes: nodes(rn.Raft)}
}

// Step advances the state machine using the given message.
func (rn *RawNode) Step(m pb.Message) error {
	// ignore unexpected local messages receiving over network
	if IsLocalMsg(m.MsgType) {
		return ErrStepLocalMsg
	}
	if pr := rn.Raft.Prs[m.From]; pr != nil || !IsResponseMsg(m.MsgType) {
		return rn.Raft.Step(m)
	}
	return ErrStepPeerNotFound
}

// Ready returns the current point-in-time state of this RawNode.
func (rn *RawNode) Ready() Ready {
	// Your Code Here (2A).

	entries := rn.Raft.RaftLog.unstableEntries()
	// 保存committed但是还没有applied的数据数组
	committedEntries := rn.Raft.RaftLog.nextEnts()

	ready := Ready{
		Entries:          entries,
		CommittedEntries: committedEntries,
		Messages:         rn.Raft.msgs,
	}

	softState := &SoftState{
		Lead:      rn.Raft.Lead,
		RaftState: rn.Raft.State,
	}
	if softState.Lead != rn.preSoftState.Lead || softState.RaftState != rn.preSoftState.RaftState {
		ready.SoftState = softState
	}

	hardState, _, _ := rn.Raft.RaftLog.storage.InitialState()
	if hardState.Vote != rn.preHardState.Vote || hardState.Term != rn.preHardState.Term || hardState.Commit != rn.preHardState.Commit {
		ready.HardState = hardState
	}

	return ready
}

// HasReady called when RawNode user need to check if any Ready pending.
func (rn *RawNode) HasReady() bool {
	// Your Code Here (2A).
	r := rn.Raft
	hardState, _, err := r.RaftLog.storage.InitialState()
	if err != nil {
		panic(err)
	}
	if hardState.Vote == rn.preHardState.Vote && hardState.Term == rn.preHardState.Term && hardState.Commit == rn.preHardState.Commit {
		return false
	}

	softState := &SoftState{
		Lead:      rn.Raft.Lead,
		RaftState: rn.Raft.State,
	}
	if softState.Lead == rn.preSoftState.Lead && softState.RaftState == rn.preSoftState.RaftState {
		return false
	}

	if len(r.msgs) > 0 || len(r.RaftLog.unstableEntries()) > 0 || r.RaftLog.hasNextEnts() {
		return false
	}

	return true
}

// Advance notifies the RawNode that the application has applied and saved progress in the
// last Ready results.
// Advance 通知 RawNode 应用程序已申请并保存了进度
// 最后的 Ready 结果。
func (rn *RawNode) Advance(rd Ready) {
	// Your Code Here (2A).
	//applied rd中的entries 并且更改对应的状态
	lastIndex, err := rn.Raft.RaftLog.storage.LastIndex()
	if err != nil {
		log.Printf("rn.Raft.RaftLog.storage.LastIndex() falied\n")
	}

	rn.Raft.RaftLog.applied = lastIndex
	rn.Raft.RaftLog.stabled = lastIndex

	//删除已经添加到storage中的日志
	lastCommitedEntries := rd.CommittedEntries[len(rd.CommittedEntries)-1]
	var index int
	for i, entry := range rn.Raft.RaftLog.entries {
		if entry.Index == lastCommitedEntries.Index && entry.Term == lastCommitedEntries.Term {
			index = i
			break
		}
	}

	rn.Raft.RaftLog.entries = rn.Raft.RaftLog.entries[index+1:]
}

// GetProgress return the Progress of this node and its peers, if this
// node is leader.
func (rn *RawNode) GetProgress() map[uint64]Progress {
	prs := make(map[uint64]Progress)
	if rn.Raft.State == StateLeader {
		for id, p := range rn.Raft.Prs {
			prs[id] = *p
		}
	}
	return prs
}

// TransferLeader tries to transfer leadership to the given transferee.
func (rn *RawNode) TransferLeader(transferee uint64) {
	_ = rn.Raft.Step(pb.Message{MsgType: pb.MessageType_MsgTransferLeader, From: transferee})
}

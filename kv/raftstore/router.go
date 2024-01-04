package raftstore

import (
	"github.com/pingcap-incubator/tinykv/log"
	"sync"
	"sync/atomic"

	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"

	"github.com/pingcap/errors"
)

// peerState contains the peer states that needs to run raft command and apply command.
// peerState 包含需要运行 raft command 和 apply 命令的对等状态。
type peerState struct {
	closed uint32
	peer   *peer //实际管理rawnode的结构
}

// router routes a message to a peer.
type router struct {
	peers       sync.Map         // regionID -> peerState
	peerSender  chan message.Msg //存储msg 之后将这个赋给worker进行处理
	storeSender chan<- message.Msg
}

func newRouter(storeSender chan<- message.Msg) *router {
	pm := &router{
		peerSender:  make(chan message.Msg, 40960),
		storeSender: storeSender,
	}
	return pm
}

// 根据regionID获取peerState 获取peer
func (pr *router) get(regionID uint64) *peerState {
	v, ok := pr.peers.Load(regionID)
	if ok {
		return v.(*peerState)
	}
	return nil
}

// 将peer封装成peerState存储到sync.Map中 key为peer.regionId value为peerState
func (pr *router) register(peer *peer) {
	id := peer.regionId
	newPeer := &peerState{
		peer: peer,
	}
	pr.peers.Store(id, newPeer)
}

// 根据regionID删除sync.Map中的peerState
func (pr *router) close(regionID uint64) {
	v, ok := pr.peers.Load(regionID)
	if ok {
		ps := v.(*peerState)
		atomic.StoreUint32(&ps.closed, 1)
		pr.peers.Delete(regionID)
	}
}

func (pr *router) send(regionID uint64, msg message.Msg) error {
	msg.RegionID = regionID
	p := pr.get(regionID)
	//判断peer是否存在并且未被关闭
	if p == nil || atomic.LoadUint32(&p.closed) == 1 {
		log.Infof("errPeerNotFound")
		return errPeerNotFound
	}
	//将消息发送到chan中
	//log.Infof("aui")
	pr.peerSender <- msg

	return nil
}

func (pr *router) sendStore(msg message.Msg) {
	pr.storeSender <- msg
}

var errPeerNotFound = errors.New("peer not found")

type RaftstoreRouter struct {
	router *router
}

func NewRaftstoreRouter(router *router) *RaftstoreRouter {
	return &RaftstoreRouter{router: router}
}

func (r *RaftstoreRouter) Send(regionID uint64, msg message.Msg) error {
	return r.router.send(regionID, msg)
}

func (r *RaftstoreRouter) SendRaftMessage(msg *raft_serverpb.RaftMessage) error {
	regionID := msg.RegionId
	if r.router.send(regionID, message.NewPeerMsg(message.MsgTypeRaftMessage, regionID, msg)) != nil {
		r.router.sendStore(message.NewPeerMsg(message.MsgTypeStoreRaftMessage, regionID, msg))
	}
	return nil

}

func (r *RaftstoreRouter) SendRaftCommand(req *raft_cmdpb.RaftCmdRequest, cb *message.Callback) error {
	cmd := &message.MsgRaftCmd{
		Request:  req,
		Callback: cb,
	}
	regionID := req.Header.RegionId
	return r.router.send(regionID, message.NewPeerMsg(message.MsgTypeRaftCmd, regionID, cmd))
}

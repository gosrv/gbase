package cluster

import (
	"fmt"
	"github.com/gosrv/gbase/gproto"
	"github.com/gosrv/gbase/gutil"
	"github.com/gosrv/gbase/tcpnet"
	"time"
)

var KeyHost = "clustermq.host"

type ClusterMQ struct {
	nodeMgr        INodeMgr
	nodeNetChannel map[string]gproto.INetChannel
	netSystem      *tcpnet.NetSystem
	host           string
}

func (this *ClusterMQ) Push(nodeUuid string, msg interface{}) error {
	net, ok := this.nodeNetChannel[nodeUuid]
	if !ok {
		return fmt.Errorf("node mq push failed, node %v is not active", nodeUuid)
	}
	net.Send(msg)
	return nil
}

func NewClusterMQ(nodeMgr INodeMgr, netSystem *tcpnet.NetSystem, host string) *ClusterMQ {
	return &ClusterMQ{nodeMgr: nodeMgr, netSystem: netSystem, host: host}
}

func (this *ClusterMQ) GoStart() error {
	addr, err := this.netSystem.GoListen("tcp", this.host)
	if err != nil {
		return err
	}
	this.host = addr.String()
	this.nodeMgr.GetMyNodeInfo().Meta[KeyHost] = this.host

	gutil.RecoverGo(func() {
		for {
			// 建立连接，两个节点之间会建立两条连接，一个用于发送，一个用于接收
			nodes := this.nodeMgr.GetAllNodesInfo()
			for _, node := range nodes {
				_, ok := this.nodeNetChannel[node.NodeUuid]
				if ok {
					continue
				}
				host, ok := node.Meta[KeyHost]
				if !ok {
					continue
				}
				netChannel, err := this.netSystem.GoConnect("tcp", host)
				if err == nil {
					continue
				}
				this.nodeNetChannel[node.NodeUuid] = netChannel
			}

			for node, net := range this.nodeNetChannel {
				if !net.IsActive() {
					_ = net.Close()
					delete(this.nodeNetChannel, node)
				}
			}
			time.Sleep(time.Second * 2)
		}
	})
	return nil
}

package cluster

import (
	"fmt"
	"github.com/gosrv/gbase/controller"
	"github.com/gosrv/gbase/gl"
	"github.com/gosrv/gbase/gnet"
	"github.com/gosrv/gbase/gproto"
	"github.com/gosrv/gbase/gutil"
	"github.com/gosrv/gbase/route"
	"github.com/gosrv/gbase/tcpnet"
	"github.com/gosrv/goioc"
	"github.com/gosrv/goioc/util"
	"github.com/pkg/errors"
	"net"
	"reflect"
	"time"
)

var KeyHost = "clustermq.host"

type ClusterMQ struct {
	gioc.IBeanCondition
	gioc.IConfigBase
	nodeMgr               INodeMgr
	nodeNetChannel        map[string]gproto.INetChannel
	netSystem             *tcpnet.NetSystem
	host                  string `cfg.d:"net.host"`
	selfSessionCtx        gnet.ISessionCtx
	controlPointCollector controller.IControlPointGroupMgr `bean`
	eventRoute            gproto.IRoute
	delegateDataRoute     gproto.IRouteDelegate
	encoder               gproto.IEncoder
	decoder               gproto.IDecoder
	ctlGroup              string
}

func (this *ClusterMQ) BeanStart() {
	this.netSystem = tcpnet.NewNetSysten(this.createNetConfig())
	err := this.GoStart()
	util.VerifyNoError(err)
}

func (this *ClusterMQ) BeanStop() {

}

func (this *ClusterMQ) Push(nodeUuid string, msg interface{}) error {
	if nodeUuid == this.nodeMgr.GetMyNodeInfo().NodeUuid {
		smsg := msg
		for i := 0; i < 4; i++ {
			smsg = this.netSystem.Config.DataRoute.Trigger(this.selfSessionCtx, reflect.TypeOf(smsg), smsg)
			if smsg == nil {
				break
			}
		}
		if smsg != nil {
			return errors.New("loop process")
		}
		return nil
	}
	net, ok := this.nodeNetChannel[nodeUuid]
	if !ok {
		return fmt.Errorf("node mq push failed, node %v is not active", nodeUuid)
	}
	net.Send(msg)
	return nil
}

func NewClusterMQ(nodeMgr INodeMgr, cfgBase, ctlGroup string, encoder gproto.IEncoder, decoder gproto.IDecoder,
	eventRoute gproto.IRouteDelegate, dataRoute gproto.IRouteDelegate) *ClusterMQ {
	if dataRoute == nil {
		dataRoute = route.NewRouteDelegate(false)
	}
	if eventRoute == nil {
		eventRoute = route.NewRouteDelegate(true)
	}
	eventRoute.SetDelegate(route.NewRouteMap(false, false))
	return &ClusterMQ{
		IBeanCondition:    gioc.NewConditionOnValue(cfgBase, true),
		IConfigBase:       gioc.NewConfigBase(cfgBase),
		nodeMgr:           nodeMgr,
		selfSessionCtx:    gnet.NewSessionCtx(),
		encoder:           encoder,
		decoder:           decoder,
		eventRoute:        eventRoute,
		delegateDataRoute: dataRoute,
		ctlGroup:          ctlGroup,
		nodeNetChannel:    map[string]gproto.INetChannel{},
	}
}
func (this *ClusterMQ) getLANAddress () string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "127.0.0.1:0"
	}

	for _, address := range addrs {
		// 检查ip地址判断是否回环地址
		if ipnet, ok := address.(*net.IPNet); ok && ipnet.IP.IsGlobalUnicast() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String() + ":0"
			}
		}
	}
	return "127.0.0.1:0"
}

func (this *ClusterMQ) GoStart() error {
	if len(this.host) == 0 {
		this.host = this.getLANAddress()
		gl.Warn("cluster use lan address %v", this.host)
	}
	addr, err := this.netSystem.GoListen("tcp", this.host)
	if err != nil {
		return err
	}
	this.host = addr.String()
	gl.Info("cluster listen on %v", this.host)

	this.nodeMgr.GetMyNodeInfo().Meta[KeyHost] = this.host

	gutil.RecoverGo(func() {
		for {
			// 建立连接，两个节点之间会建立两条连接，一个用于发送，一个用于接收
			nodes := this.nodeMgr.GetAllNodesInfo()
			for _, node := range nodes {
				if node.NodeUuid == this.nodeMgr.GetMyNodeInfo().NodeUuid {
					continue
				}
				_, ok := this.nodeNetChannel[node.NodeUuid]
				if ok {
					continue
				}
				host, ok := node.Meta[KeyHost]
				if !ok {
					continue
				}
				netChannel, err := this.netSystem.GoConnect("tcp", host)
				if err != nil {
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

func (this *ClusterMQ) createNetConfig() *gnet.NetConfig {
	this.delegateDataRoute.SetDelegate(controller.NewControlPointRoute(
		this.controlPointCollector.GetControlPointGroup(this.ctlGroup)))
	return &gnet.NetConfig{
		// 网络消息编码器，4字节长度 + 2字节id + protobuf
		Encoder: this.encoder,
		// 网络消息解码器，4字节长度 + 2字节id + protobuf
		Decoder: this.decoder,
		// 事件路由器
		EventRoute: this.eventRoute,
		// 数据路由器，转发给控制器
		DataRoute:        this.delegateDataRoute,
		ReadBufSize:      16384,
		WriteChannelSize: 1024,
		HeartTickMs:      10000,
	}
}

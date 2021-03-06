package tcpnet

import (
	"github.com/gosrv/gbase/controller"
	"github.com/gosrv/gbase/gnet"
	"github.com/gosrv/gbase/gproto"
	"github.com/gosrv/gbase/route"
	"github.com/gosrv/glog"
	"github.com/gosrv/goioc"
)

/**
自动网络配置：
如果配置文件中存在配置tcpnetConfigValue，则此网络模块启动
*/
type TcpNetServer struct {
	// 启动条件
	gioc.IBeanCondition
	gioc.IConfigBase
	// 从配置文件中注入host
	host string            `cfg.d:"net.host"`
	log  glog.IFieldLogger `log:"app"`
	// 注入控制器
	controlPointCollector controller.IControlPointGroupMgr `bean`
	eventRoute            gproto.IRoute
	delegateDataRoute     gproto.IRouteDelegate
	encoder               gproto.IEncoder
	decoder               gproto.IDecoder
	ctlGroup              string
	netsystem             *NetSystem
}

func (this *TcpNetServer) GetEventRoute() gproto.IRoute {
	return this.eventRoute
}

// 启动网络
func (this *TcpNetServer) BeanStart() {
	this.netsystem = NewNetSysten(this.createNetConfig())
	if len(this.host) > 0 {
		this.netsystem.GoListen("tcp", this.host)
		this.log.Info("net listen on %v", this.host)
	}
}

func (this *TcpNetServer) BeanStop() {

}

func (this *TcpNetServer) NetConnect(address string) {
	this.netsystem.GoConnect("tcp", address)
}

func NewTcpNetServer(cfgBase, ctlGroup string, encoder gproto.IEncoder, decoder gproto.IDecoder,
	eventRoute gproto.IRouteDelegate, dataRoute gproto.IRouteDelegate) *TcpNetServer {
	if dataRoute == nil {
		dataRoute = route.NewRouteDelegate(false)
	}
	if eventRoute == nil {
		eventRoute = route.NewRouteDelegate(true)
	}
	eventRoute.SetDelegate(route.NewRouteMap(false, false))

	return &TcpNetServer{
		// 启用条件：配置文件中存在配置tcpnetConfigValue
		IBeanCondition:    gioc.NewConditionOnValue(cfgBase, true),
		IConfigBase:       gioc.NewConfigBase(cfgBase),
		encoder:           encoder,
		decoder:           decoder,
		eventRoute:        eventRoute,
		delegateDataRoute: dataRoute,
		ctlGroup:          ctlGroup,
	}
}

func (this *TcpNetServer) createNetConfig() *gnet.NetConfig {
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

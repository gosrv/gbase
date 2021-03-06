package wsnet

import (
	"errors"
	"github.com/gorilla/websocket"
	"github.com/gosrv/gbase/gl"
	"github.com/gosrv/gbase/gnet"
	"github.com/gosrv/gbase/gproto"
	"github.com/gosrv/gbase/gutil"
	"github.com/gosrv/goioc/util"
	"net/http"
	"reflect"
)

type WsNetSystem struct {
	host    string
	entry   string
	msgType int
	config  *gnet.NetConfig
	handler *http.ServeMux
}

const (
	MsgTypeString = "string"
	MsgTypeBinary = "bin"
)

func NewWsNetSystem(host string, entry string, msgType string, config *gnet.NetConfig, handler *http.ServeMux) *WsNetSystem {
	mt := 0
	switch msgType {
	case MsgTypeString:
		mt = websocket.TextMessage
	case MsgTypeBinary:
		mt = websocket.BinaryMessage
	default:
		gl.Panic("unknown websocket msg type")
		return nil
	}
	return &WsNetSystem{host: host, entry: entry, msgType: mt, config: config, handler: handler}
}

var upgrader = websocket.Upgrader{
	ReadBufferSize:  32 * 1024,
	WriteBufferSize: 64 * 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

// 将客户端消息，转发到上游服务器
func (this *WsNetSystem) goReadProcess(netChannel *wsNetChannel) error {
	// 通知上游打开连接
	for netChannel.IsActive() {
		messageType, msg, err := netChannel.conn.ReadMessage()
		util.Verify(messageType == this.msgType)
		if err != nil {
			return errors.New("net channel read closed")
		}
		netMsg := this.config.Decoder.Decode(msg)
		netChannel.msgUnprocessedChannel <- netMsg
	}
	return nil
}

// 将上游服务器的消息发送到客户端
func (this *WsNetSystem) goWriteProcess(netChannel *wsNetChannel) error {
	eventRoute := this.config.EventRoute
	dataRoute := this.config.DataRoute

	eventRoute.Trigger(netChannel.ctx, gnet.NetEventConnect, nil)
	defer func() {
		_ = netChannel.Close()
		for {
			// 把剩下的活干完就可以退出了
			if msg, ok := <-netChannel.msgUnprocessedChannel; ok {
				netChannel.ctx.Clear(gnet.ScopeRequest)
				netChannel.ctx.SetAttribute(gnet.ScopeRequest, reflect.TypeOf(msg), msg)
				_ = dataRoute.Trigger(netChannel.ctx, reflect.TypeOf(msg), msg)
			} else {
				break
			}
		}
		eventRoute.Trigger(netChannel.ctx, gnet.NetEventDisconnect, nil)
	}()

	for netChannel.IsActive() {
		select {
		case _, _ = <-netChannel.closeChannel:
			return nil
		case msg, ok := <-netChannel.msgProcessedChannel:
			{
				if !ok {
					// channel已被关闭
					return errors.New("gnet msg processed channel closed")
				}
				err := netChannel.conn.WriteMessage(this.msgType, msg.([]byte))
				if err != nil {
					return err
				}
			}
		case msg, ok := <-netChannel.msgUnprocessedChannel:
			{
				if !ok {
					// channel已被关闭
					return errors.New("gnet msg unprocessed channel closed")
				}
				netChannel.ctx.Clear(gnet.ScopeRequest)
				netChannel.ctx.SetAttribute(gnet.ScopeRequest, reflect.TypeOf(msg), msg)
				response := dataRoute.Trigger(netChannel.ctx, reflect.TypeOf(msg), msg)
				if response == nil || reflect.ValueOf(response).IsNil() {
					continue
				}
				netChannel.msgProcessedChannel <- response
			}
		case _, ok := <-netChannel.heartTicker.C:
			{
				if !ok {
					return errors.New("ticker stoped")
				}
				eventRoute.Trigger(netChannel.ctx, gnet.NetEventTick, nil)
			}
		}
	}
	return nil
}

func (this *WsNetSystem) wsStart(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		gl.Warn("ws upgrade error %v", err)
		return
	}
	netChannel := NewWsNetChannel(conn, this.config.WriteChannelSize,
		this.config.WriteChannelSize, this.config.HeartTickMs)
	netChannel.ctx.SetAttribute(gnet.ScopeSession, gproto.INetChannelType, netChannel)
	netChannel.ctx.SetAttribute(gnet.ScopeSession, gnet.ISessionCtxType, netChannel.ctx)

	gutil.RecoverGo(func() {
		defer netChannel.Close()
		this.goWriteProcess(netChannel)
	})
	gutil.RecoverGo(func() {
		defer netChannel.Close()
		this.goWriteProcess(netChannel)
	})
}

func (this *WsNetSystem) GoStart() *WsNetSystem {
	gutil.RecoverGo(func() {
		this.handler.HandleFunc(this.entry, func(writer http.ResponseWriter, request *http.Request) {
			this.wsStart(writer, request)
		})
		err := http.ListenAndServe(this.host, this.handler)
		if err != nil {
			gl.Panic("start websocket error %v", err)
		}
	})
	return nil
}

package cluster

var NodeMQName = "_node.mq"

type INodeMQ interface {
	// uuid是节点的uuid, msg是一个消息
	Push(nodeUuid string, msg interface{}) error
}

type NodeMQ struct {
	nodeMq INodeMQ
}

func NewNodeMQ(nodeMq INodeMQ) *NodeMQ {
	return &NodeMQ{nodeMq: nodeMq}
}

func (this *NodeMQ) Push(nodeUuid string, msg interface{}) error {
	return this.nodeMq.Push(nodeUuid, msg)
}

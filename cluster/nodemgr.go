package cluster

type NodeInfo struct {
	NodeUuid string
	Meta     map[string]string
	Tick     int64
	Stable   bool
}

type INodeMgr interface {
	// 节点是否活跃
	IsNodeActive(nodeUuid string) bool
	// 获取节点信息
	GetNodeInfo(nodeUuid string) *NodeInfo
	// 获取所有节点信息
	GetAllNodesInfo() []*NodeInfo
	// 获取本节点信息
	GetMyNodeInfo() *NodeInfo
}

type NodeMgr struct {
	nodeMgr INodeMgr
}

func NewNodeMgr(nodeMgr INodeMgr) *NodeMgr {
	return &NodeMgr{nodeMgr: nodeMgr}
}

func (this *NodeMgr) GetMyNodeInfo() *NodeInfo {
	return this.nodeMgr.GetMyNodeInfo()
}

func (this *NodeMgr) IsNodeActive(nodeUuid string) bool {
	return this.nodeMgr.IsNodeActive(nodeUuid)
}

func (this *NodeMgr) GetNodeInfo(nodeUuid string) *NodeInfo {
	return this.nodeMgr.GetNodeInfo(nodeUuid)
}

func (this *NodeMgr) GetAllNodesInfo() []*NodeInfo {
	return this.nodeMgr.GetAllNodesInfo()
}

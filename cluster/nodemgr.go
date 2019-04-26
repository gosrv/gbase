package cluster

type NodeInfo struct {
	NodeUuid string
	Meta     map[string]string
	Tick     int64
}

type INodeMgr interface {
	IsNodeActive(nodeUuid string) bool
	GetNodeInfo(nodeUuid string) *NodeInfo
	GetAllNodesInfo() []*NodeInfo
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

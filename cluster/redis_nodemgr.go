package cluster

import (
	"encoding/json"
	"github.com/gosrv/gbase/gdb/gredis"
	"github.com/gosrv/gbase/gl"
	"github.com/gosrv/gbase/gutil"
	"time"
)

var clusterNodeKey = "_cluster._nodes"

type ActiveNodeInfo struct {
	nodeInfo   *NodeInfo
	activeTime int64
}

type RedisNodeMgr struct {
	nodeUuid        string `bean:"app.id"`
	myNodeInfo      *NodeInfo
	nodeOpt         *gredis.HashOperation `redis:""`
	activeNodesInfo map[string]*ActiveNodeInfo
	allNodesInfo    []*NodeInfo
}

func (this *RedisNodeMgr) BeanStart() {
	this.GOStart()
}

func (this *RedisNodeMgr) BeanStop() {

}

func NewRedisNodeMgr() *RedisNodeMgr {
	return &RedisNodeMgr{}
}

func (this *RedisNodeMgr) BeanInit() {
	this.myNodeInfo = &NodeInfo{NodeUuid: this.nodeUuid, Tick: time.Now().Unix(), Meta: map[string]string{}}
	this.registerServer()
}

func (this *RedisNodeMgr) BeanUninit() {

}

func (this *RedisNodeMgr) GetNodeInfo(nodeUuid string) *NodeInfo {
	nodeInfo, ok := this.activeNodesInfo[nodeUuid]
	if !ok {
		return nil
	}
	return nodeInfo.nodeInfo
}

func (this *RedisNodeMgr) GetAllNodesInfo() []*NodeInfo {
	return this.allNodesInfo
}

func (this *RedisNodeMgr) IsNodeActive(nodeUuid string) bool {
	_, ok := this.activeNodesInfo[nodeUuid]
	return ok
}

func (this *RedisNodeMgr) GetMyNodeInfo() *NodeInfo {
	return this.myNodeInfo
}

func (this *RedisNodeMgr) GOStart() {
	gutil.RecoverGo(func() {
		for {
			this.registerServer()
			time.Sleep(time.Second * 2)
		}
	})
}

func (this *RedisNodeMgr) registerServer() {
	now := time.Now().Unix()
	this.myNodeInfo.Tick = now
	data, err := json.Marshal(this.myNodeInfo)
	if err != nil {
		gl.Debug("json marshal error %v", err)
	}
	_, err = this.nodeOpt.HSet(clusterNodeKey, this.myNodeInfo.NodeUuid, string(data))
	if err != nil {
		gl.Debug("redis hash opt error %v", err)
	}
	allData, err := this.nodeOpt.HGetAll(clusterNodeKey)
	if err != nil {
		gl.Debug("redis hash opt error %v", err)
	}
	nodesInfo := make(map[string]*ActiveNodeInfo)
	for uuid, ndata := range allData {
		activeNodeInfo := &ActiveNodeInfo{}
		err := json.Unmarshal([]byte(ndata), &activeNodeInfo.nodeInfo)
		if err != nil {
			gl.Debug("json unmarshal error %v", err)
			continue
		}
		activeNodeInfo.nodeInfo.Stable = false
		oldActiveNodeInfo, ok := this.activeNodesInfo[uuid]
		if !ok {
			// 发现新节点
			activeNodeInfo.activeTime = now
			nodesInfo[uuid] = activeNodeInfo
		} else {
			// 老节点
			if oldActiveNodeInfo.nodeInfo.Tick != activeNodeInfo.nodeInfo.Tick {
				// 老节点活跃
				activeNodeInfo.nodeInfo.Stable = true
				activeNodeInfo.activeTime = now
				nodesInfo[uuid] = activeNodeInfo
			} else if oldActiveNodeInfo.activeTime+20 < now {
				// 老节点失效
				this.nodeOpt.HDel(clusterNodeKey, uuid)
			} else {
				oldActiveNodeInfo.nodeInfo.Stable = false
				nodesInfo[uuid] = oldActiveNodeInfo
			}
		}
	}
	allNodesInfo := make([]*NodeInfo, 0, len(nodesInfo))
	for _, ni := range nodesInfo {
		allNodesInfo = append(allNodesInfo, ni.nodeInfo)
	}
	this.activeNodesInfo = nodesInfo
	this.allNodesInfo = allNodesInfo
}

package cell

import (
	"encoding/json"
	"github.com/gosrv/gbase/cluster"
	"github.com/gosrv/gbase/gdb/gredis"
	"github.com/gosrv/gbase/gl"
	"github.com/gosrv/gbase/gutil"
	"github.com/gosrv/goioc"
	"github.com/gosrv/goioc/util"
	"math"
	"strconv"
	"sync/atomic"
	"time"
)

/**
 * 迁移步骤，A迁移数据，B接受迁移
 * 1. 上迁移锁migrate=uuidA，上锁最长5分钟
 * 2. 开始进行数据存储工作
 * 3. 完成迁移,A设置服务槽数据serviceNodeName[slot]=uuidB;versin+=1
 */

var MetaSlotSkeleton = "slot.load"

type NodeLoad struct {
	CurPower int32
	MaxPower int32
}

type ResClass struct {
	Reses []ResDes
}

type ResDes struct {
	id    int64
	power int
}

func (this *NodeLoad) FreePower() int {
	return int(this.MaxPower - this.CurPower)
}

const (
	MigrateTimeoutSec  = 60
	BalanceCheckGapSec = 60
)

type SlotGroupSkeleton struct {
	gioc.IConfigBase
	*SlotGroupStub
	load    *NodeLoad
	cfgBase string
	// 最大可管理槽
	maxManageSlot int `cfg.d:"maxmgr"`
	// 节点唯一id
	nodeUuid string `bean:"app.id"`
	// 槽数量
	slotSize int `cfg.d:"size"`

	nodeMgr        *cluster.NodeMgr            `bean:""`
	optSlotVersion *gredis.BoundValueOperation `redis:"slot:version"`
	optSlotSize    *gredis.BoundValueOperation `redis:"slot:size"`
	optSlots       *gredis.BoundHashOperation  `redis:"slot"`
	optMigrateLock *gredis.BoundValueOperation `redis:"slot:lock"`
	// 快速处理时间间隔
	fastProcessGapSec int `cfg.d:"fastpgap"`
	// 慢速处理时间间隔
	slowProcessGapSec int `cfg.d:"slowpgap"`

	migrateSlot         int
	preBalanceCheckTime int64

	// 检查到正在迁移的节点
	migrateCheckStartTime int64
	migrateCheckUuid      string
	delayIncVersion       int64
}

func NewSlotGroupSkeleton(cfgBase string) *SlotGroupSkeleton {
	return &SlotGroupSkeleton{IConfigBase: gioc.NewConfigBase(cfgBase), cfgBase: cfgBase}
}

func (this *SlotGroupSkeleton) BeanStart() {
	this.load = &NodeLoad{}
	this.SlotGroupStub = NewSlotGroupStub(this.cfgBase, this.optSlotSize, this.optSlotVersion,
		this.optSlots, this.fastProcessGapSec, this.slowProcessGapSec)
	this.GoStart()
}

func (this *SlotGroupSkeleton) BeanStop() {

}

func (this *SlotGroupSkeleton) GoStart() {
	// 初始大小
	this.initSize()
	// 初始版本
	this.optSlotVersion.SetNX("0")
	// 读取槽
	this.SlotGroupStub.slowProcess()
	// 瓜分槽
	this.RaceSlot()

	gutil.RecoverGo(func() {
		count := 0
		for {
			count++
			err := this.fastProcess()
			if err != nil {
				gl.Warn("slot fast process error %v", err)
			}
			if count%(this.slowProcessGapSec/this.fastProcessGapSec) == 0 {
				err = this.slowProcess()
				if err != nil {
					gl.Warn("slot slow process error %v", err)
				}
			}
			time.Sleep(time.Duration(this.fastProcessGapSec) * time.Second)
		}
	})
}

func (this *SlotGroupSkeleton) initSize() {
	_, _ = this.optSlotSize.SetNX(strconv.Itoa(this.slotSize))
	strSize, err := this.optSlotSize.Get()
	util.Verify(err == nil)
	util.Verify(len(strSize) > 0)
	size, err := strconv.Atoi(strSize)
	util.VerifyNoError(err)
	util.Verify(size == this.slotSize)
	this.slots = make([]string, size, size)
}

func (this *SlotGroupSkeleton) fastProcess() error {
	// 更新自己的meta数据
	jdata, err := json.Marshal(this.load)
	if err != nil {
		return err
	}
	this.nodeMgr.GetMyNodeInfo().Meta[MetaSlotSkeleton] = string(jdata)

	err = this.SlotGroupStub.fastProcess()
	if err != nil {
		return err
	}

	if this.delayIncVersion > 0 {
		_, err := this.optSlotVersion.IncrBy(this.delayIncVersion)
		if err == nil {
			this.delayIncVersion = 0
		}
	}

	return nil
}

func (this *SlotGroupSkeleton) slowProcess() error {
	err := this.SlotGroupStub.slowProcess()
	if err != nil {
		return err
	}
	if len(this.migrateCheckUuid) > 0 {
		// 有节点正在迁移
		now := time.Now().Unix()
		if this.migrateCheckStartTime+MigrateTimeoutSec < now {
			_, _ = this.optMigrateLock.Cas(this.migrateCheckUuid, "")
			this.migrateCheckUuid = ""
			this.migrateCheckStartTime = -1
		}
		return nil
	} else {
		// 检查是否有节点迁移
		this.migrateCheckUuid, err = this.optMigrateLock.Get()
		if err == nil && len(this.migrateCheckUuid) > 0 {
			this.migrateCheckStartTime = time.Now().Unix()
		}
	}

	return nil
}

func (this *SlotGroupSkeleton) GetMigrateOutSlot() int {
	return this.migrateSlot
}

func (this *SlotGroupSkeleton) FinishMigrateOutSlot(slot int) {
	// 无论结果如何，都要释放锁
	defer func() {
		this.migrateSlot = -1
		_, _ = this.optMigrateLock.Cas(this.nodeUuid, "")
	}()

	if slot != this.migrateSlot {
		return
	}
	// 查找最空闲的节点
	idleNode, idleMeta := this.findIdleNode()
	// 大家都很忙
	if idleMeta == nil {
		return
	}

	ok, err := this.optSlots.Cas(strconv.Itoa(slot), this.nodeUuid, idleNode.NodeUuid)
	if err != nil || !ok {
		return
	}
	this.SlotGroupStub.slots[this.migrateSlot] = idleNode.NodeUuid

	this.slotVersion += 1
	_, err = this.optSlotVersion.IncrBy(1)
	if err != nil {
		// redis节点迁移，节点崩溃都有可能出现这种情况
		gl.Warn("inc slot verson error %v", err)
		this.delayIncVersion += 1
	}
}

func (this *SlotGroupSkeleton) IsMgrRes(resid int64) bool {
	return this.slots[this.GetResSlot(resid)] == this.nodeUuid
}

func (this *SlotGroupSkeleton) GetResSlot(resid int64) int {
	return int(resid % int64(this.slotSize))
}

// 如果超载，并且有节点可以接受，则开始进行负载均衡
func (this *SlotGroupSkeleton) BalanceCheck(ress []ResClass) [][]int64 {
	atomic.StoreInt32(&this.load.CurPower, int32(len(ress)))
	// 如果未达到最大负载，什么都不需要做
	if this.load.CurPower < this.load.MaxPower || this.migrateSlot >= 0 {
		return this.checkInvalidMigrateSlots(ress)
	}

	// 负载均衡有时间间隔，如果未到时间也不进行检查
	now := time.Now().Unix()
	if this.preBalanceCheckTime+BalanceCheckGapSec > now {
		return this.checkInvalidMigrateSlots(ress)
	}
	this.preBalanceCheckTime = now

	// 查找最空闲的节点
	_, idleMeta := this.findIdleNode()
	// 大家都很忙
	if idleMeta == nil {
		return this.checkInvalidMigrateSlots(ress)
	}

	// 可以进行迁移，我们要迁移一个power最小的slot
	// 先进行槽内资源统计
	slotPower := make([]int, this.slotSize, this.slotSize)
	for _, resc := range ress {
		for _, res := range resc.Reses {
			slotPower[this.GetResSlot(res.id)] += res.power
		}
	}
	// 查找一个大于0，并且最小power的slot
	migrateSlotIdx := -1
	migrateSlotPower := math.MaxInt32
	for slotIdx, slotPower := range slotPower {
		if slotPower > 0 && slotPower < migrateSlotPower {
			migrateSlotIdx = slotIdx
			migrateSlotPower = slotPower
		}
	}
	// 目标节点如果容纳这些负荷超载就不再处理
	if idleMeta.CurPower+int32(migrateSlotPower) >= idleMeta.MaxPower {
		return this.checkInvalidMigrateSlots(ress)
	}
	ok, err := this.optMigrateLock.Cas("", this.nodeUuid)
	if err != nil || !ok {
		return this.checkInvalidMigrateSlots(ress)
	}
	this.migrateSlot = migrateSlotIdx
	return this.checkInvalidMigrateSlots(ress)
}

// 检查资源id中，是否有不属于这个节点管理的资源
func (this *SlotGroupSkeleton) checkInvalidMigrateSlots(ress []ResClass) [][]int64 {
	var migrateSlots [][]int64
	for _, resc := range ress {
		var mslots []int64
		for _, res := range resc.Reses {
			slot := res.id % int64(this.slotSize)
			if this.SlotGroupStub.slots[slot] != this.nodeUuid || slot == int64(this.migrateSlot) {
				mslots = append(mslots, res.id)
			}
		}
	}

	return migrateSlots
}

// 查找一个最空闲的节点
func (this *SlotGroupSkeleton) findIdleNode() (*cluster.NodeInfo, *NodeLoad) {
	var idleNode *cluster.NodeInfo = nil
	var idleMeta *NodeLoad = nil

	for _, node := range this.nodeMgr.GetAllNodesInfo() {
		nodeMeta, ok := node.Meta[MetaSlotSkeleton]
		if !ok {
			continue
		}
		nm := &NodeLoad{}
		err := json.Unmarshal([]byte(nodeMeta), nm)
		if err != nil {
			continue
		}
		if nm.FreePower() <= 0 {
			continue
		}
		if idleMeta == nil || idleMeta.FreePower() < nm.FreePower() {
			idleNode = node
			idleMeta = nm
		}
	}
	return idleNode, idleMeta
}

func (this *SlotGroupSkeleton) RaceSlot() {
	curMgrSlot := 0
	for _, uuid := range this.SlotGroupStub.slots {
		if uuid == this.nodeUuid {
			curMgrSlot++
		}
	}

	if curMgrSlot >= this.maxManageSlot {
		return
	}

	for idx, uuid := range this.SlotGroupStub.slots {
		if len(uuid) == 0 || !this.nodeMgr.IsNodeActive(uuid) {
			ok, err := this.optSlots.Cas(strconv.Itoa(idx), uuid, this.nodeUuid)
			if err == nil && ok {
				curMgrSlot++
				this.SlotGroupStub.slots[idx] = this.nodeUuid
			}
		}
		if curMgrSlot >= this.maxManageSlot {
			return
		}
	}
}

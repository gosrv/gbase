package cell

import (
	"github.com/gosrv/gbase/gdb/gredis"
	"github.com/gosrv/gbase/gl"
	"github.com/gosrv/gbase/gutil"
	"github.com/gosrv/goioc"
	"github.com/gosrv/goioc/util"
	"github.com/pkg/errors"
	"strconv"
	"time"
)

const (
	stubFastProcessGapSec = 1
	stubSlowProcessGapSec = 60
)

type SlotGroupStub struct {
	gioc.IConfigBase
	// 节点数据版本，使用optSlotVersion获取，版本变化时说明节点数据变化了
	slotVersion int

	// 操作节点总数
	optSlotSize *gredis.BoundValueOperation `bean:""`
	// 操作节点版本
	optSlotVersion *gredis.BoundValueOperation `bean:""`
	// 操作节点数据
	optSlots *gredis.BoundHashOperation `bean:""`

	// 节点数据
	slots []string
	// 快速处理时间间隔
	fastProcessGapSec int `cfg.d:"fastpgap"`
	// 慢速处理时间间隔
	slowProcessGapSec int `cfg.d:"slowpgap"`
}

func (this *SlotGroupStub) BeanStart() {
	this.GoStart()
}

func (this *SlotGroupStub) BeanStop() {

}

func NewSlotGroupStub(cfgBase string, optSlotSize *gredis.BoundValueOperation, optSlotVersion *gredis.BoundValueOperation,
	optSlots *gredis.BoundHashOperation, fastProcessGapSec int, slowProcessGapSec int) *SlotGroupStub {
	if slowProcessGapSec <= 0 {
		slowProcessGapSec = stubSlowProcessGapSec
	}
	if fastProcessGapSec <= 0 {
		fastProcessGapSec = stubFastProcessGapSec
	}
	return &SlotGroupStub{IConfigBase: gioc.NewConfigBase(cfgBase),
		optSlotSize: optSlotSize, optSlotVersion: optSlotVersion,
		optSlots: optSlots, fastProcessGapSec: fastProcessGapSec, slowProcessGapSec: slowProcessGapSec}
}

func (this *SlotGroupStub) GoStart() {
	gutil.RecoverGo(func() {
		// 先获取slot的大小
		for {
			slotSize := this.fetchSlotSize()
			if slotSize <= 0 {
				gl.Debug("wait for slot size")
				time.Sleep(time.Duration(this.fastProcessGapSec) * time.Second)
				continue
			}
			this.slots = make([]string, slotSize, slotSize)
			break
		}

		// 获取slot数据
		for {
			err := this.slowProcess()
			if err == nil {
				break
			}
			gl.Debug("wait for slot data")
			time.Sleep(time.Duration(this.fastProcessGapSec) * time.Second)
		}

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

func (this *SlotGroupStub) fetchSlotSize() int {
	strSize, err := this.optSlotSize.Get()
	if err != nil || len(strSize) == 0 {
		return 0
	}

	size, err := strconv.Atoi(strSize)
	util.VerifyNoError(err)

	return size
}

func (this *SlotGroupStub) GetResUuid(resid int64) string {
	if len(this.slots) == 0 {
		return ""
	}
	return this.slots[resid%int64(len(this.slots))]
}

// 比对版本号，如果版本号不一致就刷新数据
func (this *SlotGroupStub) fastProcess() error {
	versionData, err := this.optSlotVersion.Get()
	if err != nil {
		return err
	}
	curSlotVersion, err := strconv.Atoi(versionData)
	if err != nil {
		return err
	}
	if curSlotVersion == this.slotVersion {
		return nil
	}
	err = this.refreshServiceSlots()
	if err != nil {
		return err
	}
	this.slotVersion = curSlotVersion
	return nil
}

// 强制刷新数据，只有redis主从节点切换才会需要这个
// TODO: 跟node info判断是否需要处理
func (this *SlotGroupStub) slowProcess() error {
	versionData, err := this.optSlotVersion.Get()
	if err != nil {
		return err
	}
	curSlotVersion, err := strconv.Atoi(versionData)
	if err != nil {
		return err
	}
	err = this.refreshServiceSlots()
	if err != nil {
		return err
	}
	this.slotVersion = curSlotVersion
	return nil
}

func (this *SlotGroupStub) refreshServiceSlots() error {
	allSlot, err := this.optSlots.HGetAll()
	if err != nil {
		return err
	}

	if len(allSlot) < len(this.slots) {
		return errors.New("slot data not ready")
	}

	util.Verify(len(allSlot) == len(this.slots))

	for k, v := range allSlot {
		i, err := strconv.Atoi(k)
		util.VerifyNoError(err)
		this.slots[i] = v
	}
	return nil
}

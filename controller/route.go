package controller

import (
	"github.com/gosrv/gbase/gl"
	"github.com/gosrv/gbase/gnet"
	"github.com/gosrv/gbase/gproto"
)

type ControlPointRoute struct {
	cpGroup IControlPointGroup
}

func NewControlPointRoute(cpGroup IControlPointGroup) *ControlPointRoute {
	return &ControlPointRoute{cpGroup: cpGroup}
}

func (this *ControlPointRoute) GetRouteKeys() []interface{} {
	return this.GetRouteKeys()
}

func (this *ControlPointRoute) Connect(key interface{}, processor gproto.FProcessor) {
	gl.Panic("not support")
}

func (this *ControlPointRoute) GetRoute(key interface{}) []gproto.FProcessor {
	gl.Panic("not support")
	return nil
}

func (this *ControlPointRoute) Trigger(from interface{}, key interface{}, value interface{}) interface{} {
	controlPoint := this.cpGroup.GetControlPoint(key)
	if controlPoint == nil {
		gl.Warn("no route for key %v", key)
		return nil
	}
	return controlPoint.Controller.Trigger().Trigger(controlPoint, from.(gnet.ISessionCtx))
}

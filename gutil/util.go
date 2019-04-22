package gutil

import (
	"encoding/json"
	"github.com/gosrv/gbase/gl"
	"reflect"
)

func Json(ins interface{}) string {
	if ins == nil {
		return "nil"
	}
	val, err := json.Marshal(ins)
	if err != nil {
		gl.Panic("to json error %v", reflect.TypeOf(ins))
	}
	return string(val)
}

func IsNilValue(ins interface{}) bool {
	return ins == nil || reflect.ValueOf(ins).IsNil()
}

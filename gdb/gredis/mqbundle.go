package gredis

import (
	"fmt"
	"github.com/gosrv/gbase/gproto"
)

type RedisMQBundle struct {
	queue   string
	encoder gproto.IEncoder
	decoder gproto.IDecoder
	listOpt *ListOperation
}

func NewRedisMQBundle(queue string, encoder gproto.IEncoder, decoder gproto.IDecoder, listOpt *ListOperation) *RedisMQBundle {
	return &RedisMQBundle{queue: queue, encoder: encoder, decoder: decoder, listOpt: listOpt}
}

func (this *RedisMQBundle) queueName(id interface{}) string {
	return this.queue + fmt.Sprintf("%v", id)
}

func (this *RedisMQBundle) Push(id interface{}, msg interface{}) bool {
	_, err := this.listOpt.RPush(this.queueName(id), this.encoder.Encode(msg))
	return err == nil
}

func (this *RedisMQBundle) Pop(id interface{}, num int) []interface{} {
	val, err := this.listOpt.LPop(this.queueName(id))
	if err != nil {
		return nil
	}
	dv := this.decoder.Decode(val)
	return []interface{}{dv}
}


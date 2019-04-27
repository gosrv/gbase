package gredis

import (
	"time"
)

type RedisMessageQueue struct {
	queue   string
	listOpt *ListOperation
}

func NewRedisMessageQueue(queue string, listOpt *ListOperation) *RedisMessageQueue {
	return &RedisMessageQueue{queue: queue, listOpt: listOpt}
}

func (this *RedisMessageQueue) SetExpireDuration(duration time.Duration) error {
	ok, err := this.listOpt.Expire(this.queue, duration)
	if ok {
		return nil
	}
	return err
}

func (this *RedisMessageQueue) Pop(num int) []string {
	val, err := this.listOpt.LPop(this.queue)
	if err != nil {
		return nil
	}
	return []string{val}
}

func (this *RedisMessageQueue) Push(msg string) bool {
	_, err := this.listOpt.RPush(this.queue, msg)
	return err == nil
}

func (this *RedisMessageQueue) Name() string {
	return this.queue
}

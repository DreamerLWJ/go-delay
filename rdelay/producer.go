package rdelay

import (
	"context"
	"time"

	"github.com/DreamerLWJ/go-delay/api"
	"github.com/pkg/errors"
)

type QueueMemberBucketFunc func(member api.DelayQueueItem) int

type BucketProducer struct {
	rds         api.RedisClient
	bucketCount int
	bucketFunc  QueueBucketKeyFunc
	memberFunc  QueueMemberBucketFunc
}

func NewBucketProducer(rds api.RedisClient, bucketCount int, bucketFunc QueueBucketKeyFunc, memberFunc QueueMemberBucketFunc) *BucketProducer {
	return &BucketProducer{rds: rds, bucketCount: bucketCount, bucketFunc: bucketFunc, memberFunc: memberFunc}
}

func (b *BucketProducer) Send(ctx context.Context, member api.DelayQueueItem) error {
	bucketIdx := b.memberFunc(member)
	bucketKey := b.bucketFunc(bucketIdx)
	queue := NewQueue(b.rds, bucketKey)
	err := queue.Push(ctx, member)
	if err != nil {
		return errors.Errorf("Send|q.Push err:%s", err)
	}
	return nil
}

func (b *BucketProducer) SendDelay(ctx context.Context, member string, duration time.Duration) error {
	delayTime := time.Now().Add(duration).Unix()
	return b.Send(ctx, api.DelayQueueItem{
		TaskKey:   member,
		DelayTime: delayTime,
	})
}

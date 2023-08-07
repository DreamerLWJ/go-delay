package rdelay

import (
	"context"
	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
	"time"
)

type QueueMemberBucketFunc func(member QueueMember) int

type BucketProducer struct {
	rds         *redis.Client
	bucketCount int
	bucketFunc  QueueBucketKeyFunc
	memberFunc  QueueMemberBucketFunc
}

func NewBucketProducer(rds *redis.Client, bucketCount int, bucketFunc QueueBucketKeyFunc, memberFunc QueueMemberBucketFunc) *BucketProducer {
	return &BucketProducer{rds: rds, bucketCount: bucketCount, bucketFunc: bucketFunc, memberFunc: memberFunc}
}

func (b *BucketProducer) Send(ctx context.Context, member QueueMember) error {
	bucketIdx := b.memberFunc(member)
	bucketKey := b.bucketFunc(bucketIdx)
	// TODO pool opt
	queue := NewQueue(b.rds, bucketKey)
	err := queue.Push(ctx, member)
	if err != nil {
		return errors.Errorf("Send|q.Push err:%s", err)
	}
	return nil
}

func (b *BucketProducer) SendDelay(ctx context.Context, member string, duration time.Duration) error {
	delayTime := time.Now().Add(duration).Unix()
	return b.Send(ctx, QueueMember{
		Member:    member,
		DelayTime: delayTime,
	})
}

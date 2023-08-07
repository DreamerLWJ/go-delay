package rdelay

import (
	"context"
	"github.com/redis/go-redis/v9"
	"sync"
)

type QueueBucketKeyFunc func(bucketIdx int) string

type BucketConsumer struct {
	rds            *redis.Client
	bucketCount    int
	interval       int // 消费时间间隔
	bucketParallel int // 每个 bucket 消费的并行度
	keyFunc        QueueBucketKeyFunc
}

func NewBucketConsumer(rds *redis.Client, bucketCount int, interval int, bucketParallel int, keyFunc QueueBucketKeyFunc) *BucketConsumer {
	if rds != nil {
		if err := rds.Ping(context.Background()).Err(); err != nil {
			panic("redis not avail")
		}
	} else {
		panic("redis not allow nil")
	}
	if interval == 0 {
		panic("interval not allow 0")
	}
	if keyFunc == nil {
		panic("key func not allow nil")
	}
	if bucketCount == 0 {
		bucketCount = 1
	}
	if bucketParallel == 0 {
		bucketParallel = 1
	}
	return &BucketConsumer{rds: rds, bucketCount: bucketCount, interval: interval, bucketParallel: bucketParallel, keyFunc: keyFunc}
}

// StartConsume 外部传进来的应该是一个 cancelCtx
func (b *BucketConsumer) StartConsume(ctx context.Context, fn ConsumeFunc) {
	wg := sync.WaitGroup{}
	wg.Add(b.bucketCount)
	for i := 0; i < b.bucketCount; i++ {
		bucketKey := b.keyFunc(i)
		queue := NewQueue(b.rds, bucketKey)
		consumer := NewConsumer(queue, b.interval, b.bucketParallel)
		go func() {
			wg.Done()
			consumer.Consume(ctx, fn)
		}()
	}
	wg.Wait()
}

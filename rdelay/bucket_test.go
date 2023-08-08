package rdelay

import (
	"context"
	"fmt"
	"github.com/DreamerLWJ/go-delay/api"
	"github.com/redis/go-redis/v9"
	"strconv"
	"testing"
	"time"
)

func TestNewBucketConsumer(t *testing.T) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1:6379",
		Password: "123456",
	})
	defer func() {
		err := client.Close()
		if err != nil {
			panic(err)
		}
	}()

	bucketCount := 10

	bucketFunc := func(bucketIdx int) string {
		return fmt.Sprintf("test_bucket_%d", bucketIdx)
	}
	memberFunc := func(member api.QueueItem) int {
		uid, err := strconv.Atoi(member.TaskKey)
		if err != nil {
			// log
		}
		return uid % 10
	}

	bucketConsumer := NewBucketConsumer(client, bucketCount, 5, 2, bucketFunc)

	cancel, cancelFunc := context.WithCancel(ctx)
	defer cancelFunc()
	bucketConsumer.StartConsume(cancel, func(member api.QueueItem) {
		fmt.Println(member)
	})

	producer := NewBucketProducer(client, bucketCount, bucketFunc, memberFunc)
	for i := 0; i < 100; i++ {
		time.Sleep(time.Second)
		err := producer.SendDelay(ctx, fmt.Sprintf("%d", i+100), time.Second*10)
		if err != nil {
			panic(err)
		}
	}
}

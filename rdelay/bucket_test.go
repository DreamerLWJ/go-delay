package rdelay

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/DreamerLWJ/go-delay/api"
	"github.com/redis/go-redis/v9"
)

type testGoRedisClient struct {
	c *redis.Client
}

func newTestGoRedisClient(c *redis.Client) *testGoRedisClient {
	return &testGoRedisClient{c: c}
}

func (t *testGoRedisClient) ZAdd(ctx context.Context, key string, members ...api.ZMember) error {
	zs := make([]redis.Z, 0, len(members))
	for i := range members {
		zs = append(zs, redis.Z{
			Score:  members[i].Score,
			Member: members[i].Member,
		})
	}
	res := t.c.ZAdd(ctx, key, zs...)
	if res.Err() != nil {
		return res.Err()
	}
	return nil
}

func (t *testGoRedisClient) Eval(ctx context.Context, script string, keys []string, args ...any) (result []string, err error) {
	res := t.c.Eval(ctx, script, keys, args...)
	if res.Err() != nil {
		return result, res.Err()
	}
	return res.StringSlice()
}

func (t *testGoRedisClient) ZRem(ctx context.Context, key string, members ...any) error {
	res := t.c.ZRem(ctx, key, members...)
	return res.Err()
}

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
	memberFunc := func(member api.DelayQueueItem) int {
		uid, err := strconv.Atoi(member.TaskKey)
		if err != nil {
			// log
		}
		return uid % 10
	}

	redisClient := newTestGoRedisClient(client)

	bucketConsumer := NewBucketConsumer(redisClient, bucketCount, 5, 2, bucketFunc)

	cancel, cancelFunc := context.WithCancel(ctx)
	defer cancelFunc()
	bucketConsumer.StartConsume(cancel, func(member api.DelayQueueItem) {
		fmt.Println(member)
	})

	producer := NewBucketProducer(redisClient, bucketCount, bucketFunc, memberFunc)
	for i := 0; i < 100; i++ {
		time.Sleep(time.Second)
		err := producer.SendDelay(ctx, fmt.Sprintf("%d", i+100), time.Second*10)
		if err != nil {
			panic(err)
		}
	}
}

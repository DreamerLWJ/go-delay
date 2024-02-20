package rdelay

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/DreamerLWJ/go-delay/api"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

func TestNewQueue(t *testing.T) {
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

	redisClient := newTestGoRedisClient(client)

	queue := NewQueue(redisClient, "test_queue")

	err := queue.Push(ctx, api.DelayQueueItem{
		TaskKey:   "123",
		DelayTime: 1,
	})
	assert.Nil(t, err)

	err = queue.Push(ctx, api.DelayQueueItem{
		TaskKey:   "456",
		DelayTime: 2,
	})
	assert.Nil(t, err)

	err = queue.Push(ctx, api.DelayQueueItem{
		TaskKey:   "789",
		DelayTime: 3,
	})
	assert.Nil(t, err)

	err = queue.Push(ctx, api.DelayQueueItem{
		TaskKey:   "1234",
		DelayTime: time.Now().Unix(),
	})
	assert.Nil(t, err)

	err = queue.Push(ctx, api.DelayQueueItem{
		TaskKey:   "5678",
		DelayTime: time.Now().Unix(),
	})
	assert.Nil(t, err)

	members, err := queue.Poll(ctx, time.Now().Unix()-10, 2)
	assert.Nil(t, err)
	fmt.Println(members)
}

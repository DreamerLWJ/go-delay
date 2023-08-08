package rdelay

import (
	"context"
	"fmt"
	"github.com/DreamerLWJ/go-delay/api"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
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

	queue := NewQueue(client, "test_queue")

	err := queue.Push(ctx, api.QueueItem{
		TaskKey:   "123",
		DelayTime: 1,
	})
	assert.Nil(t, err)

	err = queue.Push(ctx, api.QueueItem{
		TaskKey:   "456",
		DelayTime: 2,
	})
	assert.Nil(t, err)

	err = queue.Push(ctx, api.QueueItem{
		TaskKey:   "789",
		DelayTime: 3,
	})
	assert.Nil(t, err)

	err = queue.Push(ctx, api.QueueItem{
		TaskKey:   "1234",
		DelayTime: time.Now().Unix(),
	})
	assert.Nil(t, err)

	err = queue.Push(ctx, api.QueueItem{
		TaskKey:   "5678",
		DelayTime: time.Now().Unix(),
	})
	assert.Nil(t, err)

	members, err := queue.Poll(ctx, time.Now().Unix()-10, 2)
	assert.Nil(t, err)
	fmt.Println(members)
}

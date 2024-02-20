package rdelay

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/DreamerLWJ/go-delay/api"
	"github.com/redis/go-redis/v9"
)

func TestNewConsumer(t *testing.T) {
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
	consumer := NewConsumer(queue, 10, 1)
	ctx, cancelFunc := context.WithCancel(context.Background())
	go func() {
		consumer.Consume(ctx, func(member api.DelayQueueItem) {
			fmt.Println(member)
		})
	}()

	time.Sleep(time.Minute)
	cancelFunc()
	time.Sleep(time.Second)
}

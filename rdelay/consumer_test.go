package rdelay

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"testing"
	"time"
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
	queue := NewQueue(client, "test_queue")
	consumer := NewConsumer(queue, 10, 1)
	ctx, cancelFunc := context.WithCancel(context.Background())
	go func() {
		consumer.Consume(ctx, func(member QueueMember) {
			fmt.Println(member)
		})
	}()

	time.Sleep(time.Minute)
	cancelFunc()
	time.Sleep(time.Second)
}

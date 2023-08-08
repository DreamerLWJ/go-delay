package main

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"time"
)

func main() {
	// to produce messages
	topic := "my-topic"
	broker := "kafka:9092"

	ctx := context.Background()

	writer := kafka.Writer{
		Addr: kafka.TCP(broker),
	}
	defer func() {
		err := writer.Close()
		if err != nil {
			panic(err)
		}
	}()

	for i := 0; i < 1; i++ {
		time.Sleep(time.Second)
		err := writer.WriteMessages(ctx, kafka.Message{
			Topic: topic,
			Value: []byte(fmt.Sprintf("test-msg: %d", i)),
		})
		if err != nil {
			panic(err)
		}
		fmt.Println("send success")
	}
}

package main

import (
	"context"
	"github.com/segmentio/kafka-go"
)

func main() {
	// to produce messages
	topic := "my-topic"
	broker := "kafka:9092"

	ctx := context.Background()

	writer := kafka.Writer{
		Addr:  kafka.TCP(broker),
		Topic: topic,
	}
	defer func() {
		err := writer.Close()
		if err != nil {
			panic(err)
		}
	}()

	err := writer.WriteMessages(ctx,
		kafka.Message{Value: []byte("one!")},
		kafka.Message{Value: []byte("two!")},
		kafka.Message{Value: []byte("three!")},
	)
	if err != nil {
		panic(err)
	}
}

package main

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
)

func main() {
	// to consume messages
	topic := "my-topic"
	groupID := "my-topic-group"
	broker := "kafka:9092"

	ctx := context.Background()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{broker},
		GroupID:     groupID,
		GroupTopics: []string{topic},
		Topic:       topic,
	})
	defer reader.Close()

	for {
		message, err := reader.ReadMessage(ctx)
		if err != nil {
			panic(err)
		}
		fmt.Printf("%+v\n", message)
	}
}

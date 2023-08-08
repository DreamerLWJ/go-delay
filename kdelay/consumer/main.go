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
		StartOffset: kafka.FirstOffset,
	})
	defer reader.Close()

	fmt.Println("new success")

	for {
		message, err := reader.FetchMessage(ctx)
		if err != nil {
			panic(err)
		}
		fmt.Printf("%+v\n", message)
		err = reader.CommitMessages(ctx, message)
		if err != nil {
			panic(err)
		}
	}
}

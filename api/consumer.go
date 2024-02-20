package api

import "context"

// ConsumeFunc func used to consume delay task
type ConsumeFunc func(member DelayQueueItem)

type Consumer interface {
	Consume(ctx context.Context, fn ConsumeFunc)
}

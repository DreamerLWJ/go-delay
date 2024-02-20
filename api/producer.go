package api

import (
	"context"
	"time"
)

type DelayMsgProducer interface {
	Send(ctx context.Context, member DelayQueueItem) error

	SendDelay(ctx context.Context, taskKey string, duration time.Duration) error
}

// NormalMsgProducer normal msg specific as msg without delay demand
type NormalMsgProducer interface {
}

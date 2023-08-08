package api

import (
	"context"
	"time"
)

type Producer interface {
	Send(ctx context.Context, member QueueItem) error

	SendDelay(ctx context.Context, taskKey string, duration time.Duration) error
}

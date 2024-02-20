package api

import "context"

// DelayQueueItem the abstract of delay queue
type DelayQueueItem struct {
	TaskKey   string // task key used to specific task
	DelayTime int64  // effective time
	Extra     any    // extra data, not support in redis delay queue,
}

// Queue delay queue define interface
type Queue interface {
	Push(ctx context.Context, item DelayQueueItem) error                        // push delay task inside queue
	Poll(ctx context.Context, pollSize int) (items []DelayQueueItem, err error) // poll expired delay task
	Del(ctx context.Context, taskKey string) error                              // del delay task
}

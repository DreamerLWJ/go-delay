package api

import "context"

// QueueItem the abstract of delay queue
type QueueItem struct {
	TaskKey   string // task key used to specific task
	DelayTime int64  // effective time
	Extra     any    // extra data, not support in redis delay queue,
}

// Queue delay queue define interface
type Queue interface {
	Push(ctx context.Context, item QueueItem) error                        // push delay task inside queue
	Poll(ctx context.Context, pollSize int) (items []QueueItem, err error) // poll expired delay task
	Del(ctx context.Context, taskKey string) error                         // del delay task
}

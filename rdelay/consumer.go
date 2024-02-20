package rdelay

import (
	"context"
	"fmt"
	"time"

	"github.com/DreamerLWJ/go-delay/api"
)

type Consumer struct {
	q        *Queue
	interval int // consume interval, or poll msg interval
	parallel int // goroutine count
}

func NewConsumer(q *Queue, interval int, parallel int) *Consumer {
	if q == nil {
		panic("queue not allow nil")
	}
	if interval == 0 {
		interval = 1
	}
	if parallel == 0 {
		parallel = 1
	}
	return &Consumer{q: q, interval: interval, parallel: parallel}
}

// Consume Externally, ctx should be assigned a value such as context.WithCancel
func (c *Consumer) Consume(ctx context.Context, fn api.ConsumeFunc) {
	msgChan := make(chan api.DelayQueueItem, c.parallel)
	c.initWorker(c.parallel*10, msgChan, fn)
	for {
		select {
		case <-ctx.Done():
			close(msgChan)
			return
		case <-time.After(time.Duration(c.interval) * time.Second):
			members, err := c.q.Poll(ctx, time.Now().Unix(), c.parallel*10)
			if err != nil {
				fmt.Println(err)
				// log
			}
			for i := range members {
				msgChan <- members[i]
			}
		}
	}
}

// init parallel consume goroutine
func (c *Consumer) initWorker(num int, msgChan chan api.DelayQueueItem, fn api.ConsumeFunc) {
	for i := 0; i < num; i++ {
		go func() {
			msg, notClose := <-msgChan
			if !notClose { // chan closed
				return
			}
			fn(msg)
		}()
	}
}

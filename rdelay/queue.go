package rdelay

import (
	"context"
	"strconv"

	"github.com/DreamerLWJ/go-delay/api"
	"github.com/pkg/errors"
)

const (
	_pollExpireMemberScript = `
local key = KEYS[1]
local maxScore = ARGV[1]
local limit = tonumber(ARGV[2]) or 0

local elementsWithScores = redis.call('ZRANGEBYSCORE', key, '-inf', maxScore, 'WITHSCORES', 'LIMIT', 0, limit)

if #elementsWithScores > 0 then
    redis.call('ZREM', key, unpack(elementsWithScores))
end

return elementsWithScores
`
)

type Queue struct {
	rds api.RedisClient
	key string
}

func NewQueue(rds api.RedisClient, key string) *Queue {
	return &Queue{rds: rds, key: key}
}

func (q *Queue) Push(ctx context.Context, member api.DelayQueueItem) error {
	err := q.rds.ZAdd(ctx, q.key, api.ZMember{
		Score:  float64(member.DelayTime),
		Member: member.TaskKey,
	})
	if err != nil {
		return errors.Errorf("Push|rds.ZAdd err:%s", err)
	}
	return nil
}

func (q *Queue) Poll(ctx context.Context, nowUnix int64, pollSize int) (members []api.DelayQueueItem, err error) {
	logHead := "Poll|"
	result, err := q.rds.Eval(ctx, _pollExpireMemberScript, []string{q.key}, nowUnix, pollSize)
	if err != nil {
		return members, errors.Errorf(logHead+"rds.Eval err:%s", err)
	}
	for i := range result {
		if i%2 == 0 {
			continue
		}
		sendTime, err := strconv.ParseInt(result[i], 10, 64)
		if err != nil {
			// log
			continue
		}
		members = append(members, api.DelayQueueItem{
			TaskKey:   result[i-1],
			DelayTime: sendTime,
		})
	}
	return members, nil
}

func (q *Queue) Del(ctx context.Context, member string) error {
	err := q.rds.ZRem(ctx, q.key, member)
	if err != nil {
		return errors.Errorf("Del|rds.ZRem err:%s", err)
	}
	return nil
}

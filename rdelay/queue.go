package rdelay

import (
	"context"
	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
	"strconv"
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

type QueueMember struct {
	Member    string
	DelayTime int64 // 生效时间
}

type Queue struct {
	rds *redis.Client
	key string
}

func NewQueue(rds *redis.Client, key string) *Queue {
	return &Queue{rds: rds, key: key}
}

func (q *Queue) Push(ctx context.Context, member QueueMember) error {
	err := q.rds.ZAdd(ctx, q.key, redis.Z{
		Score:  float64(member.DelayTime),
		Member: member.Member,
	}).Err()
	if err != nil {
		return errors.Errorf("Push|rds.ZAdd err:%s", err)
	}
	return nil
}

func (q *Queue) Poll(ctx context.Context, nowUnix int64, pollSize int) (members []QueueMember, err error) {
	logHead := "Poll|"
	result, err := q.rds.Eval(ctx, _pollExpireMemberScript, []string{q.key}, nowUnix, pollSize).StringSlice()
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
		members = append(members, QueueMember{
			Member:    result[i-1],
			DelayTime: sendTime,
		})
	}
	return members, nil
}

func (q *Queue) Del(ctx context.Context, member string) error {
	err := q.rds.ZRem(ctx, q.key, member).Err()
	if err != nil {
		return errors.Errorf("Del|rds.ZRem err:%s", err)
	}
	return nil
}

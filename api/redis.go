package api

import "context"

type ZMember struct {
	Score  float64
	Member any
}

// RedisClient the redis client used inside redis implementation
type RedisClient interface {
	ZAdd(ctx context.Context, key string, members ...ZMember) error

	Eval(ctx context.Context, script string, keys []string, args ...any) (result []string, err error)

	ZRem(ctx context.Context, key string, member ...any) error
}

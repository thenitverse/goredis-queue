package queue

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

const pendingKey = "queue:pending"

type QueueClient struct {
	redisClient *redis.Client
}

func NewQueueClient(ctx context.Context, address string) (*QueueClient, error) {
	rds := redis.NewClient(&redis.Options{
		Addr: address,
	})
	if err := rds.Ping(ctx).Err(); err != nil {
		return nil, err
	}
	return &QueueClient{redisClient: rds}, nil

}

func (q *QueueClient) Close() error {
	return q.redisClient.Close()
}

func (q *QueueClient) Enqueue(ctx context.Context, jobID string) error {
	return q.redisClient.LPush(ctx, pendingKey, jobID).Err()
}

func (q *QueueClient) Dequeue(ctx context.Context, timeout time.Duration) (string, bool, error) {
	res, err := q.redisClient.BRPop(ctx, timeout, pendingKey).Result()
	if errors.Is(err, redis.Nil) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return res[1], true, nil
}

package queue

import (
	"context"
	"errors"
	"time"
)

const (
	StatusQueued     = "QUEUED"
	StatusProcessing = "PROCESSING"
	StatusCompleted  = "COMPLETED"
	StatusFailed     = "FAILED"
)

var ErrJobNotFound = errors.New("job not found")

type Job struct {
	ID        string `redis:"id"`
	Status    string `redis:"status"`
	Payload   string `redis:"payload"`
	Retries   int    `redis:"retries"`
	Error     string `redis:"error"`
	CreatedAt int64  `redis:"created_at"`
	UpdatedAt int64  `redis:"updated_at"`
}

func jobKey(id string) string {
	jobId := "job:" + id
	return jobId
}

func now() int64 {
	return time.Now().UnixMilli()
}

func (q *QueueClient) Enqueue(ctx context.Context, id, payload string) error {
	pipe := q.redisClient.TxPipeline()
	pipe.HSet(ctx, jobKey(id), map[string]any{
		"id":         id,
		"status":     StatusQueued,
		"payload":    payload,
		"retries":    0,
		"created_at": now(),
		"updated_at": now(),
	})
	pipe.LPush(ctx, pendingKey, id)
	_, err := pipe.Exec(ctx)
	return err
}

func (q *QueueClient) Getjob(ctx context.Context, id string) (*Job, error) {
	cmd := q.redisClient.HGetAll(ctx, jobKey(id))
	if err := cmd.Err(); err != nil {
		return nil, err
	}
	if len(cmd.Val()) == 0 {
		return nil, ErrJobNotFound
	}
	var j Job
	if err := cmd.Scan(&j); err != nil {
		return nil, err
	}
	return &j, nil

}
func (q *QueueClient) SetStatus(ctx context.Context, id, status string) error {
	return q.redisClient.HSet(ctx, jobKey(id), "status", status, "updated_at", now()).Err()
}
func (q *QueueClient) MarkFailed(ctx context.Context, id, errmsg string) error {
	pipe := q.redisClient.TxPipeline()
	pipe.HSet(ctx, jobKey(id), "status", StatusFailed, "error", errmsg, "updated_at", now())
	pipe.HIncrBy(ctx, jobKey(id), "retries", 1)
	_, err := pipe.Exec(ctx)
	return err

}

func (q *QueueClient) RecoverStale(ctx context.Context, maxAge time.Duration) (int, error) {
	ids, err := q.redisClient.LRange(ctx, processingKey, 0, -1).Result()
	if err != nil {
		return 0, err
	}
	recovered := 0
	for _, id := range ids {
		job, err := q.Getjob(ctx, id)
		if err != nil {
			continue
		}
		if now()-job.UpdatedAt < maxAge.Milliseconds() {
			continue
		}
		pipe := q.redisClient.TxPipeline()
		pipe.LRem(ctx, processingKey, 1, id)
		pipe.HSet(ctx, jobKey(id), "status", StatusQueued, "updated_at", now())
		pipe.HIncrBy(ctx, jobKey(id), "retries", 1)
		pipe.LPush(ctx, pendingKey, id)
		if _, err := pipe.Exec(ctx); err != nil {
			return recovered, err
		}
		recovered++
	}
	return recovered, nil

}

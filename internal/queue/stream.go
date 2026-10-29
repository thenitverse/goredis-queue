package queue

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	streamKey = "stream:jobs"
	groupName = "workers"
)

func (q *QueueClient) EnsureGroup(ctx context.Context) error {
	err := q.redisClient.XGroupCreateMkStream(ctx, streamKey, groupName, "$").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return err
	}
	return nil
}

func (q *QueueClient) StreamEnqueue(ctx context.Context, id, payload string) error {
	pipe := q.redisClient.TxPipeline()
	pipe.HSet(ctx, jobKey(id), map[string]any{
		"id":         id,
		"status":     StatusQueued,
		"payload":    payload,
		"retries":    0,
		"created_at": now(),
		"updated_at": now(),
	})
	pipe.XAdd(ctx, &redis.XAddArgs{Stream: streamKey, Values: map[string]any{
		"job_id": id}})

	_, err := pipe.Exec(ctx)
	return err
}

type Message struct {
	StreamID string
	jobID    string
}

func (q *QueueClient) StreamRoad(ctx context.Context, consumer string, block time.Duration) (*Message, error) {
	res, err := q.redisClient.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    groupName,
		Consumer: consumer,
		Streams:  []string{streamKey, ">"},
		Count:    1,
		Block:    block,
	}).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	m := res[0].Messages[0]
	id, ok := m.Values["job_id"].(string)
	if !ok {
		return nil, fmt.Errorf("message %s has no valid job_id", m.ID)
	}

	return &Message{StreamID: m.ID, jobID: id}, nil
}
func (q *QueueClient) StreamAck(ctx context.Context, streamID string) error {
	pipe := q.redisClient.TxPipeline()
	pipe.XAck(ctx, streamKey, groupName, streamID)
	pipe.XDel(ctx, streamKey, streamID) // optional: keep the stream from growing forever
	_, err := pipe.Exec(ctx)
	return err
}

// StreamClaimStale takes over messages idle longer than minIdle (crashed consumers).
func (q *QueueClient) StreamClaimStale(ctx context.Context, consumer string, minIdle time.Duration) ([]Message, error) {
	msgs, _, err := q.redisClient.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream: streamKey, Group: groupName, Consumer: consumer,
		MinIdle: minIdle, Start: "0-0", Count: 10,
	}).Result()
	if err != nil {
		return nil, err
	}
	out := make([]Message, 0, len(msgs))
	for _, m := range msgs {
		id, ok := m.Values["job_id"].(string)
		if !ok {
			return nil, fmt.Errorf("message %s has no valied job_id", m.ID)
		}
		out = append(out, Message{StreamID: m.ID, jobID: id})
	}
	return out, nil
}

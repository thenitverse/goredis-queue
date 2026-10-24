package main

import (
	"context"
	"log"
	"time"

	"github.com/thenitverse/goredis-queue/internal/queue"
)

func main() {
	ctx := context.Background()

	q, err := queue.NewQueueClient(ctx, "localhost:6379")
	if err != nil {
		log.Fatal(err)
	}
	defer q.Close()

	err = q.EnqueueDelayed(
		ctx,
		"phase4-test-001",
		`{"n":101}`,
		10*time.Second,
	)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Scheduled delayed-test-1 for 10 seconds from now")
}

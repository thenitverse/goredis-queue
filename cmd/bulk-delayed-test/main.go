package main

import (
	"context"
	"fmt"
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
	for i := 1; i <= 50; i++ {
		id := fmt.Sprintf("bulk-test-%03d", i)

		err := q.EnqueueDelayed(
			ctx, id, fmt.Sprintf(`{"n":%d}`, i),
			15*time.Second,
		)
		if err != nil {
			log.Fatalf("Failed to schedule %s; %v", id, err)
		}
	}
	fmt.Println("Successfully scheduled 50 delayed jobs....")
}

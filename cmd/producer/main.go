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
		log.Fatalf("Producer failed to connect to redis: %v", err)
	}
	defer q.Close()

	fmt.Println("Producer started. Enqueuing 10 jobs...")
	failed := 0

	for i := 1; i <= 10; i++ {
		jobID := fmt.Sprintf("job-id-%d", i)
		payload := fmt.Sprintf(`{"n":%d}`, i)
		if err := q.Enqueue(ctx, jobID, payload); err != nil {
			log.Printf("Failed to enqueue %s: %v", jobID, err)
			failed++
		} else {
			fmt.Printf("Enqueued: %s\n", jobID)
		}
		time.Sleep(200 * time.Millisecond)

	}
	if failed > 0 {
		log.Fatalf("%d of 10 jobs failed to enqueue", failed)
	}
	fmt.Println("All 10 jobs enqueued successfully.  Exiting...")
	// Enqueue delayed jobs
	if err := q.EnqueueDelayed(ctx, "job-id-11", `{"n":11}`, 10*time.Second); err != nil {
		log.Printf("Failed to enqueue delayed job 11: %v", err)
	} else {
		fmt.Println("Scheduled job-id-11 (10 seconds delay)")
	}

	if err := q.EnqueueDelayed(ctx, "job-id-12", `{"n":12}`, 20*time.Second); err != nil {
		log.Printf("Failed to enqueue delayed job 12: %v", err)
	} else {
		fmt.Println("Scheduled job-id-12 (20 seconds delay)")
	}
}

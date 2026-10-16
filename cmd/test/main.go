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

	// 1. Connect to Redis using your new constructor
	q, err := queue.NewQueueClient(ctx, "localhost:6379")
	if err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}
	defer q.Close()

	// 2. Enqueue a test job
	jobID := "job-12345"
	fmt.Println("Enqueuing job:", jobID)
	if err := q.Enqueue(ctx, jobID); err != nil {
		log.Fatalf("Failed to enqueue: %v", err)
	}

	// 3. Dequeue the job
	fmt.Println("Waiting to dequeue job...")
	job, found, err := q.Dequeue(ctx, 5*time.Second)
	if err != nil {
		log.Fatalf("Dequeue error: %v", err)
	}

	if !found {
		fmt.Println("Timeout reached: No jobs found.")
		return
	}

	fmt.Printf("Successfully processed job: %s\n", job)
}

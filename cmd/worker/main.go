package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/thenitverse/goredis-queue/internal/queue"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	q, err := queue.NewQueueClient(ctx, "localhost:6379")
	if err != nil {
		log.Fatalf("worker failed to connect to redis: %v", err)
	}
	defer q.Close()

	for ctx.Err() == nil {
		jobID, ok, err := q.Dequeue(ctx, 10*time.Second)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			log.Printf("dequeue error: %v", err)
			time.Sleep(time.Second)
			continue
		}
		if !ok {
			continue
		}
		fmt.Printf("Processing %s\n", jobID)
		if err := q.SetStatus(context.Background(), jobID, queue.StatusProcessing); err != nil {
			log.Printf("set status processing: %v", err)
		}

		time.Sleep(30 * time.Second) // real work goes here

		if err := q.SetStatus(context.Background(), jobID, queue.StatusCompleted); err != nil {
			log.Printf("set status completed: %v", err)
		}
		if err := q.Ack(context.Background(), jobID); err != nil {
			log.Printf("ack: %v", err)
		}
		fmt.Printf("done %s\n", jobID)

	}

}

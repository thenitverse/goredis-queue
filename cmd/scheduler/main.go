package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/thenitverse/goredis-queue/internal/queue"
)

func main() {
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt, syscall.SIGTERM,
	)
	defer stop()

	q, err := queue.NewQueueClient(ctx, "localhost:6379")
	if err != nil {
		log.Fatalf("scheduler failed to connect to redis: %v", err)

	}
	defer q.Close()
	log.Println("Scheduler started...")

	moveDue := func() {
		n, err := q.MoveDue(ctx, 100)
		if err != nil {
			log.Printf("move due error: %v", err)
			return
		}
		if n > 0 {
			log.Printf("moved %d due job(s)", n)
		}
	}
	moveDue()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("Scheduler shutting down")
			return
		case <-ticker.C:
			moveDue()
		}
	}
}

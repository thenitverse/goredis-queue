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
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	q, err := queue.NewQueueClient(ctx, "localhost:6379")
	if err != nil {
		log.Fatalf("reaper failed to connect to Redis: %v", err)
	}
	defer q.Close()

	log.Println("Reaper started")

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	// Check immediately on startup, then periodically.
	recoverJobs := func() {
		n, err := q.RecoverStale(ctx, 45*time.Second)
		if err != nil {
			log.Printf("recovery error: %v", err)
			return
		}
		if n > 0 {
			log.Printf("recovered %d stale job(s)", n)
		}
	}

	recoverJobs()

	for {
		select {
		case <-ctx.Done():
			log.Println("Reaper shutting down")
			return
		case <-ticker.C:
			recoverJobs()
		}
	}
}

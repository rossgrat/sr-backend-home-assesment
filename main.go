package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sr-backend-home-assessment/src/db"
	"sr-backend-home-assessment/src/worker"
	"sync"
	"syscall"
)

func main() {
	fmt.Println("Starting application...")
	ctx := context.Background()
	db, err := db.Init(ctx, "postgres://kafkauser:kafkapass@postgres:5432/kafkadb")
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	wg1 := sync.WaitGroup{}

	wg1.Go(func() {
		worker.RunWorker(ctx)
	})

	go func() {
		<-sigs
		cancel()
	}()

	wg1.Wait()

}

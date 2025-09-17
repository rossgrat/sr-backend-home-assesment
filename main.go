package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sr-backend-home-assessment/src/db"
	"sr-backend-home-assessment/src/worker/cleaner"
	"sr-backend-home-assessment/src/worker/packer"
	"sync"
	"syscall"

	"github.com/go-chi/chi/v5"
)

func main() {
	fmt.Println("Starting application...")
	ctx := context.Background()
	_, err := db.Init(ctx, "postgres://kafkauser:kafkapass@postgres:5432/kafkadb")
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	wg1 := sync.WaitGroup{}
	wg2 := sync.WaitGroup{}

	wg1.Go(func() {
		cleaner.RunCleaner(ctx)
	})

	wg2.Go(func() {
		packer.RunPacker(ctx)
	})

	go func() {
		<-sigs
		cancel()
	}()

	// Setup chi router
	r := chi.NewRouter()
	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	// Start HTTP server
	go func() {
		fmt.Println("HTTP server listening on :8080")
		if err := http.ListenAndServe(":8080", r); err != nil {
			fmt.Printf("HTTP server error: %v\n", err)
			cancel()
		}
	}()

	wg1.Wait()
	wg2.Wait()

}

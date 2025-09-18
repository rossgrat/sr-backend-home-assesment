package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"sr-backend-home-assessment/src/cache"
	"sr-backend-home-assessment/src/db"
	"sr-backend-home-assessment/src/processors/cleaner"
	"sr-backend-home-assessment/src/processors/packer"
	"sync"
	"syscall"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	slog.InfoContext(ctx, "Starting service...")

	_, err := db.Init(ctx, &db.Config{
		ConnString:     "postgres://kafkauser:kafkapass@postgres:5432/kafkadb?sslmode=disable",
		MigrationsPath: "/app/src/db/migrations",
	})
	if err != nil {
		panic(err)
	}

	cache := cache.New(&cache.Config{
		Brokers:       "kafka:29092",
		ConsumerTopic: "device_events_cleaned_compacted",
	})
	cache.Hydrate(ctx)
	slog.InfoContext(ctx, "Cache hydrated with initial data")
	cache.Dump()

	wg1 := sync.WaitGroup{}
	wg2 := sync.WaitGroup{}

	wCleaner := cleaner.New(&cleaner.Config{
		Brokers:         "kafka:29092",
		ConsumerGroupID: "cleaner-group",
		ConsumerTopic:   "device-events",
		PublisherTopic:  "device_events_cleaned",
		Cache:           cache,
	})
	wPacker := packer.New(&packer.Config{
		Brokers:         "kafka:29092",
		ConsumerGroupID: "packer-group",
		ConsumerTopic:   "device_events_cleaned",
		PublisherTopic:  "device_events_cleaned_compacted",
	})

	wg1.Go(func() {
		wCleaner.Run(ctx)
	})
	wg2.Go(func() {
		wPacker.Run(ctx)
	})

	go func() {
		<-sigs
		cancel()
	}()

	// // Setup chi router
	// r := chi.NewRouter()
	// r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
	// 	w.WriteHeader(http.StatusOK)
	// 	w.Write([]byte("OK"))
	// })

	// // Start HTTP server
	// go func() {
	// 	fmt.Println("HTTP server listening on :8080")
	// 	if err := http.ListenAndServe(":8080", r); err != nil {
	// 		fmt.Printf("HTTP server error: %v\n", err)
	// 		cancel()
	// 	}
	// }()

	wg1.Wait()
	wg2.Wait()

	wCleaner.Close(ctx)
	wPacker.Close(ctx)

}

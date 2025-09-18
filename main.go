package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sr-backend-home-assessment/internal/api"
	"sr-backend-home-assessment/internal/cache"
	"sr-backend-home-assessment/internal/db"
	"sr-backend-home-assessment/internal/processors/cleaner"
	"sr-backend-home-assessment/internal/processors/packer"
	"sync"
	"syscall"

	"github.com/go-chi/chi/v5"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	slog.InfoContext(ctx, "Starting service...")

	// Setup API and DB
	db, err := db.Init(ctx, db.Config{
		ConnString:     "postgres://kafkauser:kafkapass@postgres:5432/kafkadb?sslmode=disable",
		MigrationsPath: "/app/src/db/migrations",
	})
	if err != nil {
		panic(err)
	}
	api := api.New(api.Config{
		DB: db,
	})

	r := chi.NewRouter()
	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})
	r.Post("/timeline", api.CreateDeviceTimeline)
	r.Get("/timeline/{device_id}", api.GetDeviceTimeline)

	// Setup event cleaner
	cache := cache.New(cache.Config{
		Brokers:       "kafka:29092",
		ConsumerTopic: "device_events_cleaned_compacted",
	})
	cache.Hydrate(ctx)
	slog.InfoContext(ctx, "Cache hydrated with initial data")
	cache.Dump()
	wCleaner := cleaner.New(cleaner.Config{
		Brokers:         "kafka:29092",
		ConsumerGroupID: "cleaner-group",
		ConsumerTopic:   "device-events",
		PublisherTopic:  "device_events_cleaned",
		Cache:           cache,
	})

	wPacker := packer.New(packer.Config{
		Brokers:         "kafka:29092",
		ConsumerGroupID: "packer-group",
		ConsumerTopic:   "device_events_cleaned",
		PublisherTopic:  "device_events_cleaned_compacted",
	})

	// Run waitgroups for cleaner, mover, and REST API
	wg1 := sync.WaitGroup{}
	wg2 := sync.WaitGroup{}
	wg3 := sync.WaitGroup{}
	wg1.Go(func() {
		wCleaner.Run(ctx)
	})
	wg2.Go(func() {
		wPacker.Run(ctx)
	})
	wg3.Go(func() {
		slog.InfoContext(ctx, "HTTP server listening on :8080")
		if err := http.ListenAndServe(":8080", r); err != nil {
			slog.ErrorContext(ctx, "HTTP server error", "error", err)
			cancel()
		}
	})

	go func() {
		<-sigs
		cancel()
	}()

	wg1.Wait()
	wg2.Wait()
	wg3.Wait()

	wCleaner.Close(ctx)
	wPacker.Close(ctx)

}

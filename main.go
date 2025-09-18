package main

import (
	"context"
	"fmt"
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
	"github.com/spf13/viper"
)

type Config struct {
	DBUser                                 string `mapstructure:"DB_USER"`
	DBPassword                             string `mapstructure:"DB_PASSWORD"`
	DBName                                 string `mapstructure:"DB_NAME"`
	KafkaBroker                            string `mapstructure:"KAFKA_BROKER"`
	KafkaDeviceEventsTopic                 string `mapstructure:"KAFKA_DEVICE_EVENTS_TOPIC"`
	KafkaDeviceEventsCleanedTopic          string `mapstructure:"KAFKA_DEVICE_EVENTS_CLEANED_TOPIC"`
	KafkaDeviceEventsCleanedCompactedTopic string `mapstructure:"KAFKA_DEVICE_EVENTS_CLEANED_COMPACTED_TOPIC"`
	MigrationsPath                         string `mapstructure:"MIGRATIONS_PATH"`
}

func loadConfig() (Config, error) {
	viper.SetConfigFile(".env")
	if err := viper.ReadInConfig(); err != nil {
		return Config{}, fmt.Errorf("error reading .env file: %w", err)
	}

	var config Config
	if err := viper.Unmarshal(&config); err != nil {
		return Config{}, fmt.Errorf("unable to decode into struct: %w", err)
	}
	return config, nil
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	slog.InfoContext(ctx, "Starting service...")

	config, err := loadConfig()
	if err != nil {
		panic(fmt.Errorf("failed to load configuration: %w", err))
	}

	// Setup API and DB
	db, err := db.Init(ctx, db.Config{
		ConnString:     fmt.Sprintf("postgres://%s:%s@postgres:5432/%s?sslmode=disable", config.DBUser, config.DBPassword, config.DBName),
		MigrationsPath: config.MigrationsPath,
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
		Brokers:       config.KafkaBroker,
		ConsumerTopic: config.KafkaDeviceEventsCleanedCompactedTopic,
	})
	cache.Hydrate(ctx)
	slog.InfoContext(ctx, "Cache hydrated with initial data")
	cache.Dump()
	wCleaner := cleaner.New(cleaner.Config{
		Brokers:         config.KafkaBroker,
		ConsumerGroupID: "cleaner-group",
		ConsumerTopic:   config.KafkaDeviceEventsTopic,
		PublisherTopic:  config.KafkaDeviceEventsCleanedTopic,
		Cache:           cache,
	})

	wPacker := packer.New(packer.Config{
		Brokers:         config.KafkaBroker,
		ConsumerGroupID: "packer-group",
		ConsumerTopic:   config.KafkaDeviceEventsCleanedTopic,
		PublisherTopic:  config.KafkaDeviceEventsCleanedCompactedTopic,
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

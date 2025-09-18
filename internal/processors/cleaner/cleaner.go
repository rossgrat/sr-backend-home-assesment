package cleaner

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sr-backend-home-assessment/internal/cache"
	"sr-backend-home-assessment/internal/worker"

	k "sr-backend-home-assessment/internal/kafka" // alias to avoid name conflict

	"github.com/segmentio/kafka-go"
)

var (
	ErrUnorderedEvent = errors.New("out of order event")
	ErrDuplicateEvent = errors.New("duplicate event")
	ErrInvalidEvent   = errors.New("invalid event")
)

type Config struct {
	Brokers         string
	ConsumerGroupID string
	ConsumerTopic   string
	PublisherTopic  string
	Cache           cache.Cache
}

type Cleaner struct {
	worker *worker.Worker
	reader *kafka.Reader
	writer *kafka.Writer
	cache  cache.Cache
}

func New(cfg Config) *Cleaner {
	cleaner := &Cleaner{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers: []string{cfg.Brokers},
			GroupID: cfg.ConsumerGroupID,
			Topic:   cfg.ConsumerTopic,
		}),
		writer: kafka.NewWriter(kafka.WriterConfig{
			Brokers: []string{cfg.Brokers},
			Topic:   cfg.PublisherTopic,
		}),
		cache: cfg.Cache,
	}

	cleaner.worker = worker.New(worker.Config{
		Name:      "cleaner-worker",
		Processor: cleaner,
	})
	return cleaner
}

func (c *Cleaner) Run(ctx context.Context) {
	c.worker.Run(ctx)
}

func (c *Cleaner) Close(ctx context.Context) {
	slog.InfoContext(ctx, "Closing cleaner resources...")
	c.reader.Close()
	c.writer.Close()
}

// Auto-commit active
func (c *Cleaner) ProcessMessage(ctx context.Context) {
	m, err := c.reader.ReadMessage(ctx)
	if err != nil {
		slog.ErrorContext(ctx, "Error reading message", "error", err)
		return
	}
	var payload k.DeviceEvent
	if err := json.Unmarshal(m.Value, &payload); err != nil {
		slog.ErrorContext(ctx, "Error parsing JSON", "error", err)
		return
	}

	if err := c.validateEvent(payload); err != nil {
		slog.InfoContext(ctx, "Invalid event, skipping",
			"error", err,
			"device_id", payload.DeviceID,
			"event_type", payload.EventType,
			"timestamp", payload.Timestamp,
		)
		return
	}

	record := k.StructuredConnectRecord{
		Schema:  k.StructuredSchema,
		Payload: payload,
	}
	out, err := json.Marshal(record)
	if err != nil {
		slog.ErrorContext(ctx, "Error marshalling record", "error", err)
		return
	}
	err = c.writer.WriteMessages(ctx, kafka.Message{Key: []byte(payload.DeviceID), Value: out})
	if err != nil {
		slog.ErrorContext(ctx, "Error writing cleaned message", "error", err)
	}

	// Set cache only after successful write
	c.cache.Set(cache.DeviceID(payload.DeviceID), &cache.DeviceState{
		LastEvent:         payload.EventType,
		LastTimestampSeen: payload.Timestamp,
	})
	slog.InfoContext(ctx, "Published cleaned message", "device_id", payload.DeviceID)
}

func (c *Cleaner) validateEvent(payload k.DeviceEvent) error {
	if payload.EventType != k.DeviceEnter && payload.EventType != k.DeviceExit {
		return ErrInvalidEvent
	}
	state, exists := c.cache.Get(cache.DeviceID(payload.DeviceID))
	if exists {
		if payload.Timestamp < state.LastTimestampSeen {
			return ErrUnorderedEvent
		}
		if payload.EventType == state.LastEvent {
			return ErrDuplicateEvent
		}
	}
	return nil
}

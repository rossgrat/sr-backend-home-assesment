package cleaner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sr-backend-home-assessment/internal/cache"
	"sr-backend-home-assessment/internal/worker"

	k "sr-backend-home-assessment/internal/kafka"

	"github.com/segmentio/kafka-go"
)

var (
	ErrReadMessage    = errors.New("error reading message")
	ErrWriteMessage   = errors.New("error writing message")
	ErrJSONParse      = errors.New("error parsing JSON")
	ErrUnorderedEvent = errors.New("out of order event")
	ErrDuplicateEvent = errors.New("duplicate event")
	ErrInvalidEvent   = errors.New("invalid event")
)

type deviceCache interface {
	Get(string) (cache.DeviceState, bool)
	Set(string, cache.DeviceState)
}

type Config struct {
	Brokers         string
	ConsumerGroupID string
	ConsumerTopic   string
	PublisherTopic  string
	Cache           deviceCache
}

type Cleaner struct {
	worker *worker.Worker
	reader k.Reader
	writer k.Writer
	cache  deviceCache
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
func (c *Cleaner) ProcessMessage(ctx context.Context) error {
	const fn = "Cleaner:ProcessMessage"
	m, err := c.reader.ReadMessage(ctx)
	if err != nil {
		return fmt.Errorf("%s:%w:%w", fn, ErrReadMessage, err)
	}
	var payload k.DeviceEvent
	if err := json.Unmarshal(m.Value, &payload); err != nil {
		return fmt.Errorf("%s:%w:%w", fn, ErrJSONParse, err)
	}

	if err := c.validateEvent(payload); err != nil {
		slog.InfoContext(ctx, "Invalid event, skipping",
			"error", err,
			"device_id", payload.DeviceID,
			"event_type", payload.EventType,
			"timestamp", payload.Timestamp,
		)
		return nil
	}

	record := k.StructuredConnectRecord{
		Schema:  k.StructuredSchema,
		Payload: payload,
	}
	out, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("%s:%w:%w", fn, ErrJSONParse, err)
	}
	err = c.writer.WriteMessages(ctx, kafka.Message{Key: []byte(payload.DeviceID), Value: out})
	if err != nil {
		return fmt.Errorf("%s:%w:%w", fn, ErrWriteMessage, err)
	}

	// Set cache only after successful write
	c.cache.Set(payload.DeviceID, cache.DeviceState{
		LastEvent:         payload.EventType,
		LastTimestampSeen: payload.Timestamp,
	})
	slog.InfoContext(ctx, "Published cleaned message", "device_id", payload.DeviceID)
	return nil
}

func (c *Cleaner) validateEvent(payload k.DeviceEvent) error {
	if payload.EventType != k.DeviceEnter && payload.EventType != k.DeviceExit {
		return ErrInvalidEvent
	}
	state, exists := c.cache.Get(payload.DeviceID)
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

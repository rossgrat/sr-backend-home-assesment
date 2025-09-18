package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	k "sr-backend-home-assessment/internal/kafka"

	"github.com/segmentio/kafka-go"
)

type DeviceID string

type DeviceState struct {
	LastEvent         string
	LastTimestampSeen int64
}

type Config struct {
	Brokers       string
	ConsumerTopic string
}

type Cache interface {
	Get(deviceID DeviceID) (*DeviceState, bool)
	Set(deviceID DeviceID, state *DeviceState)
	Delete(deviceID DeviceID)
	Hydrate(ctx context.Context)
	Dump()
}

type StateCache struct {
	brokers string
	store   map[DeviceID]*DeviceState
	reader  *kafka.Reader
}

func New(cfg Config) *StateCache {
	cache := &StateCache{
		store: make(map[DeviceID]*DeviceState),
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:     []string{cfg.Brokers},
			Topic:       cfg.ConsumerTopic,
			StartOffset: kafka.FirstOffset,
			// No consumer group for one-time read
		}),
		brokers: cfg.Brokers,
	}

	return cache
}

func (c *StateCache) Get(deviceID DeviceID) (*DeviceState, bool) {
	state, exists := c.store[deviceID]
	return state, exists
}

func (c *StateCache) Set(deviceID DeviceID, state *DeviceState) {
	c.store[deviceID] = state
}

func (c *StateCache) Delete(deviceID DeviceID) {
	delete(c.store, deviceID)
}

func (c *StateCache) Dump() {
	for deviceID, state := range c.store {
		slog.Info("Cache Dump", "deviceID", deviceID, "state", state)
	}
}

func (c *StateCache) waitForBroker(ctx context.Context, maxWait time.Duration, interval time.Duration) error {
	deadline := time.Now().Add(maxWait)
	for time.Now().Before(deadline) {
		dialCtx, cancel := context.WithTimeout(ctx, interval)
		conn, err := kafka.DialContext(dialCtx, "tcp", c.brokers)
		cancel()
		if err == nil {
			conn.Close()
			slog.InfoContext(ctx, "Broker is ready", "broker", c.brokers)
			return nil
		}
		slog.InfoContext(ctx, "Broker not ready", "broker", c.brokers, "error", err)
		time.Sleep(interval)
	}
	return fmt.Errorf("broker not reachable after %s", maxWait)
}

// Blocking operation
func (c *StateCache) Hydrate(ctx context.Context) {
	defer c.reader.Close()

	slog.InfoContext(ctx, "Pinging broker to ensure connectivity...")
	if err := c.waitForBroker(ctx, time.Second*30, time.Second*5); err != nil {
		slog.ErrorContext(ctx, "Broker failed to respond", "error", err)
		return
	}

	slog.InfoContext(ctx, "Starting cache hydration...")

	for {
		select {
		case <-ctx.Done():
			slog.InfoContext(ctx, "Cache hydrate stopped...")
			return
		default:
			readMessageTimeoutCtx, cancel := context.WithTimeout(ctx, time.Second*5)
			defer cancel()
			m, err := c.reader.ReadMessage(readMessageTimeoutCtx)
			if err != nil {
				if errors.Is(err, context.DeadlineExceeded) {
					slog.InfoContext(ctx, "Cache hydration complete - Deadline exceeded")
					return // No more messages to read
				}
				slog.ErrorContext(ctx, "Error reading message", "error", err)
				return
			}

			var record k.StructuredConnectRecord
			if err := json.Unmarshal(m.Value, &record); err != nil {
				slog.ErrorContext(ctx, "Error parsing JSON", "error", err)
				continue
			}

			deviceState := &DeviceState{
				LastEvent:         record.Payload.EventType,
				LastTimestampSeen: record.Payload.Timestamp,
			}
			c.Set(DeviceID(record.Payload.DeviceID), deviceState)

			lag := c.reader.Lag()
			if lag == 0 {
				slog.InfoContext(ctx, "Cache hydration complete - Lag is zero")
				return // No more messages to read
			}

		}
	}
}

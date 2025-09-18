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

var (
	ErrBrokerUnreachable = errors.New("broker unreachable")
	ErrReadMessage       = errors.New("error reading message")
	ErrParseMessage      = errors.New("error parsing JSON")
)

type DeviceState struct {
	LastEvent         string
	LastTimestampSeen int64
}

type Config struct {
	Brokers       string
	ConsumerTopic string
}

type StateCache struct {
	brokers string
	store   map[string]DeviceState
	reader  k.Reader
}

func New(cfg Config) *StateCache {
	cache := &StateCache{
		store: make(map[string]DeviceState),
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

func (c *StateCache) Get(deviceID string) (DeviceState, bool) {
	state, exists := c.store[deviceID]
	return state, exists
}

func (c *StateCache) Set(deviceID string, state DeviceState) {
	c.store[deviceID] = state
}

func (c *StateCache) Delete(deviceID string) {
	delete(c.store, deviceID)
}

func (c *StateCache) Dump() {
	for deviceID, state := range c.store {
		slog.Info("Cache Dump", "deviceID", deviceID, "state", state)
	}
}

func (c *StateCache) waitForBroker(ctx context.Context, maxWait time.Duration, interval time.Duration) error {
	const fn = "StateCache:waitForBroker"
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
	return fmt.Errorf("%s:%w", fn, ErrBrokerUnreachable)
}

// Blocking operation
func (c *StateCache) Hydrate(ctx context.Context) error {
	const fn = "StateCache:Hydrate"
	defer c.reader.Close()

	slog.InfoContext(ctx, "Pinging broker to ensure connectivity...")
	if err := c.waitForBroker(ctx, time.Second*30, time.Second*5); err != nil {
		return fmt.Errorf("%s:%w", fn, err)
	}

	slog.InfoContext(ctx, "Starting cache hydration...")

	for {
		select {
		case <-ctx.Done():
			slog.InfoContext(ctx, "Cache hydrate stopped...")
			return nil
		default:

			done, err := c.ReadMessage(ctx)
			if err != nil {
				return fmt.Errorf("%s:%w", fn, err)
			}
			if done {
				return nil
			}
		}
	}
}

func (c *StateCache) ReadMessage(ctx context.Context) (bool, error) {
	const fn = "StateCache:ReadMessage"
	readMessageTimeoutCtx, cancel := context.WithTimeout(ctx, time.Second*5)
	m, err := c.reader.ReadMessage(readMessageTimeoutCtx)
	if err != nil {
		cancel()
		if errors.Is(err, context.DeadlineExceeded) {
			slog.InfoContext(ctx, "Cache hydration complete - No messages in cache")

			return true, nil
		}
		return false, fmt.Errorf("%s:%w:%w", fn, ErrReadMessage, err)
	}
	cancel()

	var record k.StructuredConnectRecord
	if err := json.Unmarshal(m.Value, &record); err != nil {
		return false, fmt.Errorf("%s:%w:%w", fn, ErrParseMessage, err)
	}

	deviceState := DeviceState{
		LastEvent:         record.Payload.EventType,
		LastTimestampSeen: record.Payload.Timestamp,
	}
	c.Set(record.Payload.DeviceID, deviceState)

	lag := c.reader.Lag()
	if lag == 0 {
		slog.InfoContext(ctx, "Cache hydration complete - Lag is zero")
		return true, nil
	}
	return false, nil
}

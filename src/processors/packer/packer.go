package packer

import (
	"context"
	"log/slog"
	"sr-backend-home-assessment/src/worker"

	"github.com/segmentio/kafka-go"
)

type Config struct {
	Brokers         string
	ConsumerGroupID string
	ConsumerTopic   string
	PublisherTopic  string
}

type Packer struct {
	worker *worker.Worker
	reader *kafka.Reader
	writer *kafka.Writer
}

func New(cfg *Config) *Packer {
	packer := &Packer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers: []string{cfg.Brokers},
			GroupID: cfg.ConsumerGroupID,
			Topic:   cfg.ConsumerTopic,
		}),
		writer: kafka.NewWriter(kafka.WriterConfig{
			Brokers: []string{cfg.Brokers},
			Topic:   cfg.PublisherTopic,
		}),
	}

	packer.worker = worker.New(worker.Config{
		Name:      "packer-worker",
		Processor: packer,
	})
	return packer
}

func (p *Packer) Run(ctx context.Context) {
	p.worker.Run(ctx)
}

func (p *Packer) Close(ctx context.Context) {
	slog.InfoContext(ctx, "Closing packer resources...")
	p.reader.Close()
	p.writer.Close()
}

// Auto-commit active
func (p *Packer) ProcessMessage(ctx context.Context) {
	m, err := p.reader.ReadMessage(ctx)
	if err != nil {
		slog.ErrorContext(ctx, "Error reading message", "error", err)
		return
	}
	err = p.writer.WriteMessages(ctx, kafka.Message{Key: m.Key, Value: m.Value})
	if err != nil {
		slog.ErrorContext(ctx, "Error writing message", "error", err)
	}
	slog.InfoContext(ctx, "Published packed message", "device_id", string(m.Key))
}

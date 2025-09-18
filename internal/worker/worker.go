package worker

import (
	"context"
	"log/slog"
)

type Config struct {
	Name      string
	Processor Processor
}

type Processor interface {
	ProcessMessage(ctx context.Context) error
}

type Worker struct {
	name      string
	processor Processor
}

func New(cfg Config) *Worker {
	return &Worker{
		name:      cfg.Name,
		processor: cfg.Processor,
	}
}

func (w *Worker) Run(ctx context.Context) {
	slog.InfoContext(ctx, "Worker started...", "worker", w.name)
	for {
		select {
		case <-ctx.Done():
			slog.InfoContext(ctx, "Worker stopped...", "worker", w.name)
			return
		default:
			err := w.processor.ProcessMessage(ctx)
			if err != nil {
				slog.ErrorContext(ctx, "Error processing message", "worker", w.name, "error", err)
			}
		}
	}
}

package db

import (
	"context"
	"errors"
	"fmt"

	"github.com/georgysavva/scany/pgxscan"
	"github.com/jackc/pgx/v4"
)

var (
	ErrInsertFailed           = errors.New("insert operation failed")
	ErrTransactionStartFailed = errors.New("transaction start failed")
	ErrSelectFailed           = errors.New("select operation failed")
)

func (db *DB) CreateTimeline(ctx context.Context, events []DeviceEvent) error {
	const fn = "DB:CreateTimeline"
	tx, err := db.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("%s:%w:%w", fn, ErrTransactionStartFailed, err)
	}
	defer func() {
		if err != nil {
			tx.Rollback(ctx)
		} else {
			tx.Commit(ctx)
		}
	}()

	for _, event := range events {
		_, err = tx.Exec(ctx, `
			INSERT INTO device_events_cleaned (
				device_id, 
				event_type, 
				timestamp
			) VALUES ($1, $2, $3)
		`, event.DeviceID, event.EventType, event.Timestamp)
		if err != nil {
			return fmt.Errorf("%s:%w:%w", fn, ErrInsertFailed, err)
		}
	}
	return nil
}

func (db *DB) LoadEventsBetween(ctx context.Context, deviceID string, start, end int64) ([]DeviceEvent, error) {
	const fn = "DB:LoadEventsBetween"
	var events []DeviceEvent
	err := pgxscan.Select(ctx, db.pool, &events, `
			SELECT 
				device_id, 
				event_type, 
				timestamp
			FROM device_events_cleaned
			WHERE device_id = $1 
			AND timestamp >= $2 
			AND timestamp <= $3
			ORDER BY timestamp ASC
		`, deviceID, start, end)
	if err != nil {
		if err == pgx.ErrNoRows {
			return []DeviceEvent{}, nil
		}
		return nil, fmt.Errorf("%s:%w:%w", fn, ErrSelectFailed, err)
	}
	return events, nil
}

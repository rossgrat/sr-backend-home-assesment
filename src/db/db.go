package db

import (
	"context"

	"github.com/georgysavva/scany/pgxscan"
	"github.com/jackc/pgx/v4/pgxpool"
)

type DeviceEvent struct {
	DeviceID  string `json:"device_id"`
	EventType string `json:"event_type"`
	Timestamp int    `json:"timestamp"`
}

type DB struct {
	pool *pgxpool.Pool
}

func Init(ctx context.Context, connString string) (*DB, error) {
	pool, err := pgxpool.Connect(ctx, connString)
	if err != nil {
		return nil, err
	}
	return &DB{pool: pool}, nil
}

func (db *DB) Close() {
	db.pool.Close()
}

func (db *DB) LoadLatestEventForDevice(ctx context.Context, deviceID string) (*DeviceEvent, error) {
	var e DeviceEvent
	err := pgxscan.Get(ctx, db.pool, &e, `
		SELECT 
			device_id, 
			event_type, 
			timestamp 
		FROM "device_events_cleaned"
		WHERE device_id = $1 
		ORDER BY timestamp DESC LIMIT 1`, deviceID)
	if err != nil {
		return nil, err
	}
	return &e, nil
}

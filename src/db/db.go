package db

import (
	"context"
	"log/slog"

	"github.com/georgysavva/scany/pgxscan"
	"github.com/jackc/pgx/v4/pgxpool"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

type DeviceEvent struct {
	DeviceID  string `json:"device_id"`
	EventType string `json:"event_type"`
	Timestamp int    `json:"timestamp"`
}

type Config struct {
	ConnString     string
	MigrationsPath string
}

type DB struct {
	connString     string
	migrationsPath string
	pool           *pgxpool.Pool
}

func (db *DB) Migrate(ctx context.Context) error {
	slog.InfoContext(ctx, "Running database migrations...", "path", db.migrationsPath)
	m, err := migrate.New(
		"file://"+db.migrationsPath,
		db.connString,
	)
	if err != nil {
		return err
	}
	err = m.Up()
	if err != nil && err != migrate.ErrNoChange {
		return err
	}
	return nil
}

func Init(ctx context.Context, cfg *Config) (*DB, error) {
	pool, err := pgxpool.Connect(ctx, cfg.ConnString)
	if err != nil {
		return nil, err
	}

	db := &DB{
		pool:           pool,
		connString:     cfg.ConnString,
		migrationsPath: cfg.MigrationsPath,
	}
	if err := db.Migrate(ctx); err != nil {
		return nil, err
	}
	return db, nil
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

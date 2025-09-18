package db

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

var (
	ErrMigrateFailed     = errors.New("database migration failed")
	ErrPoolConnectFailed = errors.New("database pool connection failed")
)

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
	const fn = "DB:Migrate"
	slog.InfoContext(ctx, "Running database migrations...", "path", db.migrationsPath)
	m, err := migrate.New(
		"file://"+db.migrationsPath,
		db.connString,
	)
	if err != nil {
		return fmt.Errorf("%s:%w:%w", fn, ErrMigrateFailed, err)
	}
	err = m.Up()
	if err != nil && err != migrate.ErrNoChange {
		return fmt.Errorf("%s:%w:%w", fn, ErrMigrateFailed, err)
	}
	return nil
}

func Init(ctx context.Context, cfg Config) (*DB, error) {
	const fn = "Init"
	var pool *pgxpool.Pool
	var err error
	maxWait := 15 * time.Second // total time to retry
	interval := 1 * time.Second // wait between retries
	deadline := time.Now().Add(maxWait)
	for {
		pool, err = pgxpool.Connect(ctx, cfg.ConnString)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("%s:%w:%w", fn, ErrPoolConnectFailed, err)
		}
		slog.InfoContext(ctx, "DB connect failed, retrying...", "err", err)
		time.Sleep(interval)
	}

	db := &DB{
		pool:           pool,
		connString:     cfg.ConnString,
		migrationsPath: cfg.MigrationsPath,
	}
	if err := db.Migrate(ctx); err != nil {
		return nil, fmt.Errorf("%s:%w", fn, err)
	}
	return db, nil
}

func (db *DB) Close() {
	db.pool.Close()
}

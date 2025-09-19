package db

import (
	"context"
	"testing"

	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

var DBPool *DB

// Setup the testcontainer DB before running an dbOps tests
func TestMain(m *testing.M) {
	ctx := context.Background()

	pgContainer, err := postgres.Run(ctx,
		"timescale/timescaledb:latest-pg17",
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("testuser"),
		postgres.WithPassword("testpass"),
	)
	if err != nil {
		panic(err)
	}
	defer pgContainer.Terminate(ctx)

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		panic(err)
	}
	migrationsPath := "./migrations"

	DBPool, err = Init(ctx, Config{
		ConnString:     connStr,
		MigrationsPath: migrationsPath,
	})
	if err != nil {
		panic(err)
	}

	m.Run()

	pgContainer.Terminate(ctx)
	DBPool.Close()
}

func TestOps(t *testing.T) {
	ctx := context.Background()
	now := int64(1000000)
	events := []DeviceEvent{
		{DeviceID: "dev1", EventType: "on", Timestamp: now},
		{DeviceID: "dev1", EventType: "off", Timestamp: now + 1},
	}

	err := DBPool.CreateTimeline(ctx, events)
	if err != nil {
		t.Fatalf("CreateTimeline failed: %v", err)
	}

	got, err := DBPool.LoadEventsBetween(ctx, "dev1", now, now+1)
	if err != nil {
		t.Fatalf("LoadEventsBetween failed: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 events, got %d", len(got))
	}
	if got[0].EventType != "on" || got[1].EventType != "off" {
		t.Fatalf("unexpected event types: %+v", got)
	}
}

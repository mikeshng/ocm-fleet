package sqlite

import (
	"context"
	"testing"

	"github.com/pressly/goose/v3"
)

// TestMigrationsUpgradeAndDown runs the embedded migrations against a
// database that already holds data. It stops at the version that predates
// the delivery failure details and the completion outbox, records a
// delivery under that schema, upgrades to the latest version, and then
// rolls the chain back to where it started.
func TestMigrationsUpgradeAndDown(t *testing.T) {
	const (
		previousVersion = 1
		deliveryTime    = "2026-03-04T09:30:00Z"
	)

	db, err := openWithPragmas(sharedMemoryDSN("migrations_upgrade_and_down"))
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	// One connection for the sentinel that keeps the in-memory database
	// alive and one for the migrations and queries.
	db.SetMaxOpenConns(2)
	sentinel, err := db.Conn(context.Background())
	if err != nil {
		db.Close()
		t.Fatalf("open sentinel connection: %v", err)
	}
	t.Cleanup(func() {
		sentinel.Close()
		db.Close()
	})

	goose.SetBaseFS(migrations)
	if err := goose.SetDialect("sqlite3"); err != nil {
		t.Fatalf("set goose dialect: %v", err)
	}
	if err := goose.UpTo(db, "migrations", previousVersion); err != nil {
		t.Fatalf("migrate to version %d: %v", previousVersion, err)
	}

	if _, err := db.Exec(
		`INSERT INTO delivery_records
		     (fulfillment_id, target_id, id, manifests, state, generation, operation, created_at, updated_at)
		 VALUES ('f1', 't1', 'f1:t1', '[]', 'delivered', 3, 'deliver', ?, ?)`,
		deliveryTime, deliveryTime,
	); err != nil {
		t.Fatalf("insert delivery at version %d: %v", previousVersion, err)
	}

	if err := goose.Up(db, "migrations"); err != nil {
		t.Fatalf("migrate to the latest version: %v", err)
	}

	var (
		message  string
		attempts int64
	)
	if err := db.QueryRow(
		`SELECT message, attempts FROM delivery_records
		  WHERE fulfillment_id = 'f1' AND target_id = 't1'`,
	).Scan(&message, &attempts); err != nil {
		t.Fatalf("read the delivery after the upgrade: %v", err)
	}
	if message != "" || attempts != 0 {
		t.Fatalf("delivery after the upgrade = (message %q, attempts %d), want (\"\", 0)", message, attempts)
	}

	var createdAtType string
	if err := db.QueryRow(
		`SELECT type FROM pragma_table_info('delivery_completion_outbox') WHERE name = 'created_at'`,
	).Scan(&createdAtType); err != nil {
		t.Fatalf("look up the outbox created_at type: %v", err)
	}
	if createdAtType != "TEXT" {
		t.Fatalf("outbox created_at type = %q, want %q", createdAtType, "TEXT")
	}

	if _, err := db.Exec(
		`INSERT INTO delivery_completion_outbox
		     (delivery_id, fulfillment_id, generation, operation, state, message, created_at)
		 VALUES ('f1:t1', 'f1', 3, 'deliver', 'failed', 'apply object 1: refused', ?)`,
		deliveryTime,
	); err != nil {
		t.Fatalf("insert outbox row: %v", err)
	}
	var createdAt string
	if err := db.QueryRow(
		`SELECT created_at FROM delivery_completion_outbox WHERE delivery_id = 'f1:t1'`,
	).Scan(&createdAt); err != nil {
		t.Fatalf("read the outbox created_at: %v", err)
	}
	if createdAt != deliveryTime {
		t.Fatalf("outbox created_at = %q, want %q", createdAt, deliveryTime)
	}

	if err := goose.DownTo(db, "migrations", previousVersion); err != nil {
		t.Fatalf("roll back to version %d: %v", previousVersion, err)
	}

	var outboxTables int
	if err := db.QueryRow(
		`SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 'delivery_completion_outbox'`,
	).Scan(&outboxTables); err != nil {
		t.Fatalf("look up the outbox table after the rollback: %v", err)
	}
	if outboxTables != 0 {
		t.Fatalf("the rollback left the delivery_completion_outbox table behind")
	}

	var failureColumns int
	if err := db.QueryRow(
		`SELECT count(*) FROM pragma_table_info('delivery_records') WHERE name IN ('message', 'attempts')`,
	).Scan(&failureColumns); err != nil {
		t.Fatalf("look up the delivery failure columns after the rollback: %v", err)
	}
	if failureColumns != 0 {
		t.Fatalf("the rollback left %d delivery failure columns behind, want 0", failureColumns)
	}

	var deliveries int
	if err := db.QueryRow(
		`SELECT count(*) FROM delivery_records
		  WHERE fulfillment_id = 'f1' AND target_id = 't1'`,
	).Scan(&deliveries); err != nil {
		t.Fatalf("read the delivery after the rollback: %v", err)
	}
	if deliveries != 1 {
		t.Fatalf("the rollback changed the recorded deliveries: got %d, want 1", deliveries)
	}
}

package postgres

import (
	"database/sql"
	"strings"
	"testing"

	"github.com/pressly/goose/v3"
)

// TestMigrationsUpgradeAndDown runs the embedded migrations against a
// database that already holds data. It stops at the version that predates
// the delivery failure details and the completion outbox, records a
// delivery under that schema, upgrades to the latest version, and then
// rolls the chain back to where it started. The versions in between also
// add the deployment name index, which the test checks on the way up and
// on the way down.
func TestMigrationsUpgradeAndDown(t *testing.T) {
	const (
		previousVersion = 2
		deliveryTime    = "2026-03-04T09:30:00Z"
	)

	db, err := sql.Open("pgx", createTestDB(t))
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	goose.SetBaseFS(migrations)
	if err := goose.SetDialect("postgres"); err != nil {
		t.Fatalf("set goose dialect: %v", err)
	}
	if err := goose.UpTo(db, "migrations", previousVersion); err != nil {
		t.Fatalf("migrate to version %d: %v", previousVersion, err)
	}

	if _, err := db.Exec(
		`INSERT INTO delivery_records
		     (fulfillment_id, target_id, id, manifests, state, generation, operation, created_at, updated_at)
		 VALUES ('f1', 't1', 'f1:t1', '[]', 'delivered', 3, 'deliver', $1, $1)`,
		deliveryTime,
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
		`SELECT data_type FROM information_schema.columns
		  WHERE table_schema = current_schema()
		    AND table_name = 'delivery_completion_outbox'
		    AND column_name = 'created_at'`,
	).Scan(&createdAtType); err != nil {
		t.Fatalf("look up the outbox created_at type: %v", err)
	}
	if createdAtType != "text" {
		t.Fatalf("outbox created_at type = %q, want %q", createdAtType, "text")
	}

	if _, err := db.Exec(
		`INSERT INTO delivery_completion_outbox
		     (delivery_id, fulfillment_id, generation, operation, state, message, created_at)
		 VALUES ('f1:t1', 'f1', 3, 'deliver', 'failed', 'apply object 1: refused', $1)`,
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

	// The deployment paging query compares and orders names byte-wise.
	// The index has to carry that comparison; PostgreSQL prints the
	// collation in the index definition.
	var indexDef string
	if err := db.QueryRow(
		`SELECT indexdef FROM pg_indexes
		  WHERE schemaname = current_schema() AND indexname = 'idx_deployments_name_c'`,
	).Scan(&indexDef); err != nil {
		t.Fatalf("look up the deployment name index after the upgrade: %v", err)
	}
	if !strings.Contains(indexDef, `COLLATE "C"`) {
		t.Fatalf("deployment name index = %q, want a COLLATE \"C\" index", indexDef)
	}

	if err := goose.DownTo(db, "migrations", previousVersion); err != nil {
		t.Fatalf("roll back to version %d: %v", previousVersion, err)
	}

	var nameIndexes int
	if err := db.QueryRow(
		`SELECT count(*) FROM pg_indexes
		  WHERE schemaname = current_schema() AND indexname = 'idx_deployments_name_c'`,
	).Scan(&nameIndexes); err != nil {
		t.Fatalf("look up the deployment name index after the rollback: %v", err)
	}
	if nameIndexes != 0 {
		t.Fatalf("the rollback left the deployment name index behind")
	}

	var outboxTables int
	if err := db.QueryRow(
		`SELECT count(*) FROM information_schema.tables
		  WHERE table_schema = current_schema() AND table_name = 'delivery_completion_outbox'`,
	).Scan(&outboxTables); err != nil {
		t.Fatalf("look up the outbox table after the rollback: %v", err)
	}
	if outboxTables != 0 {
		t.Fatalf("the rollback left the delivery_completion_outbox table behind")
	}

	var failureColumns int
	if err := db.QueryRow(
		`SELECT count(*) FROM information_schema.columns
		  WHERE table_schema = current_schema() AND table_name = 'delivery_records'
		    AND column_name IN ('message', 'attempts')`,
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

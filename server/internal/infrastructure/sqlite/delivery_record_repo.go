package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/stolostron/ocm-fleet/server/internal/domain"
)

// DeliveryRepo implements [domain.DeliveryRepository] backed by SQLite.
type DeliveryRepo struct {
	DB *sql.Tx
}

func (r *DeliveryRepo) Put(ctx context.Context, d domain.Delivery) error {
	s := (&d).Snapshot()
	manifests, err := json.Marshal(s.Manifests)
	if err != nil {
		return fmt.Errorf("marshal manifests: %w", err)
	}

	_, err = r.DB.ExecContext(ctx,
		`INSERT INTO delivery_records (id, fulfillment_id, target_id, manifests, generation, state, operation, message, attempts, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT (fulfillment_id, target_id) DO UPDATE SET
		   id = excluded.id,
		   manifests = excluded.manifests,
		   generation = excluded.generation,
		   state = excluded.state,
		   operation = excluded.operation,
		   message = excluded.message,
		   attempts = excluded.attempts,
		   updated_at = excluded.updated_at`,
		string(s.ID), string(s.FulfillmentID), string(s.TargetID),
		string(manifests), int64(s.Generation), string(s.State), string(s.Operation),
		s.Message, int64(s.Attempts),
		s.CreatedAt.UTC().Format(time.RFC3339),
		s.UpdatedAt.UTC().Format(time.RFC3339),
	)
	if err != nil {
		return fmt.Errorf("upsert delivery: %w", err)
	}
	return nil
}

func (r *DeliveryRepo) Get(ctx context.Context, id domain.DeliveryID) (domain.Delivery, error) {
	row := r.DB.QueryRowContext(ctx,
		`SELECT id, fulfillment_id, target_id, manifests, generation, state, operation, message, attempts, created_at, updated_at
		 FROM delivery_records WHERE id = ?`,
		string(id),
	)
	s, err := scanDeliverySnapshot(row)
	if err != nil {
		return domain.Delivery{}, err
	}
	return domain.DeliveryFromSnapshot(s), nil
}

// GetForUpdate is Get: write transactions here are BEGIN IMMEDIATE, so
// only one of them runs at a time and the row another transaction read
// cannot change before this one commits or rolls back.
func (r *DeliveryRepo) GetForUpdate(ctx context.Context, id domain.DeliveryID) (domain.Delivery, error) {
	return r.Get(ctx, id)
}

func (r *DeliveryRepo) GetByFulfillmentTarget(ctx context.Context, fID domain.FulfillmentID, tgtID domain.TargetID) (domain.Delivery, error) {
	row := r.DB.QueryRowContext(ctx,
		`SELECT id, fulfillment_id, target_id, manifests, generation, state, operation, message, attempts, created_at, updated_at
		 FROM delivery_records WHERE fulfillment_id = ? AND target_id = ?`,
		string(fID), string(tgtID),
	)
	s, err := scanDeliverySnapshot(row)
	if err != nil {
		return domain.Delivery{}, err
	}
	return domain.DeliveryFromSnapshot(s), nil
}

func (r *DeliveryRepo) ListByFulfillment(ctx context.Context, fID domain.FulfillmentID) ([]domain.Delivery, error) {
	rows, err := r.DB.QueryContext(ctx,
		`SELECT id, fulfillment_id, target_id, manifests, generation, state, operation, message, attempts, created_at, updated_at
		 FROM delivery_records WHERE fulfillment_id = ?
		 ORDER BY target_id`,
		string(fID),
	)
	if err != nil {
		return nil, fmt.Errorf("list deliveries: %w", err)
	}
	defer rows.Close()

	var deliveries []domain.Delivery
	for rows.Next() {
		s, err := scanDeliverySnapshot(rows)
		if err != nil {
			return nil, err
		}
		deliveries = append(deliveries, domain.DeliveryFromSnapshot(s))
	}
	return deliveries, rows.Err()
}

func (r *DeliveryRepo) ListActive(ctx context.Context, targetIDs []domain.TargetID) ([]domain.Delivery, error) {
	args := []any{
		string(domain.DeliveryStatePending),
		string(domain.DeliveryStateAccepted),
		string(domain.DeliveryStateProgressing),
	}

	q := `SELECT id, fulfillment_id, target_id, manifests, generation, state, operation, message, attempts, created_at, updated_at
	      FROM delivery_records WHERE state IN (?, ?, ?)`
	if len(targetIDs) > 0 {
		q += ` AND target_id IN (`
		for i, tid := range targetIDs {
			if i > 0 {
				q += `, `
			}
			q += `?`
			args = append(args, string(tid))
		}
		q += `)`
	}

	rows, err := r.DB.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("list active deliveries: %w", err)
	}
	defer rows.Close()

	var deliveries []domain.Delivery
	for rows.Next() {
		s, err := scanDeliverySnapshot(rows)
		if err != nil {
			return nil, err
		}
		deliveries = append(deliveries, domain.DeliveryFromSnapshot(s))
	}
	return deliveries, rows.Err()
}

func (r *DeliveryRepo) DeleteByFulfillment(ctx context.Context, fID domain.FulfillmentID) error {
	_, err := r.DB.ExecContext(ctx,
		`DELETE FROM delivery_records WHERE fulfillment_id = ?`,
		string(fID),
	)
	if err != nil {
		return fmt.Errorf("delete deliveries: %w", err)
	}
	return nil
}

// listDeliveriesByFulfillments loads the delivery records of the given
// fulfillments in a single query, grouped by fulfillment and ordered by
// target ID. It backs the deliveries of [domain.DeploymentView] without
// issuing one query per deployment.
func listDeliveriesByFulfillments(ctx context.Context, tx *sql.Tx, fIDs []domain.FulfillmentID) (map[domain.FulfillmentID][]domain.Delivery, error) {
	if len(fIDs) == 0 {
		return nil, nil
	}
	args := make([]any, len(fIDs))
	placeholders := make([]string, len(fIDs))
	for i, fID := range fIDs {
		args[i] = string(fID)
		placeholders[i] = "?"
	}
	rows, err := tx.QueryContext(ctx,
		`SELECT id, fulfillment_id, target_id, manifests, generation, state, operation, message, attempts, created_at, updated_at
		 FROM delivery_records WHERE fulfillment_id IN (`+strings.Join(placeholders, ", ")+`)
		 ORDER BY target_id`,
		args...,
	)
	if err != nil {
		return nil, fmt.Errorf("list deliveries by fulfillment: %w", err)
	}
	defer rows.Close()

	grouped := make(map[domain.FulfillmentID][]domain.Delivery, len(fIDs))
	for rows.Next() {
		s, err := scanDeliverySnapshot(rows)
		if err != nil {
			return nil, err
		}
		grouped[s.FulfillmentID] = append(grouped[s.FulfillmentID], domain.DeliveryFromSnapshot(s))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list deliveries by fulfillment: %w", err)
	}
	return grouped, nil
}

func scanDeliverySnapshot(s scanner) (domain.DeliverySnapshot, error) {
	var snap domain.DeliverySnapshot
	var id, fID, tgtID, manifestsJSON, stateStr, operationStr, message, createdAtStr, updatedAtStr string
	var generation, attempts int64
	if err := s.Scan(&id, &fID, &tgtID, &manifestsJSON, &generation, &stateStr, &operationStr, &message, &attempts, &createdAtStr, &updatedAtStr); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return snap, fmt.Errorf("%w", domain.ErrNotFound)
		}
		return snap, fmt.Errorf("scan delivery: %w", err)
	}
	snap.ID = domain.DeliveryID(id)
	snap.FulfillmentID = domain.FulfillmentID(fID)
	snap.TargetID = domain.TargetID(tgtID)
	snap.Generation = domain.Generation(generation)
	snap.State = domain.DeliveryState(stateStr)
	snap.Operation = domain.DeliveryOperation(operationStr)
	snap.Message = message
	snap.Attempts = int(attempts)
	if err := json.Unmarshal([]byte(manifestsJSON), &snap.Manifests); err != nil {
		return snap, fmt.Errorf("unmarshal manifests: %w", err)
	}
	t, err := time.Parse(time.RFC3339, createdAtStr)
	if err != nil {
		return snap, fmt.Errorf("parse created_at: %w", err)
	}
	snap.CreatedAt = t
	t, err = time.Parse(time.RFC3339, updatedAtStr)
	if err != nil {
		return snap, fmt.Errorf("parse updated_at: %w", err)
	}
	snap.UpdatedAt = t
	return snap, nil
}

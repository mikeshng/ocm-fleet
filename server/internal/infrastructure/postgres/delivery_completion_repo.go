package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/stolostron/ocm-fleet/server/internal/domain"
)

// DeliveryCompletionRepo implements
// [domain.DeliveryCompletionRepository] backed by Postgres.
type DeliveryCompletionRepo struct {
	DB *sql.Tx
}

func (r *DeliveryCompletionRepo) Put(ctx context.Context, c domain.DeliveryCompletion) error {
	_, err := r.DB.ExecContext(ctx,
		`INSERT INTO delivery_completion_outbox (delivery_id, fulfillment_id, generation, operation, state, message, created_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)
		 ON CONFLICT (delivery_id) DO UPDATE SET
		   fulfillment_id = excluded.fulfillment_id,
		   generation = excluded.generation,
		   operation = excluded.operation,
		   state = excluded.state,
		   message = excluded.message,
		   created_at = excluded.created_at`,
		string(c.DeliveryID), string(c.FulfillmentID), int64(c.Generation),
		string(c.Operation), string(c.State), c.Message,
		c.CreatedAt.UTC().Format(time.RFC3339),
	)
	if err != nil {
		return fmt.Errorf("put delivery completion: %w", err)
	}
	return nil
}

func (r *DeliveryCompletionRepo) ListOutstanding(ctx context.Context) ([]domain.DeliveryCompletion, error) {
	rows, err := r.DB.QueryContext(ctx,
		`SELECT delivery_id, fulfillment_id, generation, operation, state, message, created_at
		 FROM delivery_completion_outbox
		 ORDER BY created_at, delivery_id`,
	)
	if err != nil {
		return nil, fmt.Errorf("list delivery completions: %w", err)
	}
	return collectRows(rows, scanDeliveryCompletion)
}

func (r *DeliveryCompletionRepo) Delete(ctx context.Context, deliveryID domain.DeliveryID, generation domain.Generation, operation domain.DeliveryOperation) error {
	_, err := r.DB.ExecContext(ctx,
		`DELETE FROM delivery_completion_outbox
		 WHERE delivery_id = $1 AND generation = $2 AND operation = $3`,
		string(deliveryID), int64(generation), string(operation),
	)
	if err != nil {
		return fmt.Errorf("delete delivery completion: %w", err)
	}
	return nil
}

func scanDeliveryCompletion(s scanner) (domain.DeliveryCompletion, error) {
	var c domain.DeliveryCompletion
	var deliveryID, fID, operation, state, message, createdAt string
	var generation int64
	if err := s.Scan(&deliveryID, &fID, &generation, &operation, &state, &message, &createdAt); err != nil {
		return c, fmt.Errorf("scan delivery completion: %w", err)
	}
	created, err := time.Parse(time.RFC3339, createdAt)
	if err != nil {
		return c, fmt.Errorf("parse delivery completion created_at: %w", err)
	}
	c.DeliveryID = domain.DeliveryID(deliveryID)
	c.FulfillmentID = domain.FulfillmentID(fID)
	c.Generation = domain.Generation(generation)
	c.Operation = domain.DeliveryOperation(operation)
	c.State = domain.DeliveryState(state)
	c.Message = message
	c.CreatedAt = created
	return c, nil
}

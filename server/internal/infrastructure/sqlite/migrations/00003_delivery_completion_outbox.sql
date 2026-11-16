-- +goose Up
-- A delivery completion that still has to be signaled to its
-- fulfillment workflow. The row is written in the same transaction as
-- the delivery's terminal state; a relay signals it and deletes it.
--
-- created_at holds the RFC3339 string the repository writes, like the
-- other application-managed timestamps in this schema.
CREATE TABLE delivery_completion_outbox (
    delivery_id TEXT NOT NULL PRIMARY KEY,
    fulfillment_id TEXT NOT NULL,
    generation INTEGER NOT NULL,
    operation TEXT NOT NULL,
    state TEXT NOT NULL,
    message TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL
);

-- +goose Down
DROP TABLE delivery_completion_outbox;

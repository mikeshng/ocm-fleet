-- +goose Up
-- The outcome of a failed delivery attempt: the message the addon
-- reported and how many attempts have failed in the current generation
-- and operation cycle. The API reads both to say why a delivery failed
-- and the orchestration uses the count to back off between retries.
ALTER TABLE delivery_records ADD COLUMN message TEXT NOT NULL DEFAULT '';
ALTER TABLE delivery_records ADD COLUMN attempts BIGINT NOT NULL DEFAULT 0;

-- +goose Down
ALTER TABLE delivery_records DROP COLUMN attempts;
ALTER TABLE delivery_records DROP COLUMN message;

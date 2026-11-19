-- +goose Up
-- The deployment list walks names in byte order, page by page:
-- WHERE name COLLATE "C" > <after> ORDER BY name COLLATE "C" LIMIT n.
-- The database's default collation orders some names differently, so
-- the index carries the comparison the query makes. It lets a page read
-- start after the token name instead of sorting the whole collection.
CREATE INDEX idx_deployments_name_c ON deployments (name COLLATE "C");

-- +goose Down
DROP INDEX idx_deployments_name_c;

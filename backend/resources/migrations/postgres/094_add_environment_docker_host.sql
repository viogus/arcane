-- +goose Up
ALTER TABLE environments ADD COLUMN IF NOT EXISTS docker_host TEXT;

-- +goose Down
ALTER TABLE environments DROP COLUMN IF EXISTS docker_host;

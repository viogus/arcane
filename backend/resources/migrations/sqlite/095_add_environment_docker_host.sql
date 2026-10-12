-- +goose Up
ALTER TABLE environments ADD COLUMN docker_host TEXT;

-- +goose Down
ALTER TABLE environments DROP COLUMN docker_host;

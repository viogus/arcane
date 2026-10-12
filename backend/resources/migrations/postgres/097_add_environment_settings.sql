-- +goose Up
CREATE TABLE IF NOT EXISTS environment_settings (
    environment_id TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (environment_id, key)
);

-- +goose Down
DROP TABLE IF EXISTS environment_settings;

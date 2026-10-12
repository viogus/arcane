-- +goose Up
-- Existing backups belong to the manager's own daemon.
ALTER TABLE volume_backups ADD COLUMN environment_id TEXT NOT NULL DEFAULT '0';
CREATE INDEX idx_volume_backups_environment_id_volume_name ON volume_backups(environment_id, volume_name);

-- +goose Down
DROP INDEX IF EXISTS idx_volume_backups_environment_id_volume_name;
ALTER TABLE volume_backups DROP COLUMN environment_id;

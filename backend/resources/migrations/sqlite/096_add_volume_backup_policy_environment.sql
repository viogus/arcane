-- +goose Up
-- Existing policies belong to the manager's own daemon.
ALTER TABLE volume_backup_policies ADD COLUMN environment_id TEXT NOT NULL DEFAULT '0';
CREATE INDEX idx_volume_backup_policies_environment_id_volume_name ON volume_backup_policies(environment_id, volume_name);

-- +goose Down
DROP INDEX IF EXISTS idx_volume_backup_policies_environment_id_volume_name;
ALTER TABLE volume_backup_policies DROP COLUMN environment_id;

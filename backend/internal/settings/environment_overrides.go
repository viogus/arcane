package settings

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/getarcaneapp/arcane/types/v2/settings"
	"gorm.io/gorm"

	"github.com/getarcaneapp/arcane/backend/v2/pkg/libarcane/envcontext"
)

// EnvironmentSetting is a single per-environment settings override row. Rows are
// stored in the environment_settings table keyed by (environment_id, key) and only
// take effect for keys registered through SetEnvironmentSettingKeys.
type EnvironmentSetting struct {
	EnvironmentID string `gorm:"column:environment_id;primaryKey"`
	Key           string `gorm:"column:key;primaryKey"`
	Value         string `gorm:"column:value;not null;default:''"`
}

// TableName matches the environment_settings migration table.
func (EnvironmentSetting) TableName() string {
	return "environment_settings"
}

// environmentSettingKeysDefaultInternal is the default set of settings keys that may
// be overridden per environment. It intentionally only contains keys that are read
// (and written) through the ctx-aware accessors - GetBoolSetting, GetIntSetting and
// GetStringSetting - so an override is always observable at read time; the job
// scheduler and the auto-update/auto-heal jobs consume every key below that way.
// Keys read from the global GetSettingsConfig snapshot are deliberately absent,
// otherwise a stored override would silently never apply.
var environmentSettingKeysDefaultInternal = []string{
	// Automation toggles.
	"autoUpdate",
	"pollingEnabled",
	"scheduledPruneEnabled",
	"vulnerabilityScanEnabled",
	"imageAutoPatchEnabled",
	"autoHealEnabled",
	// Automation schedules.
	"environmentHealthInterval",
	"dockerClientRefreshInterval",
	"eventCleanupInterval",
	"expiredSessionsCleanupInterval",
	"autoUpdateInterval",
	"pollingInterval",
	"scheduledPruneInterval",
	"vulnerabilityScanInterval",
	"imageAutoPatchInterval",
	"autoHealInterval",
	// Automation parameters.
	"autoHealExcludedContainers",
	"autoHealMaxRestarts",
	"autoHealRestartWindow",
	"autoUpdateExcludedContainers",
}

// EnvironmentSettingKeys returns the default set of settings keys that may carry a
// per-environment override.
func EnvironmentSettingKeys() []string {
	return append([]string(nil), environmentSettingKeysDefaultInternal...)
}

// SetEnvironmentSettingKeys replaces the set of settings keys that may carry a
// per-environment override. Keys outside the registered set are ignored by both the
// read and the write path. Passing an empty slice disables the feature.
func (s *SettingsService) SetEnvironmentSettingKeys(keys []string) {
	registered := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		if key = strings.TrimSpace(key); key != "" {
			registered[key] = struct{}{}
		}
	}

	s.environmentSettingKeysMu.Lock()
	s.environmentSettingKeys = registered
	s.environmentSettingKeysMu.Unlock()
}

// isEnvironmentSettingKeyInternal reports whether key may carry a per-environment
// override.
func (s *SettingsService) isEnvironmentSettingKeyInternal(key string) bool {
	if key == "" {
		return false
	}
	s.environmentSettingKeysMu.RLock()
	defer s.environmentSettingKeysMu.RUnlock()
	_, ok := s.environmentSettingKeys[key]
	return ok
}

// environmentOverrideInternal returns the stored override for key in environmentID
// when both the environment and the key are eligible. A missing row, an unregistered
// key or a manager ("0"/empty) environment reports ok == false so callers fall back
// to the global value.
func (s *SettingsService) environmentOverrideInternal(environmentID, key string) (string, bool) {
	if environmentID == "" || !s.isEnvironmentSettingKeyInternal(key) {
		return "", false
	}

	cache := s.environmentSettings.Load()
	if cache == nil {
		return "", false
	}
	envValues, ok := (*cache)[environmentID]
	if !ok {
		return "", false
	}
	value, ok := envValues[key]
	return value, ok
}

// loadEnvironmentSettingsInternal reads the per-environment overrides into the cache. A database
// that has not run the environment_settings migration yet (a fresh test schema, or an agent-mode
// database that never serves environments) simply has no overrides, so a missing table is not an
// error; any other query failure is still reported.
func (s *SettingsService) loadEnvironmentSettingsInternal(ctx context.Context) error {
	var rows []EnvironmentSetting
	if err := s.db.WithContext(ctx).Find(&rows).Error; err != nil {
		if isMissingEnvironmentSettingsTableInternal(err) {
			s.storeEnvironmentSettingsInternal(nil)
			return nil
		}
		return fmt.Errorf("failed to load environment settings from the database: %w", err)
	}
	s.storeEnvironmentSettingsInternal(rows)
	return nil
}

func isMissingEnvironmentSettingsTableInternal(err error) bool {
	message := strings.ToLower(err.Error())
	// Match only the two phrasings that name the missing table itself. A generic "does not exist"
	// would also swallow a column or type error that merely mentions environment_settings.
	return strings.Contains(message, "no such table: environment_settings") ||
		strings.Contains(message, `relation "environment_settings" does not exist`)
}

// storeEnvironmentSettingsInternal replaces the in-memory override cache. Every row
// is cached; registration is enforced by the read and write paths so that a later
// SetEnvironmentSettingKeys call takes effect without a reload.
func (s *SettingsService) storeEnvironmentSettingsInternal(rows []EnvironmentSetting) {
	cache := make(map[string]map[string]string)
	for _, row := range rows {
		environmentID := strings.TrimSpace(row.EnvironmentID)
		if environmentID == "" || environmentID == "0" || row.Key == "" {
			continue
		}
		envValues, ok := cache[environmentID]
		if !ok {
			envValues = make(map[string]string)
			cache[environmentID] = envValues
		}
		envValues[row.Key] = row.Value
	}
	s.environmentSettings.Store(&cache)
}

// environmentOverrideValuesInternal returns the registered overrides for the target
// environment carried on ctx as a key/value map. It returns nil when ctx has no
// environment target (a manager-local read), when the target is the manager ("0") or
// when the environment has no registered override, so callers keep the global values.
func (s *SettingsService) environmentOverrideValuesInternal(ctx context.Context) map[string]string {
	environmentID := environmentIDFromContextInternal(ctx)
	if environmentID == "" {
		return nil
	}

	cache := s.environmentSettings.Load()
	if cache == nil {
		return nil
	}
	envValues := (*cache)[environmentID]
	if len(envValues) == 0 {
		return nil
	}

	overrides := make(map[string]string, len(envValues))
	for key, value := range envValues {
		if s.isEnvironmentSettingKeyInternal(key) {
			overrides[key] = value
		}
	}
	if len(overrides) == 0 {
		return nil
	}
	return overrides
}

// applyEnvironmentOverridesToSettingVariablesInternal returns values with the target
// environment's registered overrides applied. Without a usable environment target the
// input slice is returned unchanged.
func (s *SettingsService) applyEnvironmentOverridesToSettingVariablesInternal(ctx context.Context, values []SettingVariable) []SettingVariable {
	overrides := s.environmentOverrideValuesInternal(ctx)
	if len(overrides) == 0 {
		return values
	}

	applied := make([]SettingVariable, 0, len(values))
	for _, value := range values {
		if override, ok := overrides[value.Key]; ok {
			value.Value = override
		}
		applied = append(applied, value)
	}
	return applied
}

// applyEnvironmentOverridesInternal returns DTOs with the target environment's
// registered overrides applied. Without a usable environment target the input slice is
// returned unchanged.
func (s *SettingsService) applyEnvironmentOverridesInternal(ctx context.Context, dtos []settings.PublicSetting) []settings.PublicSetting {
	overrides := s.environmentOverrideValuesInternal(ctx)
	if len(overrides) == 0 {
		return dtos
	}

	applied := make([]settings.PublicSetting, 0, len(dtos))
	for _, dto := range dtos {
		if override, ok := overrides[dto.Key]; ok {
			dto.Value = override
		}
		applied = append(applied, dto)
	}
	return applied
}

// writeEnvironmentSettingValuesInternal upserts override rows for one environment in
// a single transaction. Callers must hold s.writes.
func (s *SettingsService) writeEnvironmentSettingValuesInternal(ctx context.Context, environmentID string, values []SettingVariable) error {
	if len(values) == 0 {
		return nil
	}

	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for _, value := range values {
			row := EnvironmentSetting{EnvironmentID: environmentID, Key: value.Key, Value: value.Value}
			if err := tx.Save(&row).Error; err != nil {
				return fmt.Errorf("failed to update environment setting %s: %w", value.Key, err)
			}
		}
		return nil
	})
}

// filterEnvironmentSettingUpdatesInternal clears every update field whose key is not
// registered for per-environment overrides, so an environment-scoped request cannot
// leak values into the global in-memory snapshot.
func (s *SettingsService) filterEnvironmentSettingUpdatesInternal(updates settings.Update) settings.Update {
	rt := reflect.TypeFor[settings.Update]()
	rv := reflect.ValueOf(&updates).Elem()
	for i := range rt.NumField() {
		field := rv.Field(i)
		if field.Kind() != reflect.Pointer || field.IsNil() {
			continue
		}
		key, _, _ := strings.Cut(rt.Field(i).Tag.Get("json"), ",")
		if s.isEnvironmentSettingKeyInternal(key) {
			continue
		}
		field.SetZero()
	}
	return updates
}

// environmentIDFromContextInternal returns the environment carried on ctx, treating
// the manager sentinel "0" and blank values as "no environment target".
func environmentIDFromContextInternal(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	environmentID := strings.TrimSpace(envcontext.EnvironmentIDFromContext(ctx))
	if environmentID == "" || environmentID == "0" {
		return ""
	}
	return environmentID
}

package settings

import (
	"context"
	"testing"

	"github.com/getarcaneapp/arcane/types/v2/settings"
	"github.com/stretchr/testify/require"

	"github.com/getarcaneapp/arcane/backend/v2/internal/config"
	"github.com/getarcaneapp/arcane/backend/v2/internal/middleware"
	"github.com/getarcaneapp/arcane/backend/v2/pkg/authz"
	"github.com/getarcaneapp/arcane/backend/v2/pkg/libarcane"
	"github.com/getarcaneapp/arcane/backend/v2/pkg/libarcane/envcontext"
)

// Keys used by these tests. The first two are registered for per-environment
// overrides by EnvironmentSettingKeys(); the last one is not registered, so it
// must always resolve to the global value.
const (
	testEnvironmentSettingBoolKey     = "autoHealEnabled"
	testEnvironmentSettingScheduleKey = "autoHealInterval"
	testEnvironmentSettingGlobalKey   = "projectsDirectory"
)

func environmentTargetContext(t *testing.T, environmentID string) context.Context {
	t.Helper()
	return envcontext.WithTarget(t.Context(), envcontext.Target{
		EnvironmentID: environmentID,
		DockerHost:    "unix:///var/run/docker.sock",
	})
}

// seedEnvironmentOverridesInternal writes per-environment overrides through the helper the
// production write path uses and refreshes the cache, returning the accepted subset.
func seedEnvironmentOverridesInternal(t *testing.T, svc *SettingsService, ctx context.Context, updates []SettingVariable) []SettingVariable {
	t.Helper()
	environmentID := environmentIDFromContextInternal(ctx)
	require.NotEmpty(t, environmentID, "seeding overrides requires an environment target")

	accepted := make([]SettingVariable, 0, len(updates))
	for _, update := range updates {
		if svc.isEnvironmentSettingKeyInternal(update.Key) {
			accepted = append(accepted, update)
		}
	}

	svc.writes.Lock()
	defer svc.writes.Unlock()
	if len(accepted) > 0 {
		require.NoError(t, svc.writeEnvironmentSettingValuesInternal(ctx, environmentID, accepted))
	}
	require.NoError(t, svc.LoadDatabaseSettings(context.WithoutCancel(ctx)))
	return accepted
}

func TestSettingsService_EnvironmentOverride_RegisteredKeyWinsOverGlobal(t *testing.T) {
	ctx := t.Context()
	db := setupSettingsTestDB(t)
	svc, err := newSettingsServiceForTest(t, ctx, db)
	require.NoError(t, err)

	require.NoError(t, svc.UpdateSetting(ctx, testEnvironmentSettingBoolKey, "true"))
	require.True(t, svc.GetBoolSetting(ctx, testEnvironmentSettingBoolKey, false))

	envCtx := environmentTargetContext(t, "env-override-wins")
	accepted := seedEnvironmentOverridesInternal(t, svc, envCtx, []SettingVariable{
		{Key: testEnvironmentSettingBoolKey, Value: "false"},
	})
	require.Len(t, accepted, 1)

	require.False(t, svc.GetBoolSetting(envCtx, testEnvironmentSettingBoolKey, true),
		"environment override must win over the global value")
	require.True(t, svc.GetBoolSetting(ctx, testEnvironmentSettingBoolKey, false),
		"global value must be untouched")
}

func TestSettingsService_EnvironmentOverride_FallsBackToGlobal(t *testing.T) {
	ctx := t.Context()
	db := setupSettingsTestDB(t)
	svc, err := newSettingsServiceForTest(t, ctx, db)
	require.NoError(t, err)

	require.NoError(t, svc.UpdateSetting(ctx, testEnvironmentSettingScheduleKey, "0 1 * * * *"))

	envCtx := environmentTargetContext(t, "env-without-override")
	require.Equal(t, "0 1 * * * *", svc.GetStringSetting(envCtx, testEnvironmentSettingScheduleKey, ""),
		"an environment without an override falls back to the global value")
}

func TestSettingsService_EnvironmentOverride_UnregisteredKeyIgnoresOverride(t *testing.T) {
	ctx := t.Context()
	db := setupSettingsTestDB(t)
	svc, err := newSettingsServiceForTest(t, ctx, db)
	require.NoError(t, err)

	require.NoError(t, svc.UpdateSetting(ctx, testEnvironmentSettingGlobalKey, "/srv/global"))

	// Seed the override row directly: the write path refuses unregistered keys,
	// so this is the only way to prove the read path also ignores them.
	require.NoError(t, db.Create(&EnvironmentSetting{
		EnvironmentID: "env-unregistered",
		Key:           testEnvironmentSettingGlobalKey,
		Value:         "/srv/environment",
	}).Error)
	require.NoError(t, svc.LoadDatabaseSettings(ctx))

	envCtx := environmentTargetContext(t, "env-unregistered")
	require.Equal(t, "/srv/global", svc.GetStringSetting(envCtx, testEnvironmentSettingGlobalKey, ""),
		"an unregistered key must never read its override")

	// The write path ignores unregistered keys as well.
	accepted := seedEnvironmentOverridesInternal(t, svc, envCtx, []SettingVariable{
		{Key: testEnvironmentSettingGlobalKey, Value: "/srv/other"},
	})
	require.Empty(t, accepted, "unregistered keys must not be accepted")

	var stored []EnvironmentSetting
	require.NoError(t, db.Where("environment_id = ?", "env-unregistered").Find(&stored).Error)
	require.Len(t, stored, 1)
	require.Equal(t, "/srv/environment", stored[0].Value, "unregistered keys must not be persisted")
}

func TestSettingsService_EnvironmentOverride_ManagerTargetNeverReadsOverrides(t *testing.T) {
	ctx := t.Context()
	db := setupSettingsTestDB(t)
	svc, err := newSettingsServiceForTest(t, ctx, db)
	require.NoError(t, err)

	require.NoError(t, svc.UpdateSetting(ctx, testEnvironmentSettingBoolKey, "true"))

	// Directly seed overrides for the manager sentinel and for a real environment.
	require.NoError(t, db.Create(&EnvironmentSetting{EnvironmentID: "0", Key: testEnvironmentSettingBoolKey, Value: "false"}).Error)
	require.NoError(t, db.Create(&EnvironmentSetting{EnvironmentID: "env-real", Key: testEnvironmentSettingBoolKey, Value: "false"}).Error)
	require.NoError(t, svc.LoadDatabaseSettings(ctx))

	require.True(t, svc.GetBoolSetting(environmentTargetContext(t, "0"), testEnvironmentSettingBoolKey, false),
		`environment "0" must never read overrides`)
	require.True(t, svc.GetBoolSetting(ctx, testEnvironmentSettingBoolKey, false),
		"a context without an environment target must never read overrides")
	// Positive control: a real environment does read the seeded override.
	require.False(t, svc.GetBoolSetting(environmentTargetContext(t, "env-real"), testEnvironmentSettingBoolKey, false))
}

func TestSettingsService_EnvironmentOverride_RoundTripDoesNotLeak(t *testing.T) {
	ctx := t.Context()
	db := setupSettingsTestDB(t)
	svc, err := newSettingsServiceForTest(t, ctx, db)
	require.NoError(t, err)

	require.NoError(t, svc.UpdateSetting(ctx, testEnvironmentSettingScheduleKey, "0 2 * * * *"))

	envOne := environmentTargetContext(t, "env-one")
	envTwo := environmentTargetContext(t, "env-two")

	accepted := seedEnvironmentOverridesInternal(t, svc, envOne, []SettingVariable{
		{Key: testEnvironmentSettingScheduleKey, Value: "0 3 * * * *"},
	})
	require.Len(t, accepted, 1)

	require.Equal(t, "0 3 * * * *", svc.GetStringSetting(envOne, testEnvironmentSettingScheduleKey, ""))
	require.Equal(t, "0 2 * * * *", svc.GetStringSetting(envTwo, testEnvironmentSettingScheduleKey, ""),
		"the override must not leak to another environment")
	require.Equal(t, "0 2 * * * *", svc.GetStringSetting(ctx, testEnvironmentSettingScheduleKey, ""),
		"the override must not leak to the global value")

	var stored []EnvironmentSetting
	require.NoError(t, db.Where("environment_id = ?", "env-one").Find(&stored).Error)
	require.Len(t, stored, 1)
	require.Equal(t, testEnvironmentSettingScheduleKey, stored[0].Key)
	require.Equal(t, "0 3 * * * *", stored[0].Value)

	var global SettingVariable
	require.NoError(t, db.First(&global, "key = ?", testEnvironmentSettingScheduleKey).Error)
	require.Equal(t, "0 2 * * * *", global.Value, "the global row must be untouched")

	// A second write for the same environment replaces the row instead of adding one.
	accepted = seedEnvironmentOverridesInternal(t, svc, envOne, []SettingVariable{
		{Key: testEnvironmentSettingScheduleKey, Value: "0 4 * * * *"},
	})
	require.Len(t, accepted, 1)
	require.Equal(t, "0 4 * * * *", svc.GetStringSetting(envOne, testEnvironmentSettingScheduleKey, ""))

	stored = nil
	require.NoError(t, db.Where("environment_id = ?", "env-one").Find(&stored).Error)
	require.Len(t, stored, 1)
	require.Equal(t, "0 4 * * * *", stored[0].Value)
}

func TestSettingsService_EnvironmentOverride_CacheRefreshesAfterUpdate(t *testing.T) {
	ctx := t.Context()
	db := setupSettingsTestDB(t)
	svc, err := newSettingsServiceForTest(t, ctx, db)
	require.NoError(t, err)

	envCtx := environmentTargetContext(t, "env-cache-refresh")
	require.NotEqual(t, "0 5 * * * *", svc.GetStringSetting(envCtx, testEnvironmentSettingScheduleKey, "fallback"))

	_ = seedEnvironmentOverridesInternal(t, svc, envCtx, []SettingVariable{
		{Key: testEnvironmentSettingScheduleKey, Value: "0 5 * * * *"},
	})
	require.Equal(t, "0 5 * * * *", svc.GetStringSetting(envCtx, testEnvironmentSettingScheduleKey, "fallback"),
		"the in-memory override cache must be refreshed by the write")

	// A freshly constructed service loads the same override from the database.
	otherSvc, err := newSettingsServiceForTest(t, ctx, db)
	require.NoError(t, err)
	require.Equal(t, "0 5 * * * *", otherSvc.GetStringSetting(envCtx, testEnvironmentSettingScheduleKey, "fallback"))
}

func TestSettingsService_UpdateSettings_EnvironmentTargetRoutesRegisteredKeysOnly(t *testing.T) {
	ctx := t.Context()
	db := setupSettingsTestDB(t)
	svc, err := newSettingsServiceForTest(t, ctx, db)
	require.NoError(t, err)

	require.NoError(t, svc.UpdateSetting(ctx, testEnvironmentSettingBoolKey, "false"))
	require.NoError(t, svc.UpdateSetting(ctx, testEnvironmentSettingGlobalKey, "/srv/global"))

	notifications := make(chan []libarcane.SettingUpdate, 4)
	unsubscribe := svc.SubscribeSettingsChanges([]string{
		testEnvironmentSettingBoolKey,
		testEnvironmentSettingScheduleKey,
		testEnvironmentSettingGlobalKey,
	}, func(updates []libarcane.SettingUpdate) {
		notifications <- updates
	})
	defer unsubscribe()

	envCtx := environmentTargetContext(t, "env-routing")
	_, err = svc.UpdateSettings(envCtx, settings.Update{
		AutoHealEnabled:   new("true"),
		AutoHealInterval:  new("0 6 * * * *"),
		ProjectsDirectory: new("/srv/environment"),
	})
	require.NoError(t, err)
	waitForSettingsNotifications(t, svc)

	// Registered keys become this environment's overrides.
	require.True(t, svc.GetBoolSetting(envCtx, testEnvironmentSettingBoolKey, false))
	require.Equal(t, "0 6 * * * *", svc.GetStringSetting(envCtx, testEnvironmentSettingScheduleKey, ""))

	// Unregistered keys are skipped entirely: neither the environment nor the global changes.
	require.Equal(t, "/srv/global", svc.GetStringSetting(envCtx, testEnvironmentSettingGlobalKey, ""))
	require.Equal(t, "/srv/global", svc.GetStringSetting(ctx, testEnvironmentSettingGlobalKey, ""))

	var globalAutoHeal SettingVariable
	require.NoError(t, db.First(&globalAutoHeal, "key = ?", testEnvironmentSettingBoolKey).Error)
	require.Equal(t, "false", globalAutoHeal.Value, "the global value must not be overwritten")

	var stored []EnvironmentSetting
	require.NoError(t, db.Where("environment_id = ?", "env-routing").Find(&stored).Error)
	require.Len(t, stored, 2, "only registered keys may be persisted for the environment")

	// Subscribers still receive the change notification for the routed keys.
	select {
	case updates := <-notifications:
		keys := make(map[string]bool, len(updates))
		for _, update := range updates {
			keys[update.Key] = true
		}
		require.True(t, keys[testEnvironmentSettingBoolKey])
		require.True(t, keys[testEnvironmentSettingScheduleKey])
		require.False(t, keys[testEnvironmentSettingGlobalKey], "skipped keys must not be published")
	case <-t.Context().Done():
		t.Fatal("timed out waiting for settings change notification")
	}
}

// testEnvironmentSettingPublicKey is a public setting, so it is the only kind of key
// that can appear in both the public settings subset and the UpdateSettings response
// slice. featureSwarmEnabled is the environment-scoped feature flag the frontend sends
// to the environment endpoint.
const testEnvironmentSettingPublicKey = "featureSwarmEnabled"

func environmentSettingValue(t *testing.T, publicSettings []settings.PublicSetting, key string) string {
	t.Helper()
	for _, setting := range publicSettings {
		if setting.Key == key {
			return setting.Value
		}
	}
	t.Fatalf("setting %q not found in %d settings", key, len(publicSettings))
	return ""
}

func settingDtoValue(t *testing.T, dtos []settings.SettingDto, key string) string {
	t.Helper()
	for _, dto := range dtos {
		if dto.Key == key {
			return dto.Value
		}
	}
	t.Fatalf("setting %q not found in %d setting DTOs", key, len(dtos))
	return ""
}

func adminSettingsContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, middleware.ContextKeyUserPermissions, authz.SudoPermissionSet())
}

func TestSettingsHandler_GetSettings_AppliesEnvironmentOverrides(t *testing.T) {
	ctx := t.Context()
	db := setupSettingsTestDB(t)
	svc, err := newSettingsServiceForTest(t, ctx, db)
	require.NoError(t, err)

	require.NoError(t, svc.UpdateSetting(ctx, testEnvironmentSettingBoolKey, "true"))
	envID := "env-handler-read"
	envCtx := environmentTargetContext(t, envID)
	seedEnvironmentOverridesInternal(t, svc, envCtx, []SettingVariable{
		{Key: testEnvironmentSettingBoolKey, Value: "false"},
	})

	handler := &SettingsHandler{settingsService: svc, cfg: &config.Config{}}

	// An environment with an override reads its own value.
	output, err := handler.GetSettings(adminSettingsContext(envCtx), &GetSettingsInput{EnvironmentID: envID})
	require.NoError(t, err)
	require.Equal(t, "false", environmentSettingValue(t, output.Body, testEnvironmentSettingBoolKey))

	// An environment without an override falls back to the global value.
	otherEnvID := "env-handler-read-fallback"
	otherCtx := environmentTargetContext(t, otherEnvID)
	output, err = handler.GetSettings(adminSettingsContext(otherCtx), &GetSettingsInput{EnvironmentID: otherEnvID})
	require.NoError(t, err)
	require.Equal(t, "true", environmentSettingValue(t, output.Body, testEnvironmentSettingBoolKey))

	// The manager ("0") and a context without an environment target stay unchanged.
	output, err = handler.GetSettings(adminSettingsContext(environmentTargetContext(t, "0")), &GetSettingsInput{EnvironmentID: "0"})
	require.NoError(t, err)
	require.Equal(t, "true", environmentSettingValue(t, output.Body, testEnvironmentSettingBoolKey))

	output, err = handler.GetSettings(adminSettingsContext(ctx), &GetSettingsInput{EnvironmentID: "0"})
	require.NoError(t, err)
	require.Equal(t, "true", environmentSettingValue(t, output.Body, testEnvironmentSettingBoolKey))
}

func TestSettingsHandler_GetPublicSettings_AppliesEnvironmentOverrides(t *testing.T) {
	ctx := t.Context()
	db := setupSettingsTestDB(t)
	svc, err := newSettingsServiceForTest(t, ctx, db)
	require.NoError(t, err)

	// The default registered set holds no public key, so the public subset cannot show an
	// override. Register a public key to exercise the merge on that endpoint.
	svc.SetEnvironmentSettingKeys([]string{testEnvironmentSettingPublicKey})
	require.NoError(t, svc.UpdateSetting(ctx, testEnvironmentSettingPublicKey, "true"))

	envID := "env-handler-public"
	envCtx := environmentTargetContext(t, envID)
	seedEnvironmentOverridesInternal(t, svc, envCtx, []SettingVariable{
		{Key: testEnvironmentSettingPublicKey, Value: "false"},
	})

	handler := &SettingsHandler{settingsService: svc, cfg: &config.Config{}}

	output, err := handler.GetPublicSettings(envCtx, &GetPublicSettingsInput{EnvironmentID: envID})
	require.NoError(t, err)
	require.Equal(t, "false", environmentSettingValue(t, output.Body, testEnvironmentSettingPublicKey))

	otherEnvID := "env-handler-public-fallback"
	output, err = handler.GetPublicSettings(environmentTargetContext(t, otherEnvID), &GetPublicSettingsInput{EnvironmentID: otherEnvID})
	require.NoError(t, err)
	require.Equal(t, "true", environmentSettingValue(t, output.Body, testEnvironmentSettingPublicKey))

	output, err = handler.GetPublicSettings(ctx, &GetPublicSettingsInput{EnvironmentID: "0"})
	require.NoError(t, err)
	require.Equal(t, "true", environmentSettingValue(t, output.Body, testEnvironmentSettingPublicKey))
}

func TestSettingsService_DefaultRegisteredKeysAreNotPublic(t *testing.T) {
	// Documents why the public endpoint and the UpdateSettings response slice show no
	// override with the default registration: neither exposes a registered key. If a
	// public key is ever registered, this test fails so the visibility choice is revisited.
	ctx := t.Context()
	svc, err := newSettingsServiceForTest(t, ctx, setupSettingsTestDB(t))
	require.NoError(t, err)

	registered := EnvironmentSettingKeys()
	for _, setting := range svc.ListSettings(SettingVisibilityPublic) {
		require.NotContains(t, registered, setting.Key)
	}
	for _, setting := range svc.ListSettings(SettingVisibilityNonAdmin) {
		require.NotContains(t, registered, setting.Key)
	}
}

func TestSettingsHandler_UpdateSettings_ReturnsEffectiveEnvironmentValues(t *testing.T) {
	ctx := t.Context()
	db := setupSettingsTestDB(t)
	svc, err := newSettingsServiceForTest(t, ctx, db)
	require.NoError(t, err)

	svc.SetEnvironmentSettingKeys([]string{testEnvironmentSettingPublicKey})
	require.NoError(t, svc.UpdateSetting(ctx, testEnvironmentSettingPublicKey, "true"))

	handler := &SettingsHandler{settingsService: svc, cfg: &config.Config{}}

	envID := "env-handler-put"
	envCtx := adminSettingsContext(environmentTargetContext(t, envID))
	output, err := handler.UpdateSettings(envCtx, &UpdateSettingsInput{
		EnvironmentID: envID,
		Body:          settings.Update{FeatureSwarmEnabled: new("false")},
	})
	require.NoError(t, err)
	require.Equal(t, "false", settingDtoValue(t, output.Body.Data, testEnvironmentSettingPublicKey),
		"the update response must carry the effective environment value")

	// The override is stored for that environment only; the global value is untouched.
	require.False(t, svc.GetBoolSetting(envCtx, testEnvironmentSettingPublicKey, true))
	require.True(t, svc.GetBoolSetting(ctx, testEnvironmentSettingPublicKey, false))

	output, err = handler.UpdateSettings(adminSettingsContext(ctx), &UpdateSettingsInput{
		EnvironmentID: "0",
		Body:          settings.Update{FeatureSwarmEnabled: new("true")},
	})
	require.NoError(t, err)
	require.Equal(t, "true", settingDtoValue(t, output.Body.Data, testEnvironmentSettingPublicKey),
		"a manager update keeps returning the global value")
}

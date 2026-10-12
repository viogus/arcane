package job

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/getarcaneapp/arcane/types/v2/jobschedule"
	schedulertypes "github.com/getarcaneapp/arcane/types/v2/scheduler"

	"github.com/getarcaneapp/arcane/backend/v2/internal/config"
	"github.com/getarcaneapp/arcane/backend/v2/internal/environment"
	"github.com/getarcaneapp/arcane/backend/v2/internal/job/children/remote"
	"github.com/getarcaneapp/arcane/backend/v2/internal/kv"
	"github.com/getarcaneapp/arcane/backend/v2/internal/settings"
	"github.com/getarcaneapp/arcane/backend/v2/pkg/libarcane/envcontext"
	schedulerpkg "github.com/getarcaneapp/arcane/backend/v2/pkg/scheduler"
)

const (
	managerAutoHealScheduleInternal     = "0 0 2 * * *"
	dockerHostAutoHealScheduleInternal  = "0 30 4 * * *"
	environmentJobsTestJobIDInternal    = "auto-heal"
	environmentJobsTestSettingsKeyValue = "autoHealInterval"
)

// newEnvironmentJobsFixtureInternal wires a job service with a real scheduler so per-environment
// schedule records can be inspected. The coordinator's executor is stubbed: these tests assert on
// installed schedules, not on run execution.
func newEnvironmentJobsFixtureInternal(t *testing.T) (*JobService, schedulertypes.JobScheduler, *settings.SettingsService) {
	t.Helper()
	ctx := t.Context()
	db := setupSettingsTestDBInternal(t)
	require.NoError(t, db.AutoMigrate(&environment.Environment{}))

	endpoint := dockerHostJobEndpointInternal
	require.NoError(t, db.Create(&environment.Environment{
		ID:         dockerHostJobEnvironmentIDInternal,
		Name:       "arm-direct",
		Enabled:    true,
		DockerHost: &endpoint,
	}).Error)
	// The agent points at a closed port so any regression back to the proxy protocol fails fast.
	require.NoError(t, db.Create(&environment.Environment{
		ID:      agentJobEnvironmentIDInternal,
		Name:    "agent",
		Enabled: true,
		ApiUrl:  "http://127.0.0.1:1",
	}).Error)

	settingsService, err := newSettingsServiceForTestInternal(t, ctx, db)
	require.NoError(t, err)

	environments := environment.NewEnvironmentService(db, nil, nil, nil, nil, nil)
	coordinator := newJobCoordinatorForTestInternal(t, db)
	coordinator.SetExecutor(
		func(context.Context, schedulertypes.Run) (schedulertypes.Outcome, error) {
			return schedulertypes.Outcome{Status: schedulertypes.Waiting}, nil
		},
		func(context.Context, schedulertypes.Run) (schedulertypes.Outcome, error) {
			return schedulertypes.Outcome{Status: schedulertypes.Waiting}, nil
		},
	)

	jobScheduler, err := schedulerpkg.NewJobScheduler(ctx, coordinator, time.UTC)
	require.NoError(t, err)
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 5*time.Second)
		defer cancel()
		require.NoError(t, jobScheduler.Stop(stopCtx))
	})

	service := &JobService{
		db:          db,
		cfg:         &config.Config{},
		settings:    settingsService,
		runs:        coordinator,
		environment: environments,
		remote:      remote.New(coordinator, kv.NewKVService(db), environments),
		scheduler:   jobScheduler,
	}
	service.SetScheduler(ctx, jobScheduler)
	return service, jobScheduler, settingsService
}

// TestJobService_SchedulesCronJobsPerDockerHostEnvironment proves a docker-host environment gets its
// own schedule record from its own settings, that the manager's record is untouched, that agent
// environments are never served in-process, and that forgetting an environment clears only its own
// record.
func TestJobService_SchedulesCronJobsPerDockerHostEnvironment(t *testing.T) {
	ctx := t.Context()
	service, jobScheduler, settingsService := newEnvironmentJobsFixtureInternal(t)

	job := &schedulertypes.GenericJob{
		JobName: environmentJobsTestJobIDInternal,
		ScheduleFn: func(ctx context.Context) string {
			return settingsService.GetStringSetting(ctx, environmentJobsTestSettingsKeyValue, managerAutoHealScheduleInternal)
		},
		RunFn: func(context.Context) (schedulertypes.Outcome, error) {
			return schedulertypes.Outcome{Status: schedulertypes.Succeeded}, nil
		},
	}
	require.NoError(t, jobScheduler.RegisterJob(job))
	require.NoError(t, jobScheduler.StartScheduler(ctx))

	// The manager's own interval is set through the same jobs API the UI uses.
	managerSchedule := managerAutoHealScheduleInternal
	_, err := service.UpdateJobSchedules(ctx, jobschedule.Update{AutoHealInterval: &managerSchedule})
	require.NoError(t, err)

	managerState, ok := jobScheduler.GetJobRuntimeState(environmentJobsTestJobIDInternal)
	require.True(t, ok)
	require.Equal(t, managerSchedule, managerState.Schedule)
	require.True(t, managerState.Scheduled)

	environmentContext := envcontext.WithTarget(ctx, envcontext.Target{
		EnvironmentID: dockerHostJobEnvironmentIDInternal,
		DockerHost:    dockerHostJobEndpointInternal,
	})
	_, err = settingsService.UpdateEnvironmentSettings(environmentContext, []settings.SettingVariable{{
		Key:   environmentJobsTestSettingsKeyValue,
		Value: dockerHostAutoHealScheduleInternal,
	}})
	require.NoError(t, err)
	require.Equal(t, dockerHostAutoHealScheduleInternal,
		settingsService.GetStringSetting(environmentContext, environmentJobsTestSettingsKeyValue, ""),
		"the environment override must be visible through the context-aware accessor")

	require.NoError(t, service.SyncEnvironmentJobs(ctx, dockerHostJobEnvironmentIDInternal))

	environmentState, ok := jobScheduler.GetJobRuntimeStateFor(ctx, dockerHostJobEnvironmentIDInternal, environmentJobsTestJobIDInternal)
	require.True(t, ok)
	require.Equal(t, dockerHostAutoHealScheduleInternal, environmentState.Schedule)
	require.True(t, environmentState.Scheduled)

	managerState, ok = jobScheduler.GetJobRuntimeState(environmentJobsTestJobIDInternal)
	require.True(t, ok)
	require.Equal(t, managerAutoHealScheduleInternal, managerState.Schedule,
		"syncing a docker-host environment must not move the manager's schedule")

	require.NoError(t, service.SyncEnvironmentJobs(ctx, agentJobEnvironmentIDInternal))
	agentState, _ := jobScheduler.GetJobRuntimeStateFor(ctx, agentJobEnvironmentIDInternal, environmentJobsTestJobIDInternal)
	require.Empty(t, agentState.Schedule, "agent environments keep their schedules on the agent, not in this process")
	require.False(t, agentState.Scheduled)

	require.NoError(t, service.ForgetEnvironmentJobs(ctx, dockerHostJobEnvironmentIDInternal))
	environmentState, _ = jobScheduler.GetJobRuntimeStateFor(ctx, dockerHostJobEnvironmentIDInternal, environmentJobsTestJobIDInternal)
	require.Empty(t, environmentState.Schedule)
	require.False(t, environmentState.Scheduled)

	managerState, ok = jobScheduler.GetJobRuntimeState(environmentJobsTestJobIDInternal)
	require.True(t, ok)
	require.Equal(t, managerSchedule, managerState.Schedule)
	require.True(t, managerState.Scheduled, "forgetting an environment must leave the manager scheduled")
}

// TestJobService_SyncEnvironmentJobsUsesEnvironmentScopedContextInternal documents the routing
// predicate behind SyncEnvironmentJobs: only docker-host environments are served in-process.
func TestJobService_SyncEnvironmentJobsUsesEnvironmentScopedContextInternal(t *testing.T) {
	service, _, _ := newEnvironmentJobsFixtureInternal(t)
	ctx := t.Context()

	require.NoError(t, service.SyncEnvironmentJobs(ctx, environment.LocalEnvironmentID))
	require.NoError(t, service.SyncEnvironmentJobs(ctx, agentJobEnvironmentIDInternal))

	unknown := "env-does-not-exist"
	require.NoError(t, service.SyncEnvironmentJobs(ctx, unknown),
		"an unknown environment must not be treated as in-process")
	require.NoError(t, service.ForgetEnvironmentJobs(ctx, unknown))

	scoped, inProcess := service.environmentScopedContextInternal(ctx, dockerHostJobEnvironmentIDInternal)
	require.True(t, inProcess)
	target, ok := envcontext.TargetFromContext(scoped)
	require.True(t, ok)
	require.Equal(t, dockerHostJobEnvironmentIDInternal, target.EnvironmentID)
	require.Equal(t, dockerHostJobEndpointInternal, target.DockerHost)
}

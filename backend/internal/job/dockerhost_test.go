package job

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/getarcaneapp/arcane/types/v2/scheduler"

	"github.com/getarcaneapp/arcane/backend/v2/internal/config"
	"github.com/getarcaneapp/arcane/backend/v2/internal/environment"
	"github.com/getarcaneapp/arcane/backend/v2/internal/job/children/remote"
	"github.com/getarcaneapp/arcane/backend/v2/internal/kv"
	"github.com/getarcaneapp/arcane/backend/v2/pkg/libarcane/envcontext"
	"github.com/getarcaneapp/arcane/backend/v2/pkg/scheduler/runs"
)

const (
	dockerHostJobEnvironmentIDInternal = "env-docker-host-job"
	agentJobEnvironmentIDInternal      = "env-agent-job"
	dockerHostJobEndpointInternal      = "tcp://192.168.2.33:6002"
	testJobIDInternal                  = "docker-client-refresh"
)

// dockerHostJobControllerInternal is a minimal scheduler controller: docker-host job runs must reach
// the job's Run with the environment's Docker target in context.
type dockerHostJobControllerInternal struct {
	job scheduler.Job
}

func (c *dockerHostJobControllerInternal) GetJob(jobID string) (scheduler.Job, bool) {
	if c.job != nil && c.job.Name() == jobID {
		return c.job, true
	}
	return nil, false
}

func (c *dockerHostJobControllerInternal) GetJobRuntimeState(string) (scheduler.JobRuntimeState, bool) {
	return scheduler.JobRuntimeState{}, false
}

func (c *dockerHostJobControllerInternal) GetJobRuntimeStateFor(context.Context, string, string) (scheduler.JobRuntimeState, bool) {
	return scheduler.JobRuntimeState{}, false
}

func (c *dockerHostJobControllerInternal) ClearJobScheduleFor(context.Context, string, string) error {
	return nil
}

func (c *dockerHostJobControllerInternal) RescheduleJob(context.Context, scheduler.Job) error {
	return nil
}

func (c *dockerHostJobControllerInternal) RunBusWatcherNow(context.Context, string) error { return nil }

type recordingJobInternal struct {
	id  string
	run func(ctx context.Context) (scheduler.Outcome, error)
}

func (j recordingJobInternal) Name() string { return j.id }

func (j recordingJobInternal) Schedule(context.Context) string { return "0 0 * * * *" }

func (j recordingJobInternal) Run(ctx context.Context) (scheduler.Outcome, error) { return j.run(ctx) }

// newDockerHostJobFixtureInternal wires a job service exactly like production does for the two kinds
// of environment that matter here: a docker-host environment (served in-process) and an agent
// environment (served through the environment proxy).
func newDockerHostJobFixtureInternal(t *testing.T) (*JobService, *runs.Coordinator) {
	t.Helper()
	db := setupSettingsTestDB(t)
	require.NoError(t, db.AutoMigrate(&environment.Environment{}))

	endpoint := dockerHostJobEndpointInternal
	require.NoError(t, db.Create(&environment.Environment{
		ID:         dockerHostJobEnvironmentIDInternal,
		Name:       "arm-direct",
		Enabled:    true,
		DockerHost: &endpoint,
	}).Error)
	// The agent points at a closed port so any regression back to the proxy protocol fails fast
	// instead of silently succeeding.
	require.NoError(t, db.Create(&environment.Environment{
		ID:      agentJobEnvironmentIDInternal,
		Name:    "agent",
		Enabled: true,
		ApiUrl:  "http://127.0.0.1:1",
	}).Error)

	settingsService, settingsErr := newSettingsServiceForTest(t, t.Context(), db)
	require.NoError(t, settingsErr)
	environments := environment.NewEnvironmentService(db, nil, nil, nil, settingsService, nil)
	coordinator := newJobCoordinatorForTest(t, db)
	// Tests drive execution explicitly; the dispatcher must not race them.
	coordinator.SetExecutor(
		func(context.Context, scheduler.Run) (scheduler.Outcome, error) {
			return scheduler.Outcome{Status: scheduler.Waiting}, nil
		},
		func(context.Context, scheduler.Run) (scheduler.Outcome, error) {
			return scheduler.Outcome{Status: scheduler.Waiting}, nil
		},
	)

	return &JobService{
		db:          db,
		cfg:         &config.Config{},
		runs:        coordinator,
		environment: environments,
		remote:      remote.New(coordinator, kv.NewKVService(db), environments),
	}, coordinator
}

// TestJobService_InProcessTargetContextInternal covers the routing predicate shared by every job
// code path: only the manager's own daemon and docker-host environments are served in-process.
func TestJobService_InProcessTargetContextInternal(t *testing.T) {
	service, _ := newDockerHostJobFixtureInternal(t)
	ctx := t.Context()

	t.Run("manager daemon runs unscoped", func(t *testing.T) {
		got, ok := service.environment.InProcessTargetContext(ctx, "0")
		require.True(t, ok)
		assert.Empty(t, envcontext.DockerHostFromContext(got))
	})

	t.Run("docker-host daemon is scoped to that environment", func(t *testing.T) {
		got, ok := service.environment.InProcessTargetContext(ctx, dockerHostJobEnvironmentIDInternal)
		require.True(t, ok)
		assert.Equal(t, dockerHostJobEndpointInternal, envcontext.DockerHostFromContext(got))
		assert.Equal(t, dockerHostJobEnvironmentIDInternal, envcontext.EnvironmentIDFromContext(got))
	})

	t.Run("agent environment is not served here", func(t *testing.T) {
		_, ok := service.environment.InProcessTargetContext(ctx, agentJobEnvironmentIDInternal)
		assert.False(t, ok)
	})

	t.Run("unknown environment is not served here", func(t *testing.T) {
		_, ok := service.environment.InProcessTargetContext(ctx, "env-missing")
		assert.False(t, ok)
	})

	t.Run("an already scoped context needs no lookup", func(t *testing.T) {
		scoped := envcontext.WithTarget(ctx, envcontext.Target{
			EnvironmentID: dockerHostJobEnvironmentIDInternal,
			DockerHost:    "tcp://host:2375",
		})
		got, ok := (&environment.EnvironmentService{}).InProcessTargetContext(scoped, dockerHostJobEnvironmentIDInternal)
		require.True(t, ok)
		assert.Equal(t, "tcp://host:2375", envcontext.DockerHostFromContext(got))
	})
}

// TestJobService_ExecuteRunInternal_RunsDockerHostRunsAgainstItsOwnDaemon is the regression guard
// for the reported gap: a run for a docker-host environment used to be delivered over the remote
// job protocol, which fails because that environment has no agent.
func TestJobService_ExecuteRunInternal_RunsDockerHostRunsAgainstItsOwnDaemon(t *testing.T) {
	service, _ := newDockerHostJobFixtureInternal(t)

	var observed envcontext.Target
	var observedOK bool
	service.scheduler = &dockerHostJobControllerInternal{job: recordingJobInternal{
		id: testJobIDInternal,
		run: func(ctx context.Context) (scheduler.Outcome, error) {
			observed, observedOK = envcontext.TargetFromContext(ctx)
			return scheduler.Outcome{Status: scheduler.Succeeded}, nil
		},
	}}

	outcome, err := service.executeRun(t.Context(), scheduler.Run{
		ID:            "2f0f0a5c-3a58-4a1f-9d11-2f9a30d1c101",
		JobID:         testJobIDInternal,
		EnvironmentID: dockerHostJobEnvironmentIDInternal,
		Status:        scheduler.Running,
		Trigger:       "scheduled",
	})
	require.NoError(t, err)
	require.Equal(t, scheduler.Succeeded, outcome.Status)
	require.True(t, observedOK, "the job must run in this process")
	assert.Equal(t, dockerHostJobEnvironmentIDInternal, observed.EnvironmentID)
	assert.Equal(t, dockerHostJobEndpointInternal, observed.DockerHost)
}

// TestJobService_ExecuteRunInternal_KeepsAgentRunsRemote proves the docker-host branch did not
// swallow agent environments: those still go through the delivery protocol and never run here.
func TestJobService_ExecuteRunInternal_KeepsAgentRunsRemote(t *testing.T) {
	service, _ := newDockerHostJobFixtureInternal(t)

	executed := 0
	service.scheduler = &dockerHostJobControllerInternal{job: recordingJobInternal{
		id: testJobIDInternal,
		run: func(context.Context) (scheduler.Outcome, error) {
			executed++
			return scheduler.Outcome{Status: scheduler.Succeeded}, nil
		},
	}}

	outcome, err := service.executeRun(t.Context(), scheduler.Run{
		ID:            "2f0f0a5c-3a58-4a1f-9d11-2f9a30d1c102",
		JobID:         testJobIDInternal,
		EnvironmentID: agentJobEnvironmentIDInternal,
		Status:        scheduler.Running,
		Trigger:       "scheduled",
	})
	require.Error(t, err)
	require.Equal(t, scheduler.Waiting, outcome.Status)
	assert.Zero(t, executed, "an agent environment must still be served by remote delivery")
}

// TestJobService_Submit_AcceptsDockerHostJobs pins the admission rule: a manager-only job can never
// be pushed to an agent, but a docker-host environment runs it here.
func TestJobService_Submit_AcceptsDockerHostJobs(t *testing.T) {
	service, _ := newDockerHostJobFixtureInternal(t)

	_, err := service.Submit(t.Context(), scheduler.Request{
		JobID:         "environment-health",
		EnvironmentID: agentJobEnvironmentIDInternal,
	})
	require.ErrorContains(t, err, "job is not remotely runnable")

	run, err := service.Submit(t.Context(), scheduler.Request{
		JobID:         "environment-health",
		EnvironmentID: dockerHostJobEnvironmentIDInternal,
	})
	require.NoError(t, err)
	assert.Equal(t, dockerHostJobEnvironmentIDInternal, run.EnvironmentID)
	assert.NotEmpty(t, run.ID)
}

// TestJobService_RunReadPaths_StayLocalForDockerHost covers the run history, cancellation and retry
// paths: they must read and mutate this manager's own queue for a docker-host environment instead
// of asking an agent that does not exist.
func TestJobService_RunReadPaths_StayLocalForDockerHost(t *testing.T) {
	service, coordinator := newDockerHostJobFixtureInternal(t)
	ctx := t.Context()

	run, err := coordinator.Submit(ctx, scheduler.Request{
		RunID:         "2f0f0a5c-3a58-4a1f-9d11-2f9a30d1c103",
		JobID:         testJobIDInternal,
		EnvironmentID: dockerHostJobEnvironmentIDInternal,
		Trigger:       "scheduled",
	})
	require.NoError(t, err)

	list, err := service.ListRuns(ctx, dockerHostJobEnvironmentIDInternal, testJobIDInternal, 1, 10)
	require.NoError(t, err)
	require.Equal(t, 1, list.Total)
	require.Equal(t, run.ID, list.Runs[0].ID)

	got, err := service.GetRun(ctx, dockerHostJobEnvironmentIDInternal, testJobIDInternal, run.ID)
	require.NoError(t, err)
	assert.Equal(t, run.ID, got.ID)

	canceled, err := service.CancelRun(ctx, dockerHostJobEnvironmentIDInternal, testJobIDInternal, run.ID)
	require.NoError(t, err)
	assert.Equal(t, run.ID, canceled.ID)

	t.Run("agent environment reads agent history, not this manager's queue", func(t *testing.T) {
		agentList, listErr := service.ListRuns(ctx, agentJobEnvironmentIDInternal, testJobIDInternal, 1, 10)
		require.NoError(t, listErr)
		assert.Zero(t, agentList.Total, "docker-host runs must not be reported through an agent environment")
	})
}

// TestJobService_ForgetEnvironmentJobs_ClearsImagePollingSchedule is the counterpart of the
// per-environment image-polling schedules: when a docker-host environment is disabled or deleted its
// watcher checkpoint must go with it. The watcher owns no scheduler registry entry, so clearing only
// the cron job IDs would leave that environment polling its daemon forever.
func TestJobService_ForgetEnvironmentJobs_ClearsImagePollingSchedule(t *testing.T) {
	service, _ := newDockerHostJobFixtureInternal(t)
	schedulerFake := newFakeJobSchedulerInternal(environmentCronJobIDsInternal()...)
	service.scheduler = schedulerFake
	refreshes := 0
	service.SetBusWatcherScheduleRefresh(func() { refreshes++ })

	require.NoError(t, service.ForgetEnvironmentJobs(t.Context(), dockerHostJobEnvironmentIDInternal))
	assert.Contains(t, schedulerFake.cleared, dockerHostJobEnvironmentIDInternal+"|image-polling")
	for _, jobID := range environmentCronJobIDsInternal() {
		assert.Contains(t, schedulerFake.cleared, dockerHostJobEnvironmentIDInternal+"|"+jobID)
	}
	// The watcher also holds a live event subscription for this environment, so it must be asked to
	// reconcile — otherwise a late event would still admit a scan into a namespace just cleared.
	assert.Equal(t, 1, refreshes)

	t.Run("the manager's own schedules are never cleared", func(t *testing.T) {
		schedulerFake.cleared = nil
		refreshes = 0
		require.NoError(t, service.ForgetEnvironmentJobs(t.Context(), environment.LocalEnvironmentID))
		assert.Empty(t, schedulerFake.cleared)
		assert.Zero(t, refreshes)
	})
}

// TestJobService_SyncEnvironmentJobsRefreshesBusWatcherSchedules covers the other half of the
// lifecycle: a docker-host environment created or re-enabled while the process runs must start
// polling immediately, and the watcher owns those records, so the job service has to ask it to
// install them after the cron jobs are in place.
func TestJobService_SyncEnvironmentJobsRefreshesBusWatcherSchedules(t *testing.T) {
	service, _ := newDockerHostJobFixtureInternal(t)
	schedulerFake := newFakeJobSchedulerInternal(environmentCronJobIDsInternal()...)
	service.scheduler = schedulerFake
	refreshes := 0
	service.SetBusWatcherScheduleRefresh(func() {
		refreshes++
		assert.Len(t, schedulerFake.rescheduled, len(environmentCronJobIDsInternal()), "cron jobs must be installed before the watcher refresh")
	})

	require.NoError(t, service.SyncEnvironmentJobs(t.Context(), dockerHostJobEnvironmentIDInternal))
	assert.Equal(t, 1, refreshes)

	t.Run("agent environments do not touch the watcher", func(t *testing.T) {
		refreshes = 0
		require.NoError(t, service.SyncEnvironmentJobs(t.Context(), "agent-environment-without-target"))
		assert.Zero(t, refreshes)
	})

	t.Run("the manager's own schedules do not touch the watcher", func(t *testing.T) {
		refreshes = 0
		require.NoError(t, service.SyncEnvironmentJobs(t.Context(), environment.LocalEnvironmentID))
		assert.Zero(t, refreshes)
	})
}

package scheduler

import (
	"context"
	"time"
)

// JobRuntimeState describes the schedule currently installed in a scheduler.
// It intentionally exposes only read-only state needed by job-management APIs.
type JobRuntimeState struct {
	Schedule  string
	NextRun   *time.Time
	Scheduled bool
}

type Job interface {
	Name() string
	Schedule(ctx context.Context) string
	Run(ctx context.Context) (Outcome, error)
}

// ConditionalJob allows a job to opt out of cron registration when it is disabled.
// Jobs that do not implement this interface are always scheduled.
type ConditionalJob interface {
	ShouldSchedule(ctx context.Context) bool
}

// EnvironmentScopedJob marks a job that belongs to exactly one environment (for example one
// volume-backup job per policy or one health-check job per environment). Its schedule is installed
// in that environment's namespace, so the runs it produces carry the environment and execute
// against that environment's own Docker daemon. An empty ID means the manager's namespace.
type EnvironmentScopedJob interface {
	EnvironmentID() string
}

// EnvironmentScopeSetter is implemented by jobs whose owning environment is recorded when the job
// is registered, such as a volume-backup job registered for one policy row.
type EnvironmentScopeSetter interface {
	SetEnvironmentScope(environmentID string)
}

// BusWatcher is a continuous event consumer owned by the application scheduler lifecycle.
type BusWatcher interface {
	Name() string
	Start(ctx context.Context) error
	RunNow(ctx context.Context) error
}

// StoppableBusWatcher lets a continuous watcher perform explicit shutdown work
// before its actor runner is joined.
type StoppableBusWatcher interface {
	BusWatcher
	Stop(ctx context.Context) error
}

// DynamicScheduler owns jobs registered and removed while the application is running.
type DynamicScheduler interface {
	Submit(ctx context.Context, request Request) (Run, error)
	AddJob(ctx context.Context, job Job) error
	RemoveJob(ctx context.Context, name string)
	HasJob(name string) bool
}

// JobController exposes scheduler operations used by job-management services.
type JobController interface {
	GetJob(jobID string) (Job, bool)
	GetJobRuntimeState(jobID string) (JobRuntimeState, bool)
	// GetJobRuntimeStateFor reports the schedule installed for one environment. Environments keep
	// separate schedule records in the shared coordinator, so callers serving a specific
	// environment must use this instead of GetJobRuntimeState, which reports the manager's.
	GetJobRuntimeStateFor(ctx context.Context, environmentID, jobID string) (JobRuntimeState, bool)
	// ClearJobScheduleFor unschedules a job for one environment only, leaving it registered for the
	// others. Used when a docker-host environment is disabled or deleted.
	ClearJobScheduleFor(ctx context.Context, environmentID, jobID string) error
	RescheduleJob(ctx context.Context, job Job) error
	RunBusWatcherNow(ctx context.Context, watcherID string) error
}

// WorkerController exposes continuous-worker health and recovery operations.
type WorkerController interface {
	RestartWatcher(ctx context.Context, watcherID string) error
	WatcherHealth(watcherID string) (WorkerHealth, bool)
}

// JobScheduler is the shared public contract for the actor-owned scheduler.
// Concrete cron and actor state remains private to the backend implementation.
type JobScheduler interface {
	ListRegisteredJobs() []Job
	WorkerController
	DynamicScheduler
	JobController

	RegisterJob(job Job) error
	RegisterBusWatcher(watcher BusWatcher, canRunManually bool) error
	StartScheduler(ctx context.Context) error
	GetLocation() *time.Location
	Stop(ctx context.Context) error
}

// GenericJob is a reusable Job built from closures. It lets a service register a
// per-entity dynamic job (e.g. one per GitOps sync or one per environment) without
// importing the scheduler package: the service constructs a GenericJob and hands it
// to the scheduler through the types/scheduler.Job interface.
//
// JobName must be unique per logical job; per-entity jobs use a "<subsystem>:<entityID>"
// scheme (e.g. "gitops-sync:abc123"). ShouldRunFn is optional — when nil the job is
// always scheduled, matching the behavior of a Job that does not implement
// ConditionalJob.
type GenericJob struct {
	JobName     string
	ScheduleFn  func(ctx context.Context) string
	RunFn       func(ctx context.Context) (Outcome, error)
	ReconcileFn func(ctx context.Context, previous Run) (Outcome, error)
	ShouldRunFn func(ctx context.Context) bool

	// EnvironmentScope is the environment the job belongs to, or empty for the
	// manager's own namespace. Schedules of scoped jobs are installed per
	// environment so their runs execute against that environment's Docker daemon.
	EnvironmentScope string
}

func (g *GenericJob) Name() string { return g.JobName }

// EnvironmentID satisfies EnvironmentScopedJob.
func (g *GenericJob) EnvironmentID() string { return g.EnvironmentScope }

// SetEnvironmentScope satisfies EnvironmentScopeSetter.
func (g *GenericJob) SetEnvironmentScope(environmentID string) { g.EnvironmentScope = environmentID }

func (g *GenericJob) Schedule(ctx context.Context) string { return g.ScheduleFn(ctx) }

func (g *GenericJob) Run(ctx context.Context) (Outcome, error) { return g.RunFn(ctx) }

// ShouldSchedule satisfies ConditionalJob. A GenericJob without a ShouldRunFn is
// always scheduled; the scheduler treats a ConditionalJob returning false as "do
// not schedule", so nil must map to true rather than a nil-func panic.
func (g *GenericJob) ShouldSchedule(ctx context.Context) bool {
	if g.ShouldRunFn == nil {
		return true
	}
	return g.ShouldRunFn(ctx)
}

func (g *GenericJob) Reconcile(ctx context.Context, previous Run) (Outcome, error) {
	if g.ReconcileFn != nil {
		return g.ReconcileFn(ctx, previous)
	}
	return Outcome{Status: Failed, Message: "The interrupted operation has no confirmed completion", Targets: previous.Outcome.Targets}, nil
}

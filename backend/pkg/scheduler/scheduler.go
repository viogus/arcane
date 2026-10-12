package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	st "github.com/getarcaneapp/arcane/types/v2/scheduler"

	"github.com/getarcaneapp/arcane/backend/v2/pkg/libarcane/envcontext"
	"github.com/getarcaneapp/arcane/backend/v2/pkg/scheduler/runs"
	scheduleutil "github.com/getarcaneapp/arcane/backend/v2/pkg/scheduler/schedule"
)

var (
	cronScheduleParser             = scheduleutil.Parser()
	errJobSchedulerStoppedInternal = errors.New("job scheduler stopped")
)

type watcherRegistrationInternal struct {
	watcher st.BusWatcher
	done    chan struct{}
}
type jobSchedulerInternal struct {
	mu          sync.RWMutex
	jobsByID    map[string]st.Job
	watchers    map[string]st.BusWatcher
	allWatchers map[string]watcherRegistrationInternal
	supervisors map[string]*watcherSupervisorInternal
	context     context.Context
	cancel      context.CancelFunc
	location    *time.Location
	coordinator *runs.Coordinator
	stopping    bool
	started     bool
}

func NewJobScheduler(ctx context.Context, coordinator *runs.Coordinator, location *time.Location) (st.JobScheduler, error) {
	if ctx == nil || coordinator == nil {
		return nil, errors.New("scheduler dependencies unavailable")
	}
	if location == nil {
		location = time.UTC
	}
	lifetime, cancel := context.WithCancel(ctx)
	return &jobSchedulerInternal{
			context:     lifetime,
			cancel:      cancel,
			location:    location,
			coordinator: coordinator,
			jobsByID:    map[string]st.Job{},
			watchers:    map[string]st.BusWatcher{},
			allWatchers: map[string]watcherRegistrationInternal{},
			supervisors: map[string]*watcherSupervisorInternal{},
		},
		nil
}

func (js *jobSchedulerInternal) RegisterJob(job st.Job) error {
	if job == nil {
		return errors.New("scheduler job unavailable")
	}
	js.mu.Lock()
	defer js.mu.Unlock()
	if js.stopping {
		return errJobSchedulerStoppedInternal
	}
	js.jobsByID[job.Name()] = job
	return nil
}

func (js *jobSchedulerInternal) RegisterBusWatcher(watcher st.BusWatcher, manual bool) error {
	js.mu.Lock()
	defer js.mu.Unlock()
	if js.stopping {
		return errJobSchedulerStoppedInternal
	}
	if _, found := js.allWatchers[watcher.Name()]; found {
		return errors.New("watcher already registered")
	}
	supervisor := newWatcherSupervisorInternal(watcher)
	done := make(chan struct{})
	js.supervisors[watcher.Name()] = supervisor
	js.allWatchers[watcher.Name()] = watcherRegistrationInternal{watcher: watcher, done: done}
	if manual {
		js.watchers[watcher.Name()] = watcher
	}
	go func() {
		defer close(done)
		if err := supervisor.runInternal(js.context); err != nil && js.context.Err() == nil {
			slog.ErrorContext(js.context, "Watcher stopped", "name", watcher.Name(), "error", err)
		}
	}()
	return nil
}

func (js *jobSchedulerInternal) RunBusWatcherNow(ctx context.Context, id string) error {
	js.mu.RLock()
	watcher, found := js.watchers[id]
	js.mu.RUnlock()
	if !found {
		return errors.New("watcher is not manually runnable")
	}
	return watcher.RunNow(ctx)
}

func (js *jobSchedulerInternal) GetJob(id string) (st.Job, bool) {
	js.mu.RLock()
	defer js.mu.RUnlock()
	job, ok := js.jobsByID[id]
	return job, ok
}
func (js *jobSchedulerInternal) HasJob(id string) bool { _, ok := js.GetJob(id); return ok }
func (js *jobSchedulerInternal) GetJobRuntimeState(id string) (st.JobRuntimeState, bool) {
	return js.getJobRuntimeStateInternal(js.context, "0", id)
}

// GetJobRuntimeStateFor reports the schedule installed for one environment's namespace.
func (js *jobSchedulerInternal) GetJobRuntimeStateFor(ctx context.Context, environmentID, jobID string) (st.JobRuntimeState, bool) {
	return js.getJobRuntimeStateInternal(ctx, environmentID, jobID)
}

func (js *jobSchedulerInternal) getJobRuntimeStateInternal(ctx context.Context, environmentID, jobID string) (st.JobRuntimeState, bool) {
	if !js.HasJob(jobID) {
		return st.JobRuntimeState{}, false
	}
	if js.coordinator == nil {
		return st.JobRuntimeState{}, false
	}
	record, err := js.coordinator.ScheduleStateFor(ctx, environmentID, jobID)
	if err != nil {
		return st.JobRuntimeState{}, true
	}
	state := st.JobRuntimeState{Schedule: record.Schedule, Scheduled: !record.NextRun.IsZero()}
	if state.Scheduled {
		state.NextRun = new(record.NextRun)
	}
	return state, true
}

func (js *jobSchedulerInternal) StartScheduler(ctx context.Context) error {
	js.mu.Lock()
	if js.stopping {
		js.mu.Unlock()
		return errJobSchedulerStoppedInternal
	}
	js.started = true
	js.mu.Unlock()
	records, err := js.coordinator.Records(ctx)
	if err != nil {
		return err
	}
	for _, record := range records {
		js.mu.RLock()
		_, watching := js.watchers[record.JobID]
		js.mu.RUnlock()
		if record.Schedule != "" && !js.HasJob(record.JobID) && !watching {
			if checkpointErr := js.coordinator.CheckpointFor(ctx, record.EnvironmentID, record.JobID, "", time.Time{}); checkpointErr != nil {
				return checkpointErr
			}
		}
	}
	var schedulingErr error
	for _, job := range js.ListRegisteredJobs() {
		if installErr := js.installInternal(ctx, job); installErr != nil {
			schedulingErr = errors.Join(schedulingErr, fmt.Errorf("%s: %w", "schedule "+job.Name(), installErr))
		}
	}
	return schedulingErr
}

func (js *jobSchedulerInternal) installInternal(ctx context.Context, job st.Job) error {
	environmentID := js.environmentForJobInternal(ctx, job)
	// Keep any daemon endpoint already carried by ctx so schedule evaluation still sees the
	// environment's own settings without losing the target a docker-host request arrived with.
	target, _ := envcontext.TargetFromContext(ctx)
	target.EnvironmentID = environmentID
	evalCtx := envcontext.WithTarget(ctx, target)
	next := time.Time{}
	schedule := job.Schedule(evalCtx)
	should := true
	if conditional, ok := job.(st.ConditionalJob); ok {
		should = conditional.ShouldSchedule(evalCtx)
	}
	if should {
		parsed, err := cronScheduleParser.Parse(schedule)
		if err != nil {
			return err
		}
		next = parsed.Next(time.Now().In(js.location))
	}
	if !should {
		schedule = ""
	}
	return js.coordinator.CheckpointFor(evalCtx, environmentID, job.Name(), schedule, next)
}

// environmentForJobInternal names the scheduling namespace a job's schedule belongs to: the job's
// own scope when it declares one, otherwise the environment carried by ctx, otherwise the manager.
func (js *jobSchedulerInternal) environmentForJobInternal(ctx context.Context, job st.Job) string {
	if scoped, ok := job.(st.EnvironmentScopedJob); ok {
		if environmentID := scoped.EnvironmentID(); environmentID != "" {
			return environmentID
		}
	}
	if environmentID := envcontext.EnvironmentIDFromContext(ctx); environmentID != "" {
		return environmentID
	}
	return "0"
}

func (js *jobSchedulerInternal) AddJob(ctx context.Context, job st.Job) error {
	if err := js.RegisterJob(job); err != nil {
		return err
	}
	js.mu.RLock()
	started := js.started
	js.mu.RUnlock()
	if !started {
		return nil
	}
	return js.installInternal(ctx, job)
}

func (js *jobSchedulerInternal) RescheduleJob(ctx context.Context, job st.Job) error {
	return js.AddJob(ctx, job)
}

// ClearJobScheduleFor unschedules one environment's record for a job while leaving the job
// registered for every other environment, as when a docker-host environment is disabled or deleted.
func (js *jobSchedulerInternal) ClearJobScheduleFor(ctx context.Context, environmentID, jobID string) error {
	if environmentID == "" {
		environmentID = "0"
	}
	js.mu.RLock()
	_, registered := js.jobsByID[jobID]
	// Bus watchers (image-polling) hold no registry entry yet still checkpoint one schedule per
	// environment, so forgetting an environment must clear those records too.
	_, watched := js.allWatchers[jobID]
	started := js.started
	js.mu.RUnlock()
	if (!registered && !watched) || !started {
		return nil
	}
	return js.coordinator.CheckpointFor(ctx, environmentID, jobID, "", time.Time{})
}

func (js *jobSchedulerInternal) RemoveJob(ctx context.Context, id string) {
	js.mu.Lock()
	job := js.jobsByID[id]
	delete(js.jobsByID, id)
	started := js.started
	js.mu.Unlock()
	if started {
		environmentID := "0"
		if job != nil {
			environmentID = js.environmentForJobInternal(ctx, job)
		}
		if err := js.coordinator.CheckpointFor(ctx, environmentID, id, "", time.Time{}); err != nil {
			slog.ErrorContext(ctx, "Unschedule failed", "job", id, "error", err)
		}
	}
}
func (js *jobSchedulerInternal) GetLocation() *time.Location { return js.location }
func (js *jobSchedulerInternal) Stop(ctx context.Context) error {
	js.mu.Lock()
	js.stopping = true
	js.cancel()
	watchers := make([]watcherRegistrationInternal, 0, len(js.allWatchers))
	for _, w := range js.allWatchers {
		watchers = append(watchers, w)
	}
	js.mu.Unlock()
	var err error
	for _, w := range watchers {
		select {
		case <-w.done:
		case <-ctx.Done():
			return errors.Join(err, ctx.Err())
		}
		if stop, ok := w.watcher.(st.StoppableBusWatcher); ok {
			err = errors.Join(err, stop.Stop(ctx))
		}
	}
	return err
}

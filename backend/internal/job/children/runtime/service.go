// Package runtime projects durable run history and scheduler runtime state
// onto job statuses.
package runtime

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/getarcaneapp/arcane/types/v2/jobschedule"
	"github.com/getarcaneapp/arcane/types/v2/scheduler"
	"github.com/getarcaneapp/arcane/types/v2/user"

	"github.com/getarcaneapp/arcane/backend/v2/pkg/libarcane/envcontext"
	"github.com/getarcaneapp/arcane/backend/v2/pkg/scheduler/runs"
)

// ApplyStatuses merges durable runs for one environment into jobs. Environments this process
// serves itself (the local manager and docker-host environments) also gain dynamic children and
// scheduler worker health, because their schedules live in this process's scheduler.
func ApplyStatuses(ctx context.Context, coordinator *runs.Coordinator, controller scheduler.JobController, environmentID string, jobs *[]jobschedule.JobStatus) error {
	runsByJob := make(map[string][]scheduler.Run)
	if coordinator != nil {
		records, err := coordinator.Records(ctx)
		if err != nil {
			return fmt.Errorf("load durable job status: %w", err)
		}
		for _, record := range records {
			if record.EnvironmentID == environmentID {
				runsByJob[record.JobID] = append(runsByJob[record.JobID], record.Runs...)
			}
		}
	}
	jobScheduler, hasScheduler := controller.(scheduler.JobScheduler)
	servedInProcess := environmentID == "0" || envcontext.IsDockerHostTarget(ctx)
	if servedInProcess && hasScheduler {
		appendDynamicJobStatuses(ctx, jobScheduler, environmentID, jobs)
	}

	for index := range *jobs {
		status := &(*jobs)[index]
		ApplyRunStatus(status, runsByJob[status.ID])
		if servedInProcess && hasScheduler {
			setRuntimeHealth(ctx, jobScheduler, environmentID, status)
		}
		for childIndex := range status.Children {
			child := &status.Children[childIndex]
			ApplyRunStatus(child, runsByJob[child.ID])
		}
	}
	return nil
}

func setRuntimeHealth(ctx context.Context, jobScheduler scheduler.JobScheduler, environmentID string, status *jobschedule.JobStatus) {
	if health, found := jobScheduler.WatcherHealth(status.ID); found {
		status.WorkerHealth = &health
		return
	}
	registered, found := jobScheduler.GetJob(status.ID)
	if !found {
		return
	}
	if conditional, ok := registered.(scheduler.ConditionalJob); ok && !conditional.ShouldSchedule(ctx) {
		return
	}
	state, found := jobScheduler.GetJobRuntimeStateFor(ctx, environmentID, status.ID)
	if !found || !state.Scheduled {
		status.NextRun = nil
		status.WorkerHealth = &scheduler.WorkerHealth{Status: "needs_attention", LastError: "Job has no installed schedule", UpdatedAt: time.Now().UTC()}
	}
}

func currentRunPriority(status scheduler.RunStatus) int {
	switch status {
	case scheduler.Running:
		return 0
	case scheduler.Queued, scheduler.Waiting, scheduler.Retrying:
		return 1
	case scheduler.NeedsAttention, scheduler.Succeeded, scheduler.Partial, scheduler.Skipped, scheduler.Failed, scheduler.Canceled:
		return 2
	default:
		return 2
	}
}

func preferCurrentRun(candidate, current scheduler.Run) bool {
	if candidate.ID == current.ID {
		return candidate.UpdatedAt.After(current.UpdatedAt)
	}
	candidatePriority := currentRunPriority(candidate.Status)
	currentPriority := currentRunPriority(current.Status)
	if candidatePriority != currentPriority {
		return candidatePriority < currentPriority
	}
	if candidate.CreatedAt.Equal(current.CreatedAt) {
		return candidate.ID > current.ID
	}
	return candidate.CreatedAt.After(current.CreatedAt)
}

func runHasError(status scheduler.RunStatus) bool {
	switch status {
	case scheduler.Failed, scheduler.Partial, scheduler.NeedsAttention, scheduler.Retrying:
		return true
	case scheduler.Queued, scheduler.Running, scheduler.Waiting, scheduler.Succeeded, scheduler.Skipped, scheduler.Canceled:
		return false
	default:
		return false
	}
}

func updateLastSuccess(status *jobschedule.JobStatus, run scheduler.Run) {
	if run.Status != scheduler.Succeeded {
		return
	}
	succeededAt := runExecutionTime(run)
	if run.FinishedAt != nil {
		succeededAt = *run.FinishedAt
	}
	if status.LastSuccess == nil || succeededAt.After(*status.LastSuccess) {
		status.LastSuccess = new(succeededAt)
	}
}

func runExecutionTime(run scheduler.Run) time.Time {
	startedAt := run.CreatedAt
	if run.StartedAt != nil && run.StartedAt.After(startedAt) {
		startedAt = *run.StartedAt
	}
	// Manager delivery attempts poll the agent; they are not job executions.
	if run.EnvironmentID == "0" || !run.RemoteAccepted {
		for _, attempt := range run.Attempts {
			if attempt.StartedAt.After(startedAt) {
				startedAt = attempt.StartedAt
			}
		}
	}
	return startedAt
}

func updateLatestRun(status *jobschedule.JobStatus, run scheduler.Run) {
	if !run.Status.Terminal() && run.Status != scheduler.NeedsAttention {
		if status.CurrentRun == nil || preferCurrentRun(run, *status.CurrentRun) {
			status.CurrentRun = new(run)
		}
		return
	}
	if status.LastRun == nil || runExecutionTime(run).After(runExecutionTime(*status.LastRun)) ||
		(runExecutionTime(run).Equal(runExecutionTime(*status.LastRun)) && run.ID > status.LastRun.ID) {
		status.LastRun = new(run)
	}
}

func ProjectRunOutcome(run scheduler.Run) scheduler.Run {
	// Delivery reconciliation must not hide a failure already reported by the agent.
	if run.RemoteOutcome != nil && run.RemoteOutcome.Status == scheduler.NeedsAttention && !run.RemoteRetryRequested {
		run.Status = scheduler.Failed
		run.Outcome = *run.RemoteOutcome
	}
	automaticResolution := run.Status == scheduler.Canceled && run.Resolution != nil && run.Resolution.ResolvedBy == user.SystemUser.Username
	if run.Status == scheduler.NeedsAttention || automaticResolution {
		run.Status = scheduler.Failed
	}
	if run.Status == scheduler.Failed {
		run.Outcome.Status = scheduler.Failed
	}
	return run
}

func ApplyRunStatus(status *jobschedule.JobStatus, jobRuns []scheduler.Run) {
	latest := make(map[string]scheduler.Run, len(jobRuns)+2)
	for _, run := range []*scheduler.Run{status.CurrentRun, status.LastRun} {
		if run != nil {
			if previous, found := latest[run.ID]; !found || !run.UpdatedAt.Before(previous.UpdatedAt) {
				latest[run.ID] = *run
			}
		}
	}
	for _, run := range jobRuns {
		if previous, found := latest[run.ID]; !found || !run.UpdatedAt.Before(previous.UpdatedAt) {
			latest[run.ID] = run
		}
	}
	status.CurrentRun = nil
	status.LastRun = nil
	status.LastError = ""
	for _, run := range latest {
		run = ProjectRunOutcome(run)
		updateLatestRun(status, run)
		updateLastSuccess(status, run)
	}
	run := status.CurrentRun
	if last := status.LastRun; last != nil {
		if run == nil || runExecutionTime(*last).After(runExecutionTime(*run)) {
			run = last
		}
	}
	if run != nil && runHasError(run.Status) {
		status.LastError = run.Outcome.Message
	}
}

func dynamicJobStatus(ctx context.Context, jobScheduler scheduler.JobScheduler, environmentID string, job scheduler.Job, category string, managerOnly bool) jobschedule.JobStatus {
	child := jobschedule.JobStatus{
		ID:             job.Name(),
		Name:           job.Name(),
		Category:       category,
		Schedule:       job.Schedule(ctx),
		Enabled:        true,
		ManagerOnly:    managerOnly,
		CanRunManually: true,
		Prerequisites:  []jobschedule.JobPrerequisite{},
	}
	if conditional, ok := job.(scheduler.ConditionalJob); ok {
		child.Enabled = conditional.ShouldSchedule(ctx)
	}
	if state, ok := jobScheduler.GetJobRuntimeStateFor(ctx, environmentID, child.ID); ok {
		child.Schedule = state.Schedule
		child.NextRun = state.NextRun
	}
	return child
}

func dynamicJobCategory(parentID string) (category string, managerOnly, supported bool) {
	switch parentID {
	case "environment-health":
		return "monitoring", true, true
	case "gitops-sync":
		return "sync", false, true
	case "system-backup":
		return "maintenance", true, true
	case "volume-backup":
		return "maintenance", false, true
	default:
		return "", false, false
	}
}

func appendDynamicJobStatuses(ctx context.Context, jobScheduler scheduler.JobScheduler, environmentID string, jobs *[]jobschedule.JobStatus) {
	parentIndices := make(map[string]int, len(*jobs))
	for index, status := range *jobs {
		parentIndices[status.ID] = index
	}
	registered := jobScheduler.ListRegisteredJobs()
	slices.SortFunc(registered, func(left, right scheduler.Job) int {
		return strings.Compare(left.Name(), right.Name())
	})
	for _, registeredJob := range registered {
		parentID, _, dynamic := strings.Cut(registeredJob.Name(), ":")
		if !dynamic {
			continue
		}
		if parentID == "environment-health" {
			if registeredJob.Name() == "environment-health:"+environmentID {
				if index, exists := parentIndices[parentID]; exists {
					health := dynamicJobStatus(ctx, jobScheduler, environmentID, registeredJob, "monitoring", true)
					health.Name = (*jobs)[index].Name
					(*jobs)[index] = health
				}
			}
			continue
		}
		if !jobBelongsToEnvironmentInternal(registeredJob, environmentID) {
			continue
		}
		category, managerOnly, supported := dynamicJobCategory(parentID)
		if !supported {
			continue
		}
		parentIndex, exists := parentIndices[parentID]
		if !exists {
			parentIndex = len(*jobs)
			parentIndices[parentID] = parentIndex
			*jobs = append(*jobs, jobschedule.JobStatus{
				ID:            parentID,
				Name:          parentID,
				Category:      category,
				Enabled:       true,
				ManagerOnly:   managerOnly,
				Prerequisites: []jobschedule.JobPrerequisite{},
			})
		}
		parent := &(*jobs)[parentIndex]
		duplicate := slices.ContainsFunc(parent.Children, func(child jobschedule.JobStatus) bool {
			return child.ID == registeredJob.Name()
		})
		if duplicate {
			continue
		}
		parent.Children = append(parent.Children, dynamicJobStatus(ctx, jobScheduler, environmentID, registeredJob, category, managerOnly))
	}
}

// jobBelongsToEnvironmentInternal reports whether a dynamic job's schedule namespace belongs to
// the environment whose job list is being built. Jobs with no declared scope (and manager-scoped
// jobs) belong to the manager's own catalog, which every environment surfaces.
func jobBelongsToEnvironmentInternal(job scheduler.Job, environmentID string) bool {
	scoped, ok := job.(scheduler.EnvironmentScopedJob)
	if !ok {
		return true
	}
	scope := scoped.EnvironmentID()
	return scope == "" || scope == "0" || scope == environmentID
}

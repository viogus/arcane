package job

import (
	"cmp"
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/getarcaneapp/arcane/types/v2/base"
	"github.com/getarcaneapp/arcane/types/v2/jobschedule"
	"github.com/getarcaneapp/arcane/types/v2/scheduler"
	"go.getarcane.app/kit/pkg"

	"github.com/getarcaneapp/arcane/backend/v2/internal/middleware"
	"github.com/getarcaneapp/arcane/backend/v2/pkg/libarcane/envcontext"
	"github.com/getarcaneapp/arcane/backend/v2/pkg/scheduler/runs"
	"github.com/getarcaneapp/arcane/backend/v2/pkg/utils/handlerutil"
)

type GetJobSchedulesInput struct {
	ID string `path:"id" doc:"Environment ID"`
}

type GetJobSchedulesOutput struct {
	Body jobschedule.Config
}

type UpdateJobSchedulesInput struct {
	ID   string             `path:"id" doc:"Environment ID"`
	Body jobschedule.Update `doc:"Job schedule update data"`
}

type ListJobsInput struct {
	ID string `path:"id" doc:"Environment ID"`
}

type GetJobsOutput struct {
	Body jobschedule.JobListResponse
}

type RunJobInput struct {
	ID        string                      `path:"id" doc:"Environment ID"`
	JobID     string                      `path:"jobId" minLength:"1" doc:"Job ID to run"`
	RequestID string                      `header:"Idempotency-Key"`
	Body      *jobschedule.SubmitRunInput `required:"false"`
}

type RunJobOutput struct {
	Body jobschedule.JobRunResponse
}

type JobSchedulesHandler struct {
	jobService      *JobService
	proxyRemoteJSON handlerutil.RemoteJSONProxy
}

func (h *JobSchedulesHandler) ListJobs(ctx context.Context, input *ListJobsInput) (*GetJobsOutput, error) {
	var jobs *jobschedule.JobListResponse
	var err error
	// A docker-host environment is served by this process: it has no agent to proxy to, so its
	// schedules and runs are the manager's own.
	if input.ID != "0" && !envcontext.IsDockerHostTarget(ctx) {
		jobs, err = h.jobService.ListRemoteJobs(ctx, input.ID)
	} else {
		jobs, err = h.jobService.ListJobsForEnvironment(ctx, input.ID)
	}
	if err != nil {
		return nil, jobHTTPError(err)
	}
	return &GetJobsOutput{Body: *jobs}, nil
}

func (h *JobSchedulesHandler) RunJob(ctx context.Context, input *RunJobInput) (*RunJobOutput, error) {
	if kit.FromPtr(input.Body).RunID != "" && input.RequestID != "" && input.RequestID != kit.FromPtr(input.Body).RunID {
		return nil, huma.Error400BadRequest("Conflicting run IDs")
	}
	userID, _ := middleware.GetUserIDFromContext(ctx)
	keyID, _ := ctx.Value(middleware.ContextKeyApiKeyID).(string)
	run, err := h.jobService.Submit(ctx, scheduler.Request{
		RunID:            cmp.Or(kit.FromPtr(input.Body).RunID, input.RequestID),
		JobID:            input.JobID,
		EnvironmentID:    input.ID,
		Trigger:          kit.Ternary(h.jobService.cfg.AgentMode && userID == "agent", "remote", "manual"),
		RequestedWithKey: keyID,
		RequestedBy:      kit.Ternary(h.jobService.cfg.AgentMode && userID == "agent", "", userID),
	})
	if err != nil {
		return nil, jobHTTPError(err)
	}
	return &RunJobOutput{
		Body: jobschedule.JobRunResponse{
			Success: true,
			Message: "Job accepted",
			RunID:   run.ID,
			Status:  run.Status,
		},
	}, nil
}

func (h *JobSchedulesHandler) Get(ctx context.Context, input *GetJobSchedulesInput) (*GetJobSchedulesOutput, error) {
	// A docker-host environment is served by this process: it has no agent to proxy to, so its
	// schedules and runs are the manager's own.
	if input.ID != "0" && !envcontext.IsDockerHostTarget(ctx) {
		cfg, err := h.proxyRemoteJSON.JSON[jobschedule.Config](ctx, input.ID, http.MethodGet, "/api/environments/0/job-schedules", nil)
		if err != nil {
			return nil, err
		}
		return &GetJobSchedulesOutput{Body: *cfg}, nil
	}

	cfg := h.jobService.GetJobSchedules(ctx)
	return &GetJobSchedulesOutput{Body: cfg}, nil
}

func (h *JobSchedulesHandler) Update(ctx context.Context, input *UpdateJobSchedulesInput) (*handlerutil.Out[jobschedule.Config], error) {
	// A docker-host environment is served by this process: it has no agent to proxy to, so its
	// schedules and runs are the manager's own.
	if input.ID != "0" && !envcontext.IsDockerHostTarget(ctx) {
		apiResp, err := h.proxyRemoteJSON.JSON[base.ApiResponse[jobschedule.Config]](ctx, input.ID, http.MethodPut, "/api/environments/0/job-schedules", input.Body)
		if err != nil {
			return nil, err
		}

		return &handlerutil.Out[jobschedule.Config]{Body: *apiResp}, nil
	}

	cfg, err := h.jobService.UpdateJobSchedules(ctx, input.Body)
	if err != nil {
		return nil, huma.Error400BadRequest(err.Error())
	}

	return &handlerutil.Out[jobschedule.Config]{
		Body: base.ApiResponse[jobschedule.Config]{
			Success: true,
			Data:    cfg,
		},
	}, nil
}

// ListRuns returns paginated history for the requested job and environment.
// Remote history combines agent runs with the manager's delivery records.
func (h *JobSchedulesHandler) ListRuns(ctx context.Context, input *jobschedule.ListRunsInput) (*jobschedule.ListRunsOutput, error) {
	result, err := h.jobService.ListRuns(ctx, input.ID, input.JobID, input.Page, input.Limit)
	if err != nil {
		return nil, jobHTTPError(err)
	}
	return &jobschedule.ListRunsOutput{Body: result}, nil
}

// GetRun returns a persisted run or receipt. For remote work, it queries the
// agent when the manager has no delivery record for the requested run.
func (h *JobSchedulesHandler) GetRun(ctx context.Context, input *jobschedule.RunInput) (*jobschedule.RunOutput, error) {
	result, err := h.jobService.GetRun(ctx, input.ID, input.JobID, input.RunID)
	if err != nil {
		return nil, jobHTTPError(err)
	}
	return &jobschedule.RunOutput{Body: result}, nil
}

// RetryRun requests an explicit retry while preserving the run ID and target
// progress. Eligibility and remote-outcome checks are enforced by the service.
func (h *JobSchedulesHandler) RetryRun(ctx context.Context, input *jobschedule.RunInput) (*jobschedule.RunOutput, error) {
	result, err := h.jobService.RetryRun(ctx, input.ID, input.JobID, input.RunID)
	if err != nil {
		return nil, jobHTTPError(err)
	}
	return &jobschedule.RunOutput{Body: result}, nil
}

// CancelRun cancels queued work only when it has never been attempted or sent
// remotely. Agent-owned cancellations are forwarded to the owning environment.
func (h *JobSchedulesHandler) CancelRun(ctx context.Context, input *jobschedule.RunInput) (*jobschedule.RunOutput, error) {
	result, err := h.jobService.CancelRun(ctx, input.ID, input.JobID, input.RunID)
	if err != nil {
		return nil, jobHTTPError(err)
	}
	return &jobschedule.RunOutput{Body: result}, nil
}

// AcknowledgeRun marks a terminal run's remote delivery settled so retention can
// compact its history. Nonterminal runs cannot be acknowledged.
func (h *JobSchedulesHandler) AcknowledgeRun(ctx context.Context, input *jobschedule.RunInput) (*jobschedule.RunOutput, error) {
	result, err := h.jobService.GetRun(ctx, input.ID, input.JobID, input.RunID)
	if err != nil {
		return nil, jobHTTPError(err)
	}
	err = h.jobService.runs.UpdateRun(ctx, result, func(current *scheduler.Run) error {
		if !current.Status.Terminal() {
			return errors.New("run is not terminal")
		}
		*current = scheduler.Run{ //exhaustruct:enforce
			ActivityEnvironmentID:   current.ActivityEnvironmentID,
			ActivityID:              current.ActivityID,
			Resolution:              current.Resolution,
			RequestedWithKey:        current.RequestedWithKey,
			ID:                      current.ID,
			JobID:                   current.JobID,
			EnvironmentID:           current.EnvironmentID,
			Trigger:                 current.Trigger,
			RequestedBy:             current.RequestedBy,
			Status:                  current.Status,
			CreatedAt:               current.CreatedAt,
			UpdatedAt:               time.Now().UTC(),
			StartedAt:               current.StartedAt,
			FinishedAt:              current.FinishedAt,
			NextAttempt:             current.NextAttempt,
			AttemptCount:            current.AttemptCount,
			Owner:                   current.Owner,
			Outcome:                 current.Outcome,
			Attempts:                current.Attempts,
			RemoteDeliveryAttempted: current.RemoteDeliveryAttempted,
			RemoteOutcome:           current.RemoteOutcome,
			RemoteRetryRequested:    current.RemoteRetryRequested,
			RemoteRetryAttempted:    current.RemoteRetryAttempted,
			RemoteAttemptCount:      current.RemoteAttemptCount,
			RemoteAccepted:          current.RemoteAccepted,
			RemoteSettled:           true,
			LastConfirmedAt:         current.LastConfirmedAt,
		}
		result = *current
		return nil
	})
	if err != nil {
		return nil, jobHTTPError(err)
	}
	return &jobschedule.RunOutput{Body: result}, nil
}

// RestartWorker requests a continuous watcher's restart on the owning runtime,
// forwarding remote requests to the agent. It does not enqueue a scheduled run.
func (h *JobSchedulesHandler) RestartWorker(ctx context.Context, input *jobschedule.JobInput) (*RunJobOutput, error) {
	// A docker-host environment is served by this process: it has no agent to proxy to, so its
	// schedules and runs are the manager's own.
	if input.ID != "0" && !envcontext.IsDockerHostTarget(ctx) {
		result, err := h.proxyRemoteJSON.JSON[jobschedule.JobRunResponse](ctx, input.ID, http.MethodPost, "/api/environments/0/jobs/"+input.JobID+"/restart", nil)
		if err != nil {
			return nil, err
		}
		return &RunJobOutput{Body: *result}, nil
	}
	workers, ok := h.jobService.scheduler.(scheduler.WorkerController)
	if !ok {
		return nil, huma.Error503ServiceUnavailable("Scheduler unavailable")
	}
	if err := workers.RestartWatcher(ctx, input.JobID); err != nil {
		return nil, jobHTTPError(err)
	}
	return &RunJobOutput{
		Body: jobschedule.JobRunResponse{
			Success: true,
			Message: "Worker restart requested",
		},
	}, nil
}

func jobHTTPError(err error) error {
	if _, ok := errors.AsType[huma.StatusError](err); ok {
		return err
	}
	if errors.Is(err, runs.ErrRunNotFound) {
		return huma.Error404NotFound("Job run not found")
	}
	if errors.Is(err, runs.ErrRunConflict) {
		return huma.Error409Conflict("Job run state changed")
	}
	return huma.Error400BadRequest(err.Error())
}

// ResolveRun records legacy operator review.
// TODO(v3): remove this deprecated mixed-version compatibility contract.
func (h *JobSchedulesHandler) ResolveRun(ctx context.Context, input *jobschedule.ResolveRunInput) (*jobschedule.RunOutput, error) {
	actor, _ := middleware.GetUserIDFromContext(ctx)
	permissions, _ := middleware.PermissionsFromContext(ctx)
	if input.Body != nil && input.Body.ResolvedBy != "" {
		if permissions == nil || !permissions.Sudo || !h.jobService.cfg.AgentMode {
			return nil, huma.Error403Forbidden("only authenticated manager transport may forward an operator identity")
		}
		actor = input.Body.ResolvedBy
	}
	run, err := h.jobService.ResolveRun(ctx, input.ID, input.JobID, input.RunID, actor)
	if err != nil {
		return nil, jobHTTPError(err)
	}
	return &jobschedule.RunOutput{Body: run}, nil
}

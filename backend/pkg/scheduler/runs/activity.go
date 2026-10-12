package runs

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/getarcaneapp/arcane/types/v2/scheduler"
	"github.com/italypaleale/francis/actor"
	kit "go.getarcane.app/kit/pkg"
)

// SetObserver connects activity projection before accepting work.
func (q *Coordinator) SetObserver(observer scheduler.RunObserver) { q.observer = observer }

func (q *Coordinator) associateActivityInternal(run *scheduler.Run) {
	if q.observer != nil && run.ActivityID == "" {
		run.ActivityID = q.observer.ActivityID(*run)
	}
	if run.ActivityID != "" {
		run.ActivityEnvironmentID = run.EnvironmentID
	}
}

func (q *Coordinator) observeRunInternal(ctx context.Context, run scheduler.Run) {
	if q.observer == nil {
		return
	}
	q.observerMu.Lock()
	defer q.observerMu.Unlock()
	q.syncActivityInternal(ctx, run)
	q.signalReconcileInternal(run.EnvironmentID, run.JobID)
}

func (q *Coordinator) syncActivityInternal(ctx context.Context, run scheduler.Run) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var state scheduler.CoordinatorState
	id := kit.SHA256Hex(run.EnvironmentID + "\x00" + run.JobID)
	if err := q.service.GetState(ctx, coordinatorTypeInternal, id, &state); err != nil {
		return err
	}
	version, pending := state.PendingActivitySync[run.ID]
	if !pending {
		return nil
	}
	current, err := q.Get(ctx, run.EnvironmentID, run.JobID, run.ID)
	if err == nil {
		err = q.observer.SyncRunActivity(ctx, current)
	}
	if err == nil {
		_, err = q.service.Invoke(ctx, coordinatorTypeInternal, id, "activity-synced", scheduler.ActivitySyncCommand{RunID: run.ID, UpdatedAt: version})
	}
	if err != nil {
		slog.ErrorContext(ctx, "Job activity synchronization failed; will retry", "runId", run.ID, "error", err)
	}
	return err
}

// forEachCoordinatorStateInternal streams every stored job coordinator state. It
// leaves decoding to the caller so each pass keeps its own error handling.
func (q *Coordinator) forEachCoordinatorStateInternal(ctx context.Context, visit func(actor.StateInfo) error) error {
	cursor := ""
	for {
		page, err := q.service.ListStates(ctx, coordinatorTypeInternal, &actor.ListStatesOpts{IncludeData: true, After: cursor, Limit: 100})
		if err != nil {
			return err
		}
		for _, entry := range page.States {
			if visitErr := visit(entry); visitErr != nil {
				return visitErr
			}
		}
		cursor = page.AfterID()
		if cursor == "" {
			return nil
		}
	}
}

// reconcileActorsInternal reconciles the coordinators whose state changed since the dispatcher's
// last pass: it projects their pending activities and flushes their records so schedule and retry
// alarms are restored. Reporting whether projections remain pending lets the dispatcher schedule
// a retry.
func (q *Coordinator) reconcileActorsInternal(ctx context.Context, ids []string) (bool, error) {
	pendingActivitySync := false
	for _, id := range ids {
		var state scheduler.CoordinatorState
		if err := q.service.GetState(ctx, coordinatorTypeInternal, id, &state); err != nil {
			if errors.Is(err, actor.ErrStateNotFound) {
				continue
			}
			return pendingActivitySync, err
		}
		if q.projectPendingActivityInternal(ctx, &state) {
			pendingActivitySync = true
		}
		if _, err := q.service.Invoke(ctx, coordinatorTypeInternal, id, "repair", nil); err != nil {
			return pendingActivitySync, err
		}
	}
	return pendingActivitySync, nil
}

// projectPendingActivityInternal projects every activity waiting on one coordinator state and
// reports whether any of them still needs another attempt.
func (q *Coordinator) projectPendingActivityInternal(ctx context.Context, state *scheduler.CoordinatorState) bool {
	if q.observer == nil {
		return false
	}
	pending := false
	for runID := range state.PendingActivitySync {
		if ctx.Err() != nil {
			return true
		}
		q.observerMu.Lock()
		syncErr := q.syncActivityInternal(ctx, scheduler.Run{ID: runID, EnvironmentID: state.Record.EnvironmentID, JobID: state.Record.JobID})
		q.observerMu.Unlock()
		if syncErr != nil {
			pending = true
		}
	}
	return pending
}

// reconcileInternal visits every coordinator state once, retrying pending
// activity projections and collecting the records that still need a repair
// flush. Reporting whether projections remain pending lets the dispatcher reuse
// one pass instead of re-decoding every state several times per tick.
func (q *Coordinator) reconcileInternal(ctx context.Context, all bool) (bool, []scheduler.QueueRecord, error) {
	pendingActivitySync := false
	records := []scheduler.QueueRecord{}
	err := q.forEachCoordinatorStateInternal(ctx, func(entry actor.StateInfo) error {
		var state scheduler.CoordinatorState
		if decodeErr := entry.Data.Decode(&state); decodeErr != nil {
			slog.ErrorContext(ctx, "Decode job coordinator state", "actorId", entry.ActorID, "error", decodeErr)
			pendingActivitySync = true // an unreadable state may still hide pending work
			return nil
		}
		if q.projectPendingActivityInternal(ctx, &state) {
			pendingActivitySync = true
		}
		if all || recordNeedsRepairInternal(&state.Record) {
			records = append(records, state.Record)
		}
		return nil
	})
	return pendingActivitySync, records, err
}

// retryActivitySyncInternal projects every activity still waiting on one and
// reports whether any of them remains pending.
func (q *Coordinator) retryActivitySyncInternal(ctx context.Context) bool {
	pendingActivitySync, _, err := q.reconcileInternal(ctx, false)
	if err != nil {
		slog.ErrorContext(ctx, "List pending job activities", "error", err)
		return true
	}
	return pendingActivitySync
}

// hasPendingActivitySyncInternal reports whether any coordinator state still
// carries an unprojected activity.
func (q *Coordinator) hasPendingActivitySyncInternal(ctx context.Context) bool {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	pending := false
	if err := q.forEachCoordinatorStateInternal(ctx, func(entry actor.StateInfo) error {
		var state scheduler.CoordinatorState
		if entry.Data.Decode(&state) != nil || len(state.PendingActivitySync) > 0 {
			pending = true
		}
		return nil
	}); err != nil {
		return true
	}
	return pending
}

package scheduler

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	imageupdatetypes "github.com/getarcaneapp/arcane/types/v2/imageupdate"
	schedulertypes "github.com/getarcaneapp/arcane/types/v2/scheduler"
	"github.com/moby/moby/api/types/events"
	"go.getarcane.app/streams/bus"

	"github.com/getarcaneapp/arcane/backend/v2/internal/common"
	"github.com/getarcaneapp/arcane/backend/v2/internal/config"
	"github.com/getarcaneapp/arcane/backend/v2/internal/docker"
	"github.com/getarcaneapp/arcane/backend/v2/internal/imageupdate"
	"github.com/getarcaneapp/arcane/backend/v2/internal/project"
	"github.com/getarcaneapp/arcane/backend/v2/internal/settings"
	"github.com/getarcaneapp/arcane/backend/v2/pkg/libarcane/envcontext"
	"github.com/getarcaneapp/arcane/backend/v2/pkg/scheduler/jobcontext"
	"github.com/getarcaneapp/arcane/backend/v2/pkg/scheduler/runs"
	"github.com/getarcaneapp/arcane/backend/v2/pkg/utils"
)

const (
	imageUpdateWatcherDebounce        = 2 * time.Second
	imageUpdateWatcherBackfillRetry   = 5 * time.Second
	imageUpdateWatcherDefaultSchedule = "0 0 * * * *"
)

type imageUpdateScannerInternal interface {
	RunImageCheck(ctx context.Context, request imageupdatetypes.CheckRequest) (schedulertypes.Outcome, error)
}

type pollingSettingReaderInternal interface {
	GetBoolSetting(ctx context.Context, key string, fallback bool) bool
	GetStringSetting(ctx context.Context, key, defaultValue string) string
}

type dockerEventBusProviderInternal interface {
	EventBus() *bus.DockerEventBus
}

type projectImageRefsBackfillerInternal interface {
	BackfillProjectImageRefs(ctx context.Context) (int, error)
}

// imagePollingEnvironmentProviderInternal exposes the environments this process serves against their
// own Docker daemon. Image polling must reach every such daemon, so one durable schedule is
// checkpointed per environment instead of only for the manager's local engine.
type imagePollingEnvironmentProviderInternal interface {
	EnabledDockerHostEnvironmentIDs(ctx context.Context) ([]string, error)
	InProcessTargetContext(ctx context.Context, environmentID string) (context.Context, bool)
	EnvironmentEventBus(ctx context.Context, environmentID string) (*bus.DockerEventBus, bool)
}

// environmentEventSubscriptionInternal is one docker-host environment's live subscription to its own
// daemon's event stream. unsubscribe stops delivery into this process; cancel also tears down the
// stream itself, so forgetting an environment really stops talking to its daemon.
type environmentEventSubscriptionInternal struct {
	unsubscribe func()
	cancel      context.CancelFunc
}

func (s environmentEventSubscriptionInternal) stop() {
	if s.unsubscribe != nil {
		s.unsubscribe()
	}
	if s.cancel != nil {
		s.cancel()
	}
}

// ImageUpdateWatcher debounces Docker events and admits durable image scans.
type ImageUpdateWatcher struct {
	imageUpdateService     imageUpdateScannerInternal
	settingsService        pollingSettingReaderInternal
	dockerService          dockerEventBusProviderInternal
	projectService         projectImageRefsBackfillerInternal
	environmentProvider    imagePollingEnvironmentProviderInternal
	dispatchMu             sync.RWMutex
	coordinator            *runs.Coordinator
	eventDegraded          atomic.Bool
	running                atomic.Bool
	scanMu                 sync.Mutex
	scanning               map[string]struct{}
	stopping               atomic.Bool
	triggerGeneration      atomic.Uint64
	acknowledgedGeneration atomic.Uint64
	triggeredAt            atomic.Int64
	trigger                chan struct{}
	scheduleRefresh        chan struct{}
	refreshPending         atomic.Bool
	environmentTrigger     chan struct{}
	environmentEventMu     sync.Mutex
	environmentEvents      map[string]environmentEventSubscriptionInternal
	pendingScanMu          sync.Mutex
	pendingScanEnvs        map[string]struct{}
	runMu                  sync.Mutex
	cancel                 context.CancelFunc
	done                   chan struct{}
	location               *time.Location
	debounce               time.Duration
	backfillRetry          time.Duration
	metadataReady          chan struct{}
	metadataReadyOnce      sync.Once
	stopped                chan struct{}
	stoppedOnce            sync.Once
}

func NewImageUpdateWatcher(cfg *config.Config,
	imageUpdateService *imageupdate.ImageUpdateService,
	settingsService *settings.SettingsService,
	dockerService *docker.DockerClientService,
	projectService *project.ProjectService) (*ImageUpdateWatcher,
	error,
) {
	if imageUpdateService == nil || settingsService == nil || dockerService == nil || projectService == nil {
		return nil, errors.New("image update watcher dependencies unavailable")
	}
	location := time.UTC
	if cfg != nil {
		location = cfg.GetLocation()
	}
	return &ImageUpdateWatcher{
			imageUpdateService: imageUpdateService,
			settingsService:    settingsService,
			dockerService:      dockerService,
			projectService:     projectService,
			location:           location,
			debounce:           imageUpdateWatcherDebounce,
			backfillRetry:      imageUpdateWatcherBackfillRetry,
			metadataReady:      make(chan struct{}),
			stopped:            make(chan struct{}),
			trigger:            make(chan struct{}, 1),
			scheduleRefresh:    make(chan struct{}, 1),
			environmentTrigger: make(chan struct{}, 1),
		},
		nil
}

func (w *ImageUpdateWatcher) Name() string { return "image-polling" }

// Start owns the debounce loop and joins its workers before returning.
func (w *ImageUpdateWatcher) Start(ctx context.Context) error {
	if w.stopping.Load() {
		return errors.New("image update watcher cannot be restarted after stop")
	}
	if !w.running.CompareAndSwap(false, true) {
		return errors.New("image update watcher already started")
	}
	defer w.running.Store(false)
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	w.runMu.Lock()
	if w.stopping.Load() {
		w.runMu.Unlock()
		cancel()
		return errors.New("image update watcher stopped")
	}
	w.cancel = cancel
	w.done = done
	w.runMu.Unlock()
	defer cancel()
	defer close(done)
	return w.runEventLoop(runCtx)
}

func (w *ImageUpdateWatcher) runEventLoop(runCtx context.Context) error {
	eventCh, unsubscribe := w.dockerService.EventBus().Subscribe(events.ImageEventType, bus.WithSubscriberBuffer(16))
	defer unsubscribe()
	defer w.clearEnvironmentEventSubscriptionsInternal()
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	defer timer.Stop()
	environmentTimer := time.NewTimer(time.Hour)
	environmentTimer.Stop()
	defer environmentTimer.Stop()
	var workers sync.WaitGroup
	defer workers.Wait()
	metadata := make(chan struct{}, 1)
	submitted := make(chan error, 1)
	submitting := false
	workers.Go(func() { w.runMetadataBackfillInternal(runCtx, metadata) })
	w.RefreshSchedule()
	ready := false
	for {
		select {
		case <-runCtx.Done():
			return nil
		case <-metadata:
			ready = true
			w.metadataReadyOnce.Do(func() { close(w.metadataReady) })
			w.Trigger()
		case <-w.trigger:
			if ready && !submitting && w.settingsService.GetBoolSetting(runCtx, "pollingEnabled", true) {
				timer.Reset(w.debounce)
			}
		case <-timer.C:
			if !w.scanAdmissionReadyInternal(timer, ready, submitting) {
				continue
			}
			submitting = true
			generation := w.triggerGeneration.Load()
			workers.Go(func() { w.submitTriggeredScanInternal(runCtx, generation, submitted) })
		case err := <-submitted:
			submitting = false
			if err != nil && runCtx.Err() == nil {
				slog.ErrorContext(runCtx, "image scan admission failed", "error", err)
			}
			if w.triggerGeneration.Load() != w.acknowledgedGeneration.Load() {
				timer.Reset(w.debounce)
			}
		case <-w.scheduleRefresh:
			// Drain every coalesced refresh request. The wake-up channel holds one token, so a second
			// refresh racing with a completed one would otherwise be lost and leave a stale schedule
			// installed until the next unrelated signal.
			for w.refreshPending.Swap(false) {
				if err := w.refreshSchedule(runCtx); err != nil {
					slog.ErrorContext(runCtx, "Failed to checkpoint image polling schedule", "error", err)
				}
			}
		case <-w.environmentTrigger:
			environmentTimer.Reset(w.debounce)
		case <-environmentTimer.C:
			// One debounce window per batch: a daemon that emits several events admits a single scan.
			if environments := w.drainPendingScanEnvironmentsInternal(); len(environments) > 0 {
				workers.Go(func() { w.submitEnvironmentTriggeredScansInternal(runCtx, environments) })
			}
		case _, ok := <-eventCh:
			if !ok {
				eventCh = nil
				w.eventDegraded.Store(true)
				continue
			}
			if w.settingsService.GetBoolSetting(runCtx, "imageEventWatcherEnabled", false) {
				w.Trigger()
			}
		}
	}
}

func (w *ImageUpdateWatcher) runMetadataBackfillInternal(ctx context.Context, metadata chan<- struct{}) {
	for attempt := 1; ctx.Err() == nil; attempt++ {
		var err error
		func() {
			defer utils.RecoverToError(&err, "image metadata backfill")
			err = w.backfillProjectImageRefsInternal(ctx, attempt)
		}()
		if err == nil {
			select {
			case metadata <- struct{}{}:
			case <-ctx.Done():
			}
			return
		}
		delay := w.backfillRetry
		if delay <= 0 {
			delay = imageUpdateWatcherBackfillRetry
		}
		retry := time.NewTimer(delay)
		select {
		case <-retry.C:
		case <-ctx.Done():
			retry.Stop()
			return
		}
	}
}

func (w *ImageUpdateWatcher) scanAdmissionReadyInternal(timer *time.Timer, ready, submitting bool) bool {
	if !ready || submitting || w.triggerGeneration.Load() == w.acknowledgedGeneration.Load() {
		return false
	}
	if remaining := time.Until(time.Unix(0, w.triggeredAt.Load()).Add(w.debounce)); remaining > 0 {
		timer.Reset(remaining)
		return false
	}
	return true
}

func (w *ImageUpdateWatcher) submitTriggeredScanInternal(ctx context.Context, generation uint64, submitted chan<- error) {
	var err error
	func() {
		defer utils.RecoverToError(&err, "image scan admission")
		w.dispatchMu.RLock()
		coordinator := w.coordinator
		w.dispatchMu.RUnlock()
		if coordinator == nil {
			err = errors.New("durable job coordinator unavailable")
			return
		}
		// The manager's own daemon publishes on the shared bus, which does not name the daemon an event
		// came from, so a shared-bus trigger stays with the manager's environment. Every docker-host
		// environment is watched through its own subscription and submitted by
		// submitEnvironmentTriggeredScansInternal instead.
		_, err = coordinator.Submit(ctx, schedulertypes.Request{JobID: w.Name(), EnvironmentID: "0", Trigger: "watcher"})
	}()
	if err == nil {
		w.acknowledgedGeneration.Store(generation)
	}
	select {
	case submitted <- err:
	case <-ctx.Done():
	}
}

func (w *ImageUpdateWatcher) Stop(ctx context.Context) error {
	w.stopping.Store(true)
	w.stoppedOnce.Do(func() { close(w.stopped) })
	w.runMu.Lock()
	cancel, done := w.cancel, w.done
	w.runMu.Unlock()
	if cancel == nil {
		return nil
	}
	cancel()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// beginScanInternal reserves the scan slot of one environment. Every docker-host daemon polls on its
// own schedule, so a scan for one environment must not be rejected because another one is running.
func (w *ImageUpdateWatcher) beginScanInternal(environmentID string) bool {
	w.scanMu.Lock()
	defer w.scanMu.Unlock()
	if _, busy := w.scanning[environmentID]; busy {
		return false
	}
	if w.scanning == nil {
		w.scanning = make(map[string]struct{}, 1)
	}
	w.scanning[environmentID] = struct{}{}
	return true
}

func (w *ImageUpdateWatcher) endScanInternal(environmentID string) {
	w.scanMu.Lock()
	delete(w.scanning, environmentID)
	w.scanMu.Unlock()
}

// scanRunningInternal reports whether environmentID has an image check in flight.
func (w *ImageUpdateWatcher) scanRunningInternal(environmentID string) bool {
	w.scanMu.Lock()
	defer w.scanMu.Unlock()
	_, busy := w.scanning[environmentID]
	return busy
}

// triggerEnvironmentInternal records that one environment's daemon emitted Docker events. The
// environment set is what the loop schedules from, so the wake-up channel may drop a token without
// losing the request.
func (w *ImageUpdateWatcher) triggerEnvironmentInternal(environmentID string) {
	if environmentID == "" {
		return
	}
	w.pendingScanMu.Lock()
	if w.pendingScanEnvs == nil {
		w.pendingScanEnvs = make(map[string]struct{}, 1)
	}
	w.pendingScanEnvs[environmentID] = struct{}{}
	w.pendingScanMu.Unlock()
	select {
	case w.environmentTrigger <- struct{}{}:
	default:
	}
}

// drainPendingScanEnvironmentsInternal returns and clears the environments whose events arrived since
// the last debounce window.
func (w *ImageUpdateWatcher) drainPendingScanEnvironmentsInternal() []string {
	w.pendingScanMu.Lock()
	defer w.pendingScanMu.Unlock()
	if len(w.pendingScanEnvs) == 0 {
		return nil
	}
	environments := make([]string, 0, len(w.pendingScanEnvs))
	for environmentID := range w.pendingScanEnvs {
		environments = append(environments, environmentID)
	}
	w.pendingScanEnvs = nil
	slices.Sort(environments)
	return environments
}

// submitEnvironmentTriggeredScansInternal admits one scan per environment whose daemon emitted events,
// so the scan runs against that daemon and lands in that environment's run history.
func (w *ImageUpdateWatcher) submitEnvironmentTriggeredScansInternal(ctx context.Context, environmentIDs []string) {
	var err error
	func() {
		defer utils.RecoverToError(&err, "environment image scan admission")
		w.dispatchMu.RLock()
		coordinator := w.coordinator
		w.dispatchMu.RUnlock()
		if coordinator == nil {
			err = errors.New("durable job coordinator unavailable")
			return
		}
		for _, environmentID := range environmentIDs {
			if _, submitErr := coordinator.Submit(ctx, schedulertypes.Request{JobID: w.Name(), EnvironmentID: environmentID, Trigger: "watcher"}); submitErr != nil && ctx.Err() == nil {
				slog.WarnContext(ctx, "environment image scan admission failed", "environmentId", environmentID, "error", submitErr)
			}
		}
	}()
	if err != nil && ctx.Err() == nil {
		slog.ErrorContext(ctx, "environment image scan admission failed", "error", err)
	}
}

// syncEnvironmentEventSubscriptionsInternal keeps one live subscription per environment this process
// serves: an environment that just became served starts streaming its own daemon's events, and one
// that is no longer served stops.
func (w *ImageUpdateWatcher) syncEnvironmentEventSubscriptionsInternal(ctx context.Context, provider imagePollingEnvironmentProviderInternal, environmentIDs []string) {
	desired := make(map[string]struct{}, len(environmentIDs))
	for _, environmentID := range environmentIDs {
		desired[environmentID] = struct{}{}
	}
	w.environmentEventMu.Lock()
	stale := make([]environmentEventSubscriptionInternal, 0)
	for environmentID, subscription := range w.environmentEvents {
		if _, keep := desired[environmentID]; !keep {
			stale = append(stale, subscription)
			delete(w.environmentEvents, environmentID)
		}
	}
	w.environmentEventMu.Unlock()
	for _, subscription := range stale {
		subscription.stop()
	}
	for _, environmentID := range environmentIDs {
		w.subscribeEnvironmentEventsInternal(ctx, provider, environmentID)
	}
}

func (w *ImageUpdateWatcher) subscribeEnvironmentEventsInternal(ctx context.Context, provider imagePollingEnvironmentProviderInternal, environmentID string) {
	if provider == nil || environmentID == "" {
		return
	}
	w.environmentEventMu.Lock()
	_, subscribed := w.environmentEvents[environmentID]
	w.environmentEventMu.Unlock()
	if subscribed {
		return
	}
	// The subscription owns the stream, so a daemon is only streamed while an environment is served:
	// cancelling this context stops both delivery and the connection underneath it.
	streamCtx, cancel := context.WithCancel(ctx)
	eventBus, ok := provider.EnvironmentEventBus(streamCtx, environmentID)
	if !ok || eventBus == nil {
		cancel()
		return
	}
	eventCh, unsubscribe := eventBus.Subscribe(events.ImageEventType, bus.WithSubscriberBuffer(16))
	w.environmentEventMu.Lock()
	if w.environmentEvents == nil {
		w.environmentEvents = make(map[string]environmentEventSubscriptionInternal, 1)
	}
	if _, exists := w.environmentEvents[environmentID]; exists {
		w.environmentEventMu.Unlock()
		environmentEventSubscriptionInternal{unsubscribe: unsubscribe, cancel: cancel}.stop()
		return
	}
	w.environmentEvents[environmentID] = environmentEventSubscriptionInternal{unsubscribe: unsubscribe, cancel: cancel}
	w.environmentEventMu.Unlock()
	go w.watchEnvironmentEventsInternal(streamCtx, environmentID, eventCh)
}

func (w *ImageUpdateWatcher) watchEnvironmentEventsInternal(ctx context.Context, environmentID string, eventCh <-chan events.Message) {
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-eventCh:
			if !ok {
				// The bus closed this subscriber; drop the subscription so the next schedule refresh
				// re-establishes it (and the stream under it).
				w.forgetEnvironmentEventSubscriptionInternal(environmentID)
				return
			}
			if w.settingsService.GetBoolSetting(ctx, "imageEventWatcherEnabled", false) {
				w.triggerEnvironmentInternal(environmentID)
			}
		}
	}
}

func (w *ImageUpdateWatcher) forgetEnvironmentEventSubscriptionInternal(environmentID string) {
	w.environmentEventMu.Lock()
	subscription, ok := w.environmentEvents[environmentID]
	if ok {
		delete(w.environmentEvents, environmentID)
	}
	w.environmentEventMu.Unlock()
	if ok {
		subscription.stop()
	}
}

func (w *ImageUpdateWatcher) clearEnvironmentEventSubscriptionsInternal() {
	w.environmentEventMu.Lock()
	subscriptions := w.environmentEvents
	w.environmentEvents = nil
	w.environmentEventMu.Unlock()
	for _, subscription := range subscriptions {
		subscription.stop()
	}
}

func (w *ImageUpdateWatcher) Trigger() {
	w.triggeredAt.Store(time.Now().UnixNano())
	w.triggerGeneration.Add(1)
	select {
	case w.trigger <- struct{}{}:
	default:
	}
}

// RefreshSchedule asks the event loop to re-resolve the polling schedule. The request is recorded
// before the wake-up is attempted: the wake-up channel is buffered with a single token and drops
// when it is already full, so the flag (drained by the loop) is what makes the request reliable.
func (w *ImageUpdateWatcher) RefreshSchedule() {
	w.refreshPending.Store(true)
	select {
	case w.scheduleRefresh <- struct{}{}:
	default:
	}
}

func (w *ImageUpdateWatcher) RunNow(ctx context.Context) (err error) {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-w.stopped:
		return errors.New("image update watcher is not running")
	case <-w.metadataReady:
	}
	if !w.running.Load() {
		return errors.New("image update watcher is not running")
	}
	environmentID := cmp.Or(envcontext.EnvironmentIDFromContext(ctx), "0")
	if _, executing := jobcontext.Run(ctx); executing {
		if !w.beginScanInternal(environmentID) {
			return common.Classify(common.ErrImageScanInProgress, errors.New("an image update check is already in progress"))
		}
		defer w.endScanInternal(environmentID)
		defer utils.RecoverToError(&err, "image update scan worker")
		return w.executeScanInternal(ctx)
	}
	if w.scanRunningInternal(environmentID) {
		return common.Classify(common.ErrImageScanInProgress, errors.New("an image update check is already in progress"))
	}
	w.dispatchMu.RLock()
	coordinator := w.coordinator
	w.dispatchMu.RUnlock()
	if coordinator == nil {
		return errors.New("durable job coordinator unavailable")
	}
	// An environment-scoped Run Now (issued from that environment's page) must land in that
	// environment's run namespace; requests without a target keep polling the manager's own engine.
	run, err := coordinator.Submit(ctx, schedulertypes.Request{JobID: w.Name(), EnvironmentID: environmentID, Trigger: "manual"})
	if err != nil {
		return err
	}
	// Back off from 50 ms to 1 s so quick runs return fast without polling long scans hard.
	for delay := 50 * time.Millisecond; ; delay = min(delay*2, time.Second) {
		current, getErr := coordinator.Get(ctx, run.EnvironmentID, run.JobID, run.ID)
		if getErr != nil {
			return getErr
		}
		if current.Status.Terminal() || current.Status == schedulertypes.NeedsAttention {
			if current.Status == schedulertypes.Succeeded {
				return nil
			}
			return &schedulertypes.OutcomeError{Outcome: current.Outcome}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		case <-w.stopped:
			return errors.New("image update watcher stopped")
		}
	}
}

func (w *ImageUpdateWatcher) executeScanInternal(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !w.settingsService.GetBoolSetting(ctx, "pollingEnabled", true) {
		slog.DebugContext(ctx, "image update watcher disabled; skipping image scan")
		return nil
	}

	slog.InfoContext(ctx, "image scan run started")
	outcome, err := w.imageUpdateService.RunImageCheck(ctx, imageupdatetypes.CheckRequest{All: true})
	if err != nil {
		return fmt.Errorf("image scan failed: %w", err)
	}
	slog.InfoContext(ctx, "image scan run completed", "status", outcome.Status, "message", outcome.Message)
	if outcome.Status != schedulertypes.Succeeded {
		return &schedulertypes.OutcomeError{Outcome: outcome}
	}
	return nil
}

func (w *ImageUpdateWatcher) backfillProjectImageRefsInternal(ctx context.Context, attempt int) error {
	started := time.Now()
	count, err := w.projectService.BackfillProjectImageRefs(ctx)
	if err == nil {
		slog.InfoContext(ctx, "project image metadata backfill completed", "projects", count, "duration", time.Since(started), "attempt", attempt)
	} else if ctx.Err() == nil {
		slog.WarnContext(ctx, "project image metadata backfill failed; retrying", "projects", count, "duration", time.Since(started), "attempt", attempt, "retryIn", w.backfillRetry, "error", err)
	}
	return err
}

func (w *ImageUpdateWatcher) refreshSchedule(ctx context.Context) error {
	w.dispatchMu.RLock()
	coordinator := w.coordinator
	w.dispatchMu.RUnlock()
	if coordinator == nil {
		return errors.New("durable job coordinator unavailable")
	}
	if !w.settingsService.GetBoolSetting(ctx, "pollingEnabled", true) {
		return coordinator.Checkpoint(ctx, w.Name(), "", time.Time{})
	}
	spec := w.settingsService.GetStringSetting(ctx, "pollingInterval", imageUpdateWatcherDefaultSchedule)
	schedule, err := cronScheduleParser.Parse(spec)
	if err != nil {
		slog.WarnContext(ctx, "invalid pollingInterval cron expression; using default schedule", "pollingInterval", spec, "error", err)
		spec = imageUpdateWatcherDefaultSchedule
		schedule, err = cronScheduleParser.Parse(spec)
		if err != nil {
			return err
		}
	}
	w.dispatchMu.RLock()
	provider := w.environmentProvider
	w.dispatchMu.RUnlock()
	if err := coordinator.Checkpoint(ctx, w.Name(), spec, schedule.Next(time.Now().In(w.location))); err != nil {
		return err
	}
	return w.checkpointEnvironmentSchedulesInternal(ctx, coordinator, provider)
}

// checkpointEnvironmentSchedulesInternal installs one durable polling schedule per docker-host
// environment, each resolved through that environment's own settings scope so a per-environment
// pollingInterval override is honoured. Agent environments are skipped because their schedules live
// on the agent, not in this process.
func (w *ImageUpdateWatcher) checkpointEnvironmentSchedulesInternal(ctx context.Context, coordinator *runs.Coordinator, provider imagePollingEnvironmentProviderInternal) error {
	if coordinator == nil || provider == nil {
		return nil
	}
	environmentIDs, err := provider.EnabledDockerHostEnvironmentIDs(ctx)
	if err != nil {
		slog.WarnContext(ctx, "image polling could not list docker-host environments", "error", err)
		return nil
	}
	var firstErr error
	served := make([]string, 0, len(environmentIDs))
	for _, environmentID := range environmentIDs {
		scoped, inProcess := provider.InProcessTargetContext(ctx, environmentID)
		if !inProcess {
			continue
		}
		served = append(served, environmentID)
		spec := w.settingsService.GetStringSetting(scoped, "pollingInterval", imageUpdateWatcherDefaultSchedule)
		schedule, parseErr := cronScheduleParser.Parse(spec)
		if parseErr != nil {
			slog.WarnContext(ctx, "invalid pollingInterval cron expression for environment; using default schedule", "environmentId", environmentID, "pollingInterval", spec, "error", parseErr)
			spec = imageUpdateWatcherDefaultSchedule
			schedule, parseErr = cronScheduleParser.Parse(spec)
			if parseErr != nil {
				if firstErr == nil {
					firstErr = parseErr
				}
				continue
			}
		}
		if checkpointErr := coordinator.CheckpointFor(scoped, environmentID, w.Name(), spec, schedule.Next(time.Now().In(w.location))); checkpointErr != nil {
			slog.WarnContext(ctx, "image polling schedule checkpoint failed for environment", "environmentId", environmentID, "error", checkpointErr)
			if firstErr == nil {
				firstErr = checkpointErr
			}
		}
	}
	// A daemon that stops being served must also stop feeding events, otherwise an event-driven scan
	// could still land in a disabled environment's run namespace.
	w.syncEnvironmentEventSubscriptionsInternal(ctx, provider, served)
	return firstErr
}

func (w *ImageUpdateWatcher) SetCoordinator(coordinator *runs.Coordinator) {
	w.dispatchMu.Lock()
	w.coordinator = coordinator
	w.dispatchMu.Unlock()
}

// SetEnvironmentProvider supplies the docker-host environments this process serves itself. Call it
// before Start; the provider is used whenever the polling schedule is refreshed.
func (w *ImageUpdateWatcher) SetEnvironmentProvider(provider imagePollingEnvironmentProviderInternal) {
	w.dispatchMu.Lock()
	w.environmentProvider = provider
	w.dispatchMu.Unlock()
}

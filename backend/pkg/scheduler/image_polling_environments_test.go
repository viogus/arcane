package scheduler

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.getarcane.app/streams/bus"

	"github.com/moby/moby/api/types/events"

	schedulertypes "github.com/getarcaneapp/arcane/types/v2/scheduler"

	"github.com/getarcaneapp/arcane/backend/v2/pkg/libarcane/envcontext"
	"github.com/getarcaneapp/arcane/backend/v2/pkg/scheduler/jobcontext"
	"github.com/getarcaneapp/arcane/backend/v2/pkg/scheduler/runs"
)

const (
	pollingDockerHostEnvironmentAInternal = "env-polling-a"
	pollingDockerHostEnvironmentBInternal = "env-polling-b"
	pollingAgentEnvironmentInternal       = "env-polling-agent"
)

// pollingEnvironmentProviderFakeInternal stands in for the environment service: it lists the
// environments this process serves itself and scopes a context to one of their daemons. The agent
// environment is deliberately listed but not served, mirroring production.
type pollingEnvironmentProviderFakeInternal struct {
	ids       []string
	inProcess map[string]string
	buses     map[string]*bus.DockerEventBus
	err       error
}

func (p pollingEnvironmentProviderFakeInternal) EnabledDockerHostEnvironmentIDs(context.Context) ([]string, error) {
	if p.err != nil {
		return nil, p.err
	}
	return p.ids, nil
}

func (p pollingEnvironmentProviderFakeInternal) InProcessTargetContext(ctx context.Context, environmentID string) (context.Context, bool) {
	host, ok := p.inProcess[environmentID]
	if !ok {
		return ctx, false
	}
	return envcontext.WithTarget(ctx, envcontext.Target{EnvironmentID: environmentID, DockerHost: host}), true
}

func (p pollingEnvironmentProviderFakeInternal) EnvironmentEventBus(_ context.Context, environmentID string) (*bus.DockerEventBus, bool) {
	eventBus, ok := p.buses[environmentID]
	return eventBus, ok
}

// perEnvironmentPollingSettingsFakeInternal answers with the schedule configured for the environment
// the context is scoped to, so a test proves each daemon's override is read on its own.
type perEnvironmentPollingSettingsFakeInternal struct {
	schedules map[string]string
}

func (s perEnvironmentPollingSettingsFakeInternal) GetBoolSetting(context.Context, string, bool) bool {
	return true
}

func (s perEnvironmentPollingSettingsFakeInternal) GetStringSetting(ctx context.Context, _, defaultValue string) string {
	if spec := s.schedules[envcontext.EnvironmentIDFromContext(ctx)]; spec != "" {
		return spec
	}
	return defaultValue
}

func pollingEnvironmentProviderForTestInternal() pollingEnvironmentProviderFakeInternal {
	return pollingEnvironmentProviderFakeInternal{
		ids: []string{
			pollingDockerHostEnvironmentAInternal,
			pollingDockerHostEnvironmentBInternal,
			pollingAgentEnvironmentInternal,
		},
		inProcess: map[string]string{
			pollingDockerHostEnvironmentAInternal: "tcp://192.168.2.33:6002",
			pollingDockerHostEnvironmentBInternal: "tcp://192.168.2.33:6001",
		},
	}
}

func watcherCoordinatorInternal(t *testing.T, watcher *ImageUpdateWatcher) *runs.Coordinator {
	t.Helper()
	watcher.dispatchMu.RLock()
	defer watcher.dispatchMu.RUnlock()
	require.NotNil(t, watcher.coordinator)
	return watcher.coordinator
}

func recordsByEnvironmentInternal(t *testing.T, coordinator *runs.Coordinator) map[string][]string {
	t.Helper()
	records, err := coordinator.Records(t.Context())
	require.NoError(t, err)
	byEnvironment := make(map[string][]string, len(records))
	for _, record := range records {
		byEnvironment[record.EnvironmentID] = append(byEnvironment[record.EnvironmentID], record.Schedule)
	}
	return byEnvironment
}

// TestImageUpdateWatcher_RefreshSchedulesEveryDockerHostEnvironment is the regression guard for the
// reported gap: image polling only ever checkpointed the manager's namespace, so the daemons behind
// docker-host environments were never polled. Each enabled docker-host environment must get its own
// schedule, read through that environment's settings, while agent environments must not.
func TestImageUpdateWatcher_RefreshSchedulesEveryDockerHostEnvironment(t *testing.T) {
	provider := pollingEnvironmentProviderForTestInternal()
	settings := perEnvironmentPollingSettingsFakeInternal{schedules: map[string]string{
		pollingDockerHostEnvironmentAInternal: "0 30 5 * * *",
	}}
	watcher := newImageUpdateWatcherForTestInternal(t, &imageUpdateScannerFakeInternal{}, settings, bus.NewDockerEventBus(), nil)
	watcher.SetEnvironmentProvider(provider)

	require.NoError(t, watcher.refreshScheduleInternal(t.Context()))

	byEnvironment := recordsByEnvironmentInternal(t, watcherCoordinatorInternal(t, watcher))
	require.Len(t, byEnvironment, 3)
	assert.Equal(t, []string{imageUpdateWatcherDefaultSchedule}, byEnvironment["0"])
	assert.Equal(t, []string{"0 30 5 * * *"}, byEnvironment[pollingDockerHostEnvironmentAInternal])
	assert.Equal(t, []string{imageUpdateWatcherDefaultSchedule}, byEnvironment[pollingDockerHostEnvironmentBInternal])
	assert.NotContains(t, byEnvironment, pollingAgentEnvironmentInternal)
}

// TestImageUpdateWatcher_RefreshScheduleUpdatesOneEnvironmentOnly proves a second refresh re-reads
// every environment independently: changing one daemon's interval must not disturb the others.
func TestImageUpdateWatcher_RefreshScheduleUpdatesOneEnvironmentOnly(t *testing.T) {
	provider := pollingEnvironmentProviderForTestInternal()
	settings := perEnvironmentPollingSettingsFakeInternal{schedules: map[string]string{
		pollingDockerHostEnvironmentAInternal: "0 30 5 * * *",
		pollingDockerHostEnvironmentBInternal: "0 15 * * * *",
	}}
	watcher := newImageUpdateWatcherForTestInternal(t, &imageUpdateScannerFakeInternal{}, settings, bus.NewDockerEventBus(), nil)
	watcher.SetEnvironmentProvider(provider)
	require.NoError(t, watcher.refreshScheduleInternal(t.Context()))

	settings.schedules[pollingDockerHostEnvironmentAInternal] = "0 45 4 * * *"
	require.NoError(t, watcher.refreshScheduleInternal(t.Context()))

	byEnvironment := recordsByEnvironmentInternal(t, watcherCoordinatorInternal(t, watcher))
	assert.Equal(t, []string{"0 45 4 * * *"}, byEnvironment[pollingDockerHostEnvironmentAInternal])
	assert.Equal(t, []string{"0 15 * * * *"}, byEnvironment[pollingDockerHostEnvironmentBInternal])
	assert.Equal(t, []string{imageUpdateWatcherDefaultSchedule}, byEnvironment["0"])
}

// TestImageUpdateWatcher_RefreshScheduleToleratesProviderFailure keeps startup resilient: without a
// usable environment list the manager's own polling schedule must still be installed.
func TestImageUpdateWatcher_RefreshScheduleToleratesProviderFailure(t *testing.T) {
	provider := pollingEnvironmentProviderFakeInternal{err: errors.New("environment lookup failed")}
	watcher := newImageUpdateWatcherForTestInternal(t, &imageUpdateScannerFakeInternal{}, perEnvironmentPollingSettingsFakeInternal{}, bus.NewDockerEventBus(), nil)
	watcher.SetEnvironmentProvider(provider)

	require.NoError(t, watcher.refreshScheduleInternal(t.Context()))

	byEnvironment := recordsByEnvironmentInternal(t, watcherCoordinatorInternal(t, watcher))
	assert.Equal(t, []string{imageUpdateWatcherDefaultSchedule}, byEnvironment["0"])
}

// TestImageUpdateWatcher_RunNowUsesTheEnvironmentNamespace covers Run Now from a docker-host
// environment's page: the run must be queued, executed and reported in that environment's own
// namespace instead of the manager's. The watcher's own startup scan also runs here, so the
// environment is identified by its manual run rather than by counting records.
func TestImageUpdateWatcher_RunNowUsesTheEnvironmentNamespace(t *testing.T) {
	watcher := newImageUpdateWatcherForTestInternal(t, &imageUpdateScannerFakeInternal{}, perEnvironmentPollingSettingsFakeInternal{}, bus.NewDockerEventBus(), nil)
	watcher.SetEnvironmentProvider(pollingEnvironmentProviderForTestInternal())
	startImageUpdateWatcherForTestInternal(t, watcher)

	scoped := envcontext.WithTarget(t.Context(), envcontext.Target{
		EnvironmentID: pollingDockerHostEnvironmentAInternal,
		DockerHost:    "tcp://192.168.2.33:6002",
	})
	// RunNow returns only once the admitted run reaches a terminal state, so a nil error already
	// proves the run was executed in this process for that environment.
	require.NoError(t, watcher.RunNow(scoped))

	coordinator := watcherCoordinatorInternal(t, watcher)
	scopedList, err := coordinator.List(t.Context(), pollingDockerHostEnvironmentAInternal, "image-polling", 1, 10)
	require.NoError(t, err)
	require.Equal(t, 1, scopedList.Total)
	assert.Equal(t, "manual", scopedList.Runs[0].Trigger)
	assert.Equal(t, pollingDockerHostEnvironmentAInternal, scopedList.Runs[0].EnvironmentID)
	assert.Equal(t, schedulertypes.Succeeded, scopedList.Runs[0].Status)

	managerList, err := coordinator.List(t.Context(), "0", "image-polling", 1, 10)
	require.NoError(t, err)
	for _, run := range managerList.Runs {
		assert.NotEqual(t, "manual", run.Trigger, "an environment-scoped Run Now must not be reported as a manager run")
	}
}

// TestImageUpdateWatcher_EnvironmentScansRunInParallel proves the in-progress guard is per
// environment: two docker-host daemons polling at the same instant must both scan, because a shared
// guard would fail one of them on every collision.
func TestImageUpdateWatcher_EnvironmentScansRunInParallel(t *testing.T) {
	release := make(chan struct{})
	scanner := &imageUpdateScannerFakeInternal{startedCh: make(chan int, 2), releaseCh: release}
	watcher := newImageUpdateWatcherForTestInternal(t, scanner, perEnvironmentPollingSettingsFakeInternal{}, bus.NewDockerEventBus(), nil)
	// Drive the executing branch directly: no event loop, no startup scan, no timing races.
	watcher.running.Store(true)
	watcher.startedOnce.Do(func() { close(watcher.started) })
	watcher.metadataReadyOnce.Do(func() { close(watcher.metadataReady) })

	executionContext := func(environmentID, endpoint string) context.Context {
		ctx := envcontext.WithTarget(t.Context(), envcontext.Target{EnvironmentID: environmentID, DockerHost: endpoint})
		return jobcontext.WithExecution(ctx, schedulertypes.Run{
			ID:            "run-" + environmentID,
			JobID:         "image-polling",
			EnvironmentID: environmentID,
			Trigger:       "scheduled",
		}, nil)
	}

	results := make(chan error, 2)
	go func() {
		results <- watcher.RunNow(executionContext(pollingDockerHostEnvironmentAInternal, "tcp://192.168.2.33:6002"))
	}()
	go func() {
		results <- watcher.RunNow(executionContext(pollingDockerHostEnvironmentBInternal, "tcp://192.168.2.33:6001"))
	}()

	waitStarted := func() int {
		t.Helper()
		select {
		case call := <-scanner.startedCh:
			return call
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for both environment scans to start")
			return 0
		}
	}
	assert.Equal(t, 1, waitStarted())
	assert.Equal(t, 2, waitStarted(), "a second environment must be able to scan while the first one is running")

	close(release)
	require.NoError(t, <-results)
	require.NoError(t, <-results)
	assert.Equal(t, 2, scanner.countInternal())
	assert.Equal(t, 2, scanner.maxActiveInternal())
}

func environmentSubscriptionCountInternal(watcher *ImageUpdateWatcher) int {
	watcher.environmentEventMu.Lock()
	defer watcher.environmentEventMu.Unlock()
	return len(watcher.environmentEvents)
}

func environmentWatcherRunCountInternal(t *testing.T, watcher *ImageUpdateWatcher, environmentID string) int {
	t.Helper()
	list, err := watcherCoordinatorInternal(t, watcher).List(t.Context(), environmentID, "image-polling", 1, 50)
	require.NoError(t, err)
	count := 0
	for _, run := range list.Runs {
		if run.Trigger == "watcher" {
			count++
		}
	}
	return count
}

func pollingEnvironmentBusesInternal() map[string]*bus.DockerEventBus {
	return map[string]*bus.DockerEventBus{
		pollingDockerHostEnvironmentAInternal: bus.NewDockerEventBus(),
		pollingDockerHostEnvironmentBInternal: bus.NewDockerEventBus(),
	}
}

// TestImageUpdateWatcher_EnvironmentEventsSubmitScansToTheirEnvironment is the regression guard for
// the reported gap: every daemon used to publish on one shared bus, so an event-driven scan could only
// ever be admitted as the manager's. Each docker-host environment now streams its own daemon, and its
// events must admit a scan in that environment's own namespace only.
func TestImageUpdateWatcher_EnvironmentEventsSubmitScansToTheirEnvironment(t *testing.T) {
	buses := pollingEnvironmentBusesInternal()
	provider := pollingEnvironmentProviderForTestInternal()
	provider.buses = buses
	watcher := newImageUpdateWatcherForTestInternal(t, &imageUpdateScannerFakeInternal{},
		&pollingSettingReaderFakeInternal{enabled: true, eventWatcherEnabled: true}, bus.NewDockerEventBus(), nil)
	watcher.SetEnvironmentProvider(&provider)
	startImageUpdateWatcherForTestInternal(t, watcher)

	require.Eventually(t, func() bool { return environmentSubscriptionCountInternal(watcher) == 2 },
		5*time.Second, 5*time.Millisecond, "every served environment must be streaming its own daemon")

	buses[pollingDockerHostEnvironmentAInternal].Publish(events.Message{Type: events.ImageEventType, Action: events.ActionPull})

	require.Eventually(t, func() bool {
		return environmentWatcherRunCountInternal(t, watcher, pollingDockerHostEnvironmentAInternal) == 1
	}, 5*time.Second, 10*time.Millisecond, "events from one daemon must admit a scan for its own environment")
	assert.Zero(t, environmentWatcherRunCountInternal(t, watcher, pollingDockerHostEnvironmentBInternal),
		"another daemon's environment must not receive the scan")

	buses[pollingDockerHostEnvironmentBInternal].Publish(events.Message{Type: events.ImageEventType, Action: events.ActionTag})

	require.Eventually(t, func() bool {
		return environmentWatcherRunCountInternal(t, watcher, pollingDockerHostEnvironmentBInternal) == 1
	}, 5*time.Second, 10*time.Millisecond, "the second daemon must admit its own scan")
}

// TestImageUpdateWatcher_ForgetEnvironmentStopsItsEventStream proves a disabled environment stops
// feeding events: the schedule refresh drops its subscription, so a late event cannot admit a scan
// into a namespace whose schedule was just cleared.
func TestImageUpdateWatcher_ForgetEnvironmentStopsItsEventStream(t *testing.T) {
	buses := pollingEnvironmentBusesInternal()
	provider := pollingEnvironmentProviderForTestInternal()
	provider.buses = buses
	watcher := newImageUpdateWatcherForTestInternal(t, &imageUpdateScannerFakeInternal{},
		&pollingSettingReaderFakeInternal{enabled: true, eventWatcherEnabled: true}, bus.NewDockerEventBus(), nil)
	watcher.SetEnvironmentProvider(&provider)
	startImageUpdateWatcherForTestInternal(t, watcher)

	require.Eventually(t, func() bool { return environmentSubscriptionCountInternal(watcher) == 2 },
		5*time.Second, 5*time.Millisecond)

	provider.ids = []string{pollingDockerHostEnvironmentAInternal}
	require.NoError(t, watcher.refreshScheduleInternal(t.Context()))
	require.Eventually(t, func() bool { return environmentSubscriptionCountInternal(watcher) == 1 },
		5*time.Second, 5*time.Millisecond, "the forgotten environment must stop streaming its daemon")

	buses[pollingDockerHostEnvironmentBInternal].Publish(events.Message{Type: events.ImageEventType, Action: events.ActionPull})
	buses[pollingDockerHostEnvironmentAInternal].Publish(events.Message{Type: events.ImageEventType, Action: events.ActionPull})

	// The event of the environment that is still served acts as the barrier: once its scan exists, the
	// dropped environment's event would have been admitted too.
	require.Eventually(t, func() bool {
		return environmentWatcherRunCountInternal(t, watcher, pollingDockerHostEnvironmentAInternal) == 1
	}, 5*time.Second, 10*time.Millisecond)
	assert.Zero(t, environmentWatcherRunCountInternal(t, watcher, pollingDockerHostEnvironmentBInternal),
		"a forgotten environment must not admit scans any more")
}

package event

import (
	"context"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/moby/moby/api/types/events"
	"github.com/stretchr/testify/require"
	"go.getarcane.app/streams/bus"

	"github.com/getarcaneapp/arcane/backend/v2/internal/event/children/correlation"
	"github.com/getarcaneapp/arcane/backend/v2/pkg/libarcane/envcontext"
)

const (
	dockerHostEnvironmentAInternal = "env-docker-host-a"
	dockerHostEnvironmentBInternal = "env-docker-host-b"
	dockerHostAgentEnvironment     = "env-agent"
)

func dockerHostTargetContextInternal(t *testing.T, environmentID, dockerHost string) context.Context {
	t.Helper()
	return envcontext.WithTarget(t.Context(), envcontext.Target{EnvironmentID: environmentID, DockerHost: dockerHost})
}

// dockerHostEventProviderFakeInternal stands in for the environment service: it lists the
// environments this process serves itself, scopes a context to one of their daemons and hands out that
// daemon's event bus. The agent environment is deliberately listed but not served, mirroring
// production. The reconciler reads it from its own goroutine, so every field is read under mu.
type dockerHostEventProviderFakeInternal struct {
	mu        sync.Mutex
	ids       []string
	inProcess map[string]string
	buses     map[string]*bus.DockerEventBus
	err       error
}

func (p *dockerHostEventProviderFakeInternal) EnabledDockerHostEnvironmentIDs(context.Context) ([]string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		return nil, p.err
	}
	return p.ids, nil
}

// setEnabledIDs changes which environments a running reconciler sees on its next pass.
func (p *dockerHostEventProviderFakeInternal) setEnabledIDs(ids []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ids = ids
}

func (p *dockerHostEventProviderFakeInternal) InProcessTargetContext(ctx context.Context, environmentID string) (context.Context, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	dockerHost, ok := p.inProcess[environmentID]
	if !ok {
		return ctx, false
	}
	return envcontext.WithTarget(ctx, envcontext.Target{EnvironmentID: environmentID, DockerHost: dockerHost}), true
}

func (p *dockerHostEventProviderFakeInternal) EnvironmentEventBus(_ context.Context, environmentID string) (*bus.DockerEventBus, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	eventBus, ok := p.buses[environmentID]
	return eventBus, ok
}

// TestCreateEventAttributesEnvironmentFromContext is the regression guard for the reported gap:
// work served for a docker-host environment runs in this process, so its event rows must name that
// environment instead of landing on the manager's own stream. A caller that names an environment
// (agent ingestion, forwarded records) keeps it.
func TestCreateEventAttributesEnvironmentFromContext(t *testing.T) {
	db := setupEventServiceTestDB(t)
	svc := NewEventService(db, nil, nil)
	scoped := dockerHostTargetContextInternal(t, dockerHostEnvironmentAInternal, "tcp://192.168.2.33:6002")

	for _, tc := range []struct {
		name      string
		ctx       context.Context
		requested *string
		want      *string
	}{
		{"scoped request without an environment", scoped, nil, new(dockerHostEnvironmentAInternal)},
		{"scoped request naming the manager", scoped, new("0"), new(dockerHostEnvironmentAInternal)},
		{"scoped request naming an environment keeps it", scoped, new(dockerHostAgentEnvironment), new(dockerHostAgentEnvironment)},
		{"unscoped request without an environment", t.Context(), nil, nil},
		{"unscoped request naming the manager", t.Context(), new("0"), new("0")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			recorded, err := svc.CreateEvent(tc.ctx, CreateEventRequest{
				Type: EventTypeContainerStart, Title: "start", EnvironmentID: tc.requested,
			})
			require.NoError(t, err)
			require.Equal(t, tc.want, recorded.EnvironmentID)
		})
	}
}

// TestDaemonSuppressionIsScopedToEnvironment proves an Arcane action against a docker-host
// environment's daemon is correlated there: the daemon's own observation is suppressed instead of
// duplicated, while the same resource name on another daemon is still reported.
func TestDaemonSuppressionIsScopedToEnvironment(t *testing.T) {
	db := setupEventServiceTestDB(t)
	svc := NewEventService(db, nil, nil)
	scopedA := dockerHostTargetContextInternal(t, dockerHostEnvironmentAInternal, "tcp://192.168.2.33:6002")
	scopedB := dockerHostTargetContextInternal(t, dockerHostEnvironmentBInternal, "tcp://192.168.2.33:6001")

	_, err := svc.CreateEvent(scopedA, CreateEventRequest{
		Type:         EventTypeContainerStart,
		Title:        "Container started",
		ResourceType: new("container"),
		ResourceID:   new("abcdef12abcdef12abcdef12abcdef12abcdef12abcdef12abcdef12abcdef12"),
		ResourceName: new("web"),
	})
	require.NoError(t, err)

	const containerID = "abcdef12abcdef12abcdef12abcdef12abcdef12abcdef12abcdef12abcdef12"
	require.True(t, svc.ShouldSuppressDaemonEvent(scopedA, "container", containerID, "web", ""))
	require.False(t, svc.ShouldSuppressDaemonEvent(scopedB, "container", containerID, "web", ""))
	require.False(t, svc.ShouldSuppressDaemonEvent(t.Context(), "container", containerID, "web", ""))

	// The daemon that was not acted on still reports its container; the acted-on daemon does not.
	svc.RecordDockerEvent(scopedA, events.Message{Type: events.ContainerEventType, Action: events.ActionStart, TimeNano: 1, Actor: events.Actor{ID: containerID, Attributes: map[string]string{"name": "web"}}})
	svc.RecordDockerEvent(scopedB, events.Message{Type: events.ContainerEventType, Action: events.ActionStart, TimeNano: 2, Actor: events.Actor{ID: containerID, Attributes: map[string]string{"name": "web"}}})
	// The daemon event writer batches rows, so wait for one flush before reading them back.
	time.Sleep(daemonEventFlushInterval)
	var rows []Event
	require.NoError(t, db.Find(&rows).Error)
	var daemonEnvironmentIDs []string
	for _, row := range rows {
		if row.Metadata["source"] == "docker" {
			require.NotNil(t, row.EnvironmentID)
			daemonEnvironmentIDs = append(daemonEnvironmentIDs, *row.EnvironmentID)
		}
	}
	// Only the daemon that was not acted on produced an event; the acted-on daemon's observation was
	// correlated with the action instead of being duplicated.
	require.Equal(t, []string{dockerHostEnvironmentBInternal}, daemonEnvironmentIDs)
	// The created action row and the surviving daemon observation.
	require.Len(t, rows, 2)
}

// TestSubscribeDockerHostEnvironmentEventsReconciles is the regression guard for the second half of
// the gap: only the manager's daemon was streamed, so a docker-host environment's container, image,
// volume and network events never appeared on its own stream.
func TestSubscribeDockerHostEnvironmentEventsReconciles(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		db := setupEventServiceTestDB(t)
		sqlDB, err := db.DB.DB()
		require.NoError(t, err)
		defer func() { require.NoError(t, sqlDB.Close()) }()
		sqlDB.SetMaxOpenConns(1)

		busA := bus.NewDockerEventBus()
		busB := bus.NewDockerEventBus()
		defer busA.Close()
		defer busB.Close()
		provider := &dockerHostEventProviderFakeInternal{
			ids: []string{dockerHostEnvironmentAInternal, dockerHostEnvironmentBInternal, dockerHostAgentEnvironment},
			inProcess: map[string]string{
				dockerHostEnvironmentAInternal: "tcp://192.168.2.33:6002",
				dockerHostEnvironmentBInternal: "tcp://192.168.2.33:6001",
			},
			buses: map[string]*bus.DockerEventBus{
				dockerHostEnvironmentAInternal: busA,
				dockerHostEnvironmentBInternal: busB,
			},
		}
		svc := NewEventService(db, nil, nil)
		svc.correlation = correlation.NewService(time.Now)
		svc.SetDockerHostEventProvider(provider)

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- svc.SubscribeDockerHostEnvironmentEvents()(ctx) }()
		synctest.Wait()

		// The agent environment is listed but served by its own agent, so it gets no subscription.
		svc.dockerHostEventMu.Lock()
		require.Len(t, svc.dockerHostEvents, 2)
		svc.dockerHostEventMu.Unlock()

		busA.Publish(events.Message{Type: events.ContainerEventType, Action: events.ActionStart, TimeNano: 1, Actor: events.Actor{ID: "container-a", Attributes: map[string]string{"name": "web"}}})
		busB.Publish(events.Message{Type: events.ContainerEventType, Action: events.ActionStart, TimeNano: 2, Actor: events.Actor{ID: "container-b", Attributes: map[string]string{"name": "web"}}})
		synctest.Wait()

		// The daemon event writer batches rows: advance past one flush before reading them back.
		time.Sleep(daemonEventFlushInterval)
		synctest.Wait()

		byEnvironment := map[string]int{}
		var rows []Event
		require.NoError(t, db.Find(&rows).Error)
		for _, row := range rows {
			require.NotNil(t, row.EnvironmentID)
			byEnvironment[*row.EnvironmentID]++
		}
		require.Equal(t, map[string]int{dockerHostEnvironmentAInternal: 1, dockerHostEnvironmentBInternal: 1}, byEnvironment)

		// A disabled environment loses its subscription and stops producing rows, while the one that
		// remains keeps recording.
		provider.setEnabledIDs([]string{dockerHostEnvironmentAInternal})
		time.Sleep(2 * dockerHostEventReconcileInterval)
		busB.Publish(events.Message{Type: events.ContainerEventType, Action: events.ActionStart, TimeNano: 3, Actor: events.Actor{ID: "container-b", Attributes: map[string]string{"name": "web"}}})
		busA.Publish(events.Message{Type: events.ImageEventType, Action: events.ActionPull, TimeNano: 4, Actor: events.Actor{ID: "sha256:" + strings.Repeat("ab", 32), Attributes: map[string]string{"name": "nginx:latest"}}})
		synctest.Wait()
		time.Sleep(daemonEventFlushInterval)
		synctest.Wait()
		byEnvironment = map[string]int{}
		rows = nil
		require.NoError(t, db.Find(&rows).Error)
		for _, row := range rows {
			byEnvironment[*row.EnvironmentID]++
		}
		require.Equal(t, map[string]int{dockerHostEnvironmentAInternal: 2, dockerHostEnvironmentBInternal: 1}, byEnvironment)

		cancel()
		synctest.Wait()
		require.NoError(t, <-done)
		// The daemon event writer lives until the service stops; close it inside the bubble.
		require.NoError(t, svc.Stop(context.WithoutCancel(t.Context())))
		synctest.Wait()
		svc.dockerHostEventMu.Lock()
		require.Empty(t, svc.dockerHostEvents)
		svc.dockerHostEventMu.Unlock()
	})
}

// TestDockerHostEventSubscriptionsSurviveProviderFailure keeps a failing environment lookup from
// taking the manager's own event recording down with it.
func TestDockerHostEventSubscriptionsSurviveProviderFailure(t *testing.T) {
	db := setupEventServiceTestDB(t)
	svc := NewEventService(db, nil, nil)
	provider := &dockerHostEventProviderFakeInternal{err: context.DeadlineExceeded}
	svc.SetDockerHostEventProvider(provider)
	svc.syncDockerHostEventSubscriptionsInternal(t.Context())
	require.Empty(t, svc.dockerHostEvents)
}

func TestEventEnvironmentIDInternalKeepsAuthoritativeEnvironments(t *testing.T) {
	scoped := dockerHostTargetContextInternal(t, dockerHostEnvironmentAInternal, "tcp://192.168.2.33:6002")
	require.Equal(t, dockerHostEnvironmentAInternal, *eventEnvironmentIDInternal(scoped, nil))
	require.Equal(t, dockerHostEnvironmentAInternal, *eventEnvironmentIDInternal(scoped, new("  ")))
	require.Equal(t, dockerHostEnvironmentAInternal, *eventEnvironmentIDInternal(scoped, new(" 0 ")))
	require.Equal(t, dockerHostAgentEnvironment, *eventEnvironmentIDInternal(scoped, new(dockerHostAgentEnvironment)))
	require.Nil(t, eventEnvironmentIDInternal(t.Context(), nil))
	require.Equal(t, "0", *eventEnvironmentIDInternal(t.Context(), new("0")))
}

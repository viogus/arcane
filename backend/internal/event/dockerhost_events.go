package event

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/moby/moby/api/types/events"
	"go.getarcane.app/streams/bus"
)

// dockerHostEventProviderInternal exposes the environments whose Docker daemon this process talks to
// directly. Those daemons are streamed per environment so their events are recorded with the
// environment that produced them instead of only the manager's own daemon being visible.
type dockerHostEventProviderInternal interface {
	EnabledDockerHostEnvironmentIDs(ctx context.Context) ([]string, error)
	InProcessTargetContext(ctx context.Context, environmentID string) (context.Context, bool)
	EnvironmentEventBus(ctx context.Context, environmentID string) (*bus.DockerEventBus, bool)
}

// dockerHostEventReconcileInterval bounds how long a newly enabled docker-host environment waits for
// its daemon events to be recorded.
const dockerHostEventReconcileInterval = 15 * time.Second

type dockerHostEventSubscriptionInternal struct {
	cancel      context.CancelFunc
	unsubscribe func()
}

func (s dockerHostEventSubscriptionInternal) stopInternal() {
	if s.cancel != nil {
		s.cancel()
	}
	if s.unsubscribe != nil {
		s.unsubscribe()
	}
}

// SetDockerHostEventProvider supplies the docker-host environments this process serves itself. It is
// injected after construction because the environment service depends on the Docker service.
func (s *EventService) SetDockerHostEventProvider(provider dockerHostEventProviderInternal) {
	if s == nil {
		return
	}
	s.dockerHostEventMu.Lock()
	s.dockerHostEventProvider = provider
	s.dockerHostEventMu.Unlock()
}

// SubscribeDockerHostEnvironmentEvents records daemon events for every docker-host environment this
// process serves, reconciling the set of subscriptions on an interval. The local daemon keeps its own
// subscription; this adds the remote daemons.
func (s *EventService) SubscribeDockerHostEnvironmentEvents() func(context.Context) error {
	if s == nil {
		return func(context.Context) error { return nil }
	}
	return func(ctx context.Context) error {
		// Every subscription is scoped to this run, so a restart reconciles from scratch instead of
		// leaving a subscriber behind on a daemon this process no longer serves.
		defer s.clearDockerHostEventSubscriptionsInternal()
		ticker := time.NewTicker(dockerHostEventReconcileInterval)
		defer ticker.Stop()
		s.syncDockerHostEventSubscriptionsInternal(ctx)
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
				s.syncDockerHostEventSubscriptionsInternal(ctx)
			}
		}
	}
}

func (s *EventService) clearDockerHostEventSubscriptionsInternal() {
	s.dockerHostEventMu.Lock()
	subscriptions := s.dockerHostEvents
	s.dockerHostEvents = nil
	s.dockerHostEventMu.Unlock()
	for _, subscription := range subscriptions {
		subscription.stopInternal()
	}
}

func (s *EventService) dockerHostEventProviderInternal() dockerHostEventProviderInternal {
	s.dockerHostEventMu.Lock()
	defer s.dockerHostEventMu.Unlock()
	return s.dockerHostEventProvider
}

func (s *EventService) syncDockerHostEventSubscriptionsInternal(ctx context.Context) {
	provider := s.dockerHostEventProviderInternal()
	if provider == nil {
		return
	}
	environmentIDs, err := provider.EnabledDockerHostEnvironmentIDs(ctx)
	if err != nil {
		slog.WarnContext(ctx, "Failed to list docker host environments for Docker event subscriptions", "error", err)
		return
	}
	desired := make(map[string]struct{}, len(environmentIDs))
	for _, environmentID := range environmentIDs {
		desired[environmentID] = struct{}{}
	}

	var stale []dockerHostEventSubscriptionInternal
	s.dockerHostEventMu.Lock()
	for environmentID, subscription := range s.dockerHostEvents {
		if _, keep := desired[environmentID]; keep {
			continue
		}
		delete(s.dockerHostEvents, environmentID)
		stale = append(stale, subscription)
	}
	s.dockerHostEventMu.Unlock()
	for _, subscription := range stale {
		subscription.stopInternal()
	}

	for _, environmentID := range environmentIDs {
		s.subscribeDockerHostEnvironmentEventsInternal(ctx, provider, environmentID)
	}
}

func (s *EventService) subscribeDockerHostEnvironmentEventsInternal(ctx context.Context, provider dockerHostEventProviderInternal, environmentID string) {
	if environmentID == "" {
		return
	}
	s.dockerHostEventMu.Lock()
	_, subscribed := s.dockerHostEvents[environmentID]
	s.dockerHostEventMu.Unlock()
	if subscribed {
		return
	}
	// The subscription owns the event stream, so an environment's daemon is only watched while this
	// process serves it: cancelling this context stops delivery and the connection underneath it. The
	// target also attributes every recorded event to the environment that produced it.
	scoped, ok := provider.InProcessTargetContext(ctx, environmentID)
	if !ok {
		return
	}
	streamCtx, cancel := context.WithCancel(scoped)
	eventBus, ok := provider.EnvironmentEventBus(streamCtx, environmentID)
	if !ok || eventBus == nil {
		cancel()
		return
	}
	containers, unsubscribeContainers := eventBus.Subscribe(events.ContainerEventType, bus.WithSubscriberBuffer(daemonEventChanBuffer))
	images, unsubscribeImages := eventBus.Subscribe(events.ImageEventType, bus.WithSubscriberBuffer(daemonEventChanBuffer))
	volumes, unsubscribeVolumes := eventBus.Subscribe(events.VolumeEventType, bus.WithSubscriberBuffer(daemonEventChanBuffer))
	networks, unsubscribeNetworks := eventBus.Subscribe(events.NetworkEventType, bus.WithSubscriberBuffer(daemonEventChanBuffer))
	subscription := dockerHostEventSubscriptionInternal{
		cancel: cancel,
		unsubscribe: sync.OnceFunc(func() {
			unsubscribeContainers()
			unsubscribeImages()
			unsubscribeVolumes()
			unsubscribeNetworks()
		}),
	}
	s.dockerHostEventMu.Lock()
	if s.dockerHostEvents == nil {
		s.dockerHostEvents = make(map[string]dockerHostEventSubscriptionInternal, 1)
	}
	if _, exists := s.dockerHostEvents[environmentID]; exists {
		s.dockerHostEventMu.Unlock()
		subscription.stopInternal()
		return
	}
	s.dockerHostEvents[environmentID] = subscription
	s.dockerHostEventMu.Unlock()
	go func() {
		_ = s.runDockerEvents(streamCtx, containers, images, volumes, networks)
	}()
}

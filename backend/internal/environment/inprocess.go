package environment

import (
	"context"
	"strings"

	"go.getarcane.app/streams/bus"

	"github.com/getarcaneapp/arcane/backend/v2/pkg/libarcane/envcontext"
)

// InProcessTargetContext reports whether this process serves the environment's Docker daemon
// itself and, if so, returns a context scoped to that daemon.
//
// The manager serves its own daemon (LocalEnvironmentID) in-process. A "docker-host" environment
// is also served in-process — it has no agent to hand work to — so a caller that would otherwise
// proxy the request to a remote agent must run its own logic here with the environment's target in
// context. Agent environments report false so callers keep using the environment proxy.
func (s *EnvironmentService) InProcessTargetContext(ctx context.Context, environmentID string) (context.Context, bool) {
	if environmentID == "" || environmentID == LocalEnvironmentID {
		return ctx, true
	}
	// A request that already carries this environment's target needs no lookup.
	if target, ok := envcontext.TargetFromContext(ctx); ok && target.EnvironmentID == environmentID && target.DockerHost != "" {
		return ctx, true
	}
	if s == nil {
		return ctx, false
	}
	env, err := s.GetEnvironmentByIDCached(ctx, environmentID)
	if err != nil || env == nil || !env.IsDockerHost() {
		return ctx, false
	}
	return envcontext.WithTarget(ctx, envcontext.Target{EnvironmentID: env.ID, DockerHost: env.DockerHostURL()}), true
}

// EnvironmentEventBus returns the event bus of the Docker daemon this process serves for the
// environment and, as its second result, establishes that the daemon is served here.
//
// A "docker-host" environment has no agent, so this process both talks to its daemon and streams its
// events; the bus is per endpoint, which is what lets a consumer attribute an event to the
// environment that produced it. The manager's own daemon publishes on the shared bus and agent
// environments are watched by their agent, so both report false.
func (s *EnvironmentService) EnvironmentEventBus(ctx context.Context, environmentID string) (*bus.DockerEventBus, bool) {
	if environmentID == "" || environmentID == LocalEnvironmentID {
		return nil, false
	}
	scoped, ok := s.InProcessTargetContext(ctx, environmentID)
	if !ok {
		return nil, false
	}
	host := strings.TrimSpace(envcontext.DockerHostFromContext(scoped))
	if host == "" || s.dockerService == nil {
		return nil, false
	}
	// The stream must run with the environment's target in context, not the caller's bare one: the
	// target carries the endpoint's credentials and attributes every event it publishes.
	return s.dockerService.HostEventBus(scoped, host), true
}

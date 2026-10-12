package ws

import "context"

// newHubContext derives the context that owns a shared WebSocket hub (container
// stats, container/project/service logs).
//
// Hubs outlive the request that created them, so the context must not cancel
// with that request: an idle-teardown timer cancels it explicitly instead. It
// must, however, keep the request's values, because the request context carries
// the target environment (see the environment proxy middleware and
// pkg/libarcane/envcontext). Dropping those values made hub producers resolve
// their Docker client from nothing, so docker-host environments streamed from
// the manager's own daemon instead of the configured remote one.
func newHubContext(base context.Context) (context.Context, context.CancelFunc) {
	return context.WithCancel(context.WithoutCancel(base))
}

package ws

import (
	"context"
	"encoding/json/v2"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	systemtypes "github.com/getarcaneapp/arcane/types/v2/system"
	"github.com/labstack/echo/v5"

	wshub "github.com/getarcaneapp/arcane/backend/v2/pkg/libarcane/ws"
)

// ============================================================================
// Container Stats WebSocket Endpoint
// ============================================================================

// ContainerStats streams container stats over WebSocket.
//
//	@Summary		Get container stats via WebSocket
//	@Description	Stream container resource statistics over WebSocket connection
//	@Tags			WebSocket
//	@Param			id			path	string	true	"Environment ID"
//	@Param			containerId	path	string	true	"Container ID"
//	@Router			/api/environments/{id}/ws/containers/{containerId}/stats [get]
func (h *WebSocketHandler) ContainerStats(c *echo.Context) error {
	containerID := c.Param("containerId")
	if strings.TrimSpace(containerID) == "" {
		return c.JSON(http.StatusBadRequest, map[string]any{"success": false, "error": "Container ID is required"})
	}

	conn, onRemove, ok := h.acceptWS(c, systemtypes.WSKindContainerStats, containerID)
	if !ok {
		return nil
	}

	// A reconnect can land exactly on the 5s idle-teardown boundary and load a
	// hub whose Run has already exited. Drop that hub and retry once so the
	// client gets a live producer instead of a silent socket.
	// The hub owns connection cleanup; preserve request values without request cancellation.
	// Request values carry the target Docker daemon, so the producer samples the
	// environment this socket belongs to instead of the manager's own daemon.
	baseCtx := context.WithoutCancel(c.Request().Context())
	hubKey := containerStatsHubKey(c.Param("id"), containerID)
	for attempt := range 2 {
		hub := h.getOrCreateContainerStatsHub(hubKey, containerID, baseCtx)
		if wshub.ServeClientWithOnRemove(baseCtx, hub, conn, onRemove) {
			return nil
		}
		h.containerStatsHubs.CompareAndDelete(hubKey, hub)
		slog.DebugContext(c.Request().Context(), "container stats hub stopped before client registration; retrying",
			"containerId", containerID, "attempt", attempt+1)
	}

	slog.WarnContext(c.Request().Context(), "failed to register container stats client", "containerId", containerID)
	_ = conn.CloseNow()
	onRemove()
	return nil
}

// containerStatsHubKey scopes a stats hub to one environment. Container IDs are
// only unique per daemon, so a key without the environment id would let two
// environments share (and leak) one producer's samples.
func containerStatsHubKey(envID, containerID string) string {
	return envID + "\x00" + containerID
}

func (h *WebSocketHandler) getOrCreateContainerStatsHub(key, containerID string, base context.Context) *wshub.Hub {
	if existing, ok := h.containerStatsHubs.Load(key); ok {
		if hub, localOk := existing.(*wshub.Hub); localOk {
			return hub
		}
	}

	hub := wshub.NewHub(64)
	actual, loaded := h.containerStatsHubs.LoadOrStore(key, hub)
	if loaded {
		if existingHub, ok := actual.(*wshub.Hub); ok {
			return existingHub
		}
		// type assertion failure is impossible in practice, but avoid running
		// an unregistered hub if it somehow occurs
		return hub
	}

	h.runContainerStatsHub(key, containerID, base, hub)
	return hub
}

// containerStatsErrorDrainGrace is how long a failed stats hub stays alive after
// broadcasting its error frame, so clients receive it before the hub shuts down.
const containerStatsErrorDrainGrace = 2 * time.Second

// runContainerStatsHub owns the producer for one environment/container pair. The
// hub outlives individual requests, so it must not cancel with them, but it must
// keep their values: the request context carries the target Docker daemon.
func (h *WebSocketHandler) runContainerStatsHub(key, containerID string, base context.Context, hub *wshub.Hub) {
	ctx, cancel := newHubContext(base)
	var cleanupTimer *time.Timer
	var cleanupTimerMu sync.Mutex

	hub.SetOnEmpty(func() {
		cleanupTimerMu.Lock()
		if cleanupTimer != nil {
			cleanupTimer.Stop()
		}
		var timer *time.Timer
		timer = time.AfterFunc(5*time.Second, func() {
			cleanupTimerMu.Lock()
			defer cleanupTimerMu.Unlock()
			if cleanupTimer != timer {
				return
			}
			h.containerStatsHubs.CompareAndDelete(key, hub)
			slog.DebugContext(ctx, "container stats hub idle, cleaning up upstream stream", "containerId", containerID)
			cleanupTimer = nil
			cancel()
		})
		cleanupTimer = timer
		cleanupTimerMu.Unlock()
	})
	hub.SetOnActive(func() {
		cleanupTimerMu.Lock()
		if cleanupTimer != nil {
			cleanupTimer.Stop()
			cleanupTimer = nil
		}
		cleanupTimerMu.Unlock()
	})

	go hub.Run(ctx)

	statsChan := make(chan any, 64)
	go func(ctx context.Context) {
		defer close(statsChan)

		err := h.containerService.StreamStats(ctx, containerID, statsChan)
		h.containerStatsHubs.CompareAndDelete(key, hub)
		if err == nil || errors.Is(err, context.Canceled) || ctx.Err() != nil {
			cancel()
			return
		}

		// The producer died but the hub stayed cached, so clients kept an open
		// socket that would never emit another sample. Tell them why, then drop
		// the hub so the next connect rebuilds a producer instead of attaching
		// to this dead one.
		slog.WarnContext(ctx, "container stats stream failed", "containerId", containerID, "error", err)
		if b, marshalErr := json.Marshal(map[string]any{
			"error":     "Failed to stream container stats: " + err.Error(),
			"timestamp": wshub.NowRFC3339(),
		}); marshalErr == nil {
			hub.Broadcast(b)
		}

		// Cancelling immediately would race hub.Run between ctx.Done and the
		// queued error frame, dropping the very message clients need. Give the
		// broadcast a moment to drain, then tear the hub down.
		time.AfterFunc(containerStatsErrorDrainGrace, cancel)
	}(ctx)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case stats, ok := <-statsChan:
				if !ok {
					return
				}
				if b, err := json.Marshal(stats); err == nil {
					hub.Broadcast(b)
				}
			}
		}
	}()
}

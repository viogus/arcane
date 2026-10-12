package activity

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/samber/mo"
	"github.com/stretchr/testify/require"

	"github.com/getarcaneapp/arcane/types/v2/activity"

	"github.com/getarcaneapp/arcane/backend/v2/internal/environment"
	"github.com/getarcaneapp/arcane/backend/v2/pkg/libarcane/envcontext"
	"github.com/getarcaneapp/arcane/backend/v2/pkg/utils/handlerutil"
)

const dockerHostTestEnvironmentIDInternal = "env-docker-host"

// newDockerHostTestHandlerInternal returns a handler whose environment lookup knows exactly one
// docker-host environment and whose proxy records every call to an agent.
func newDockerHostTestHandlerInternal(t *testing.T) (*ActivityHandler, *int) {
	t.Helper()

	db := setupActivityServiceTestDBInternal(t)
	service := NewActivityService(db, nil)

	require.NoError(t, db.WithContext(t.Context()).Create(&Activity{
		EnvironmentID: dockerHostTestEnvironmentIDInternal,
		Type:          activity.TypeImagePull,
		Status:        activity.StatusSuccess,
		ResourceName:  new("nginx:latest"),
		StartedAt:     time.Now().UTC(),
	}).Error)

	dockerHost := "tcp://192.168.2.33:6001"
	proxyCalls := 0
	handler := NewHandler(service, EnvironmentDependencies{
		GetActiveRemoteEnvironment: func(environmentID string) mo.Option[environment.Environment] {
			if environmentID != dockerHostTestEnvironmentIDInternal {
				return mo.None[environment.Environment]()
			}
			return mo.Some(environment.Environment{
				ID:         dockerHostTestEnvironmentIDInternal,
				Name:       "arm-direct",
				DockerHost: &dockerHost,
			})
		},
		ProxyJSONRequest: handlerutil.RemoteJSONProxy(func(context.Context, string, string, string, []byte, any) error {
			proxyCalls++
			return errors.New("a docker-host environment must not be proxied to an agent")
		}),
	})

	return handler, &proxyCalls
}

// TestActivityHandler_ListActivitiesServesDockerHostEnvironmentInProcess covers the docker-host
// activity feed: the manager records those rows itself, so listing must read them locally instead
// of proxying to an agent that does not exist.
func TestActivityHandler_ListActivitiesServesDockerHostEnvironmentInProcess(t *testing.T) {
	handler, proxyCalls := newDockerHostTestHandlerInternal(t)

	page, err := handler.ListActivities(t.Context(), &ListActivitiesInput{
		EnvironmentID: dockerHostTestEnvironmentIDInternal,
		Limit:         50,
		Order:         "desc",
	})
	require.NoError(t, err)
	require.Len(t, page.Body.Data, 1)
	require.Equal(t, activity.TypeImagePull, page.Body.Data[0].Type)
	require.Equal(t, dockerHostTestEnvironmentIDInternal, page.Body.Data[0].SourceEnvironmentID)
	require.Zero(t, *proxyCalls, "docker-host activity rows are served in-process")
}

// TestActivityHandler_ListActivitiesServesDockerHostEnvironmentWithoutCacheEntry covers the request
// path that produced HTTP 500 in production: docker-host environments have no agent heartbeat, so the
// active-remote-environment cache can miss them. The middleware attaches the resolved docker-host
// target to the request context, which must be enough to serve the rows in-process.
func TestActivityHandler_ListActivitiesServesDockerHostEnvironmentWithoutCacheEntry(t *testing.T) {
	db := setupActivityServiceTestDBInternal(t)
	service := NewActivityService(db, nil)

	require.NoError(t, db.WithContext(t.Context()).Create(&Activity{
		EnvironmentID: dockerHostTestEnvironmentIDInternal,
		Type:          activity.TypeImagePull,
		Status:        activity.StatusSuccess,
		StartedAt:     time.Now().UTC(),
	}).Error)

	proxyCalls := 0
	handler := NewHandler(service, EnvironmentDependencies{
		GetActiveRemoteEnvironment: func(string) mo.Option[environment.Environment] {
			return mo.None[environment.Environment]()
		},
		ProxyJSONRequest: handlerutil.RemoteJSONProxy(func(context.Context, string, string, string, []byte, any) error {
			proxyCalls++
			return errors.New("a docker-host environment must not be proxied to an agent")
		}),
	})

	ctx := envcontext.WithTarget(t.Context(), envcontext.Target{
		EnvironmentID: dockerHostTestEnvironmentIDInternal,
		DockerHost:    "tcp://192.168.2.33:6001",
	})

	page, err := handler.ListActivities(ctx, &ListActivitiesInput{
		EnvironmentID: dockerHostTestEnvironmentIDInternal,
		Limit:         50,
		Order:         "desc",
	})
	require.NoError(t, err)
	require.Len(t, page.Body.Data, 1)
	require.Zero(t, proxyCalls, "the resolved docker-host target serves the rows in-process")

	// Without the target the same input still takes the proxy path.
	_, err = handler.ListActivities(t.Context(), &ListActivitiesInput{
		EnvironmentID: dockerHostTestEnvironmentIDInternal,
		Limit:         50,
		Order:         "desc",
	})
	require.Error(t, err)
	require.Equal(t, 1, proxyCalls)
}

// TestActivityHandler_ListActivitiesStillProxiesAgentEnvironments guards the other side of the
// contract: an agent environment must keep going through the remote proxy.
func TestActivityHandler_ListActivitiesStillProxiesAgentEnvironments(t *testing.T) {
	handler, proxyCalls := newDockerHostTestHandlerInternal(t)

	_, err := handler.ListActivities(t.Context(), &ListActivitiesInput{
		EnvironmentID: "env-agent",
		Limit:         50,
		Order:         "desc",
	})
	require.Error(t, err)
	require.Equal(t, 1, *proxyCalls, "agent environments are still fetched through the remote proxy")
}

// TestActivityHandler_StreamsDockerHostEnvironmentInProcess is the stream-side half of the same
// contract: the per-environment activity poller must publish a snapshot, never a proxy error.
func TestActivityHandler_StreamsDockerHostEnvironmentInProcess(t *testing.T) {
	handler, proxyCalls := newDockerHostTestHandlerInternal(t)

	dockerHost := "tcp://192.168.2.33:6001"
	dockerHostEnvironment := environment.Environment{
		ID:         dockerHostTestEnvironmentIDInternal,
		Name:       "arm-direct",
		DockerHost: &dockerHost,
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	events := make(chan activity.StreamEvent, 16)
	go handler.runRemoteActivityStreamPollerInternal(ctx, dockerHostEnvironment, 50, func(event activity.StreamEvent) {
		select {
		case events <- event:
		default:
		}
	})

	snapshotEvent := receiveActivityStreamEventInternal(t, events)
	require.Equal(t, "snapshot", snapshotEvent.Type)
	require.Equal(t, dockerHostTestEnvironmentIDInternal, snapshotEvent.EnvironmentID)
	require.Len(t, snapshotEvent.Activities, 1)
	require.Zero(t, *proxyCalls, "the docker-host stream reads local rows")

	select {
	case event := <-events:
		t.Fatalf("unexpected extra activity stream event: %+v", event)
	case <-time.After(200 * time.Millisecond):
	}
}

// TestActivityHandler_StreamsDockerHostEnvironmentWithoutCacheEntry is the stream-side regression:
// the snapshot cache has no entry (nothing heartbeats a docker-host environment), so the poller must
// fall back to the environment it was handed instead of returning before publishing a snapshot.
func TestActivityHandler_StreamsDockerHostEnvironmentWithoutCacheEntry(t *testing.T) {
	db := setupActivityServiceTestDBInternal(t)
	service := NewActivityService(db, nil)

	require.NoError(t, db.WithContext(t.Context()).Create(&Activity{
		EnvironmentID: dockerHostTestEnvironmentIDInternal,
		Type:          activity.TypeImagePull,
		Status:        activity.StatusSuccess,
		ResourceName:  new("nginx:latest"),
		StartedAt:     time.Now().UTC(),
	}).Error)

	proxyCalls := 0
	handler := NewHandler(service, EnvironmentDependencies{
		GetActiveRemoteEnvironment: func(string) mo.Option[environment.Environment] {
			return mo.None[environment.Environment]()
		},
		ProxyJSONRequest: handlerutil.RemoteJSONProxy(func(context.Context, string, string, string, []byte, any) error {
			proxyCalls++
			return errors.New("a docker-host environment must not be proxied to an agent")
		}),
	})

	dockerHost := "tcp://192.168.2.33:6001"
	dockerHostEnvironment := environment.Environment{
		ID:         dockerHostTestEnvironmentIDInternal,
		Name:       "arm-direct",
		DockerHost: &dockerHost,
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	events := make(chan activity.StreamEvent, 16)
	go handler.runRemoteActivityStreamPollerInternal(ctx, dockerHostEnvironment, 50, func(event activity.StreamEvent) {
		select {
		case events <- event:
		default:
		}
	})

	snapshotEvent := receiveActivityStreamEventInternal(t, events)
	require.Equal(t, "snapshot", snapshotEvent.Type)
	require.Equal(t, dockerHostTestEnvironmentIDInternal, snapshotEvent.EnvironmentID)
	require.Len(t, snapshotEvent.Activities, 1)
	require.Zero(t, proxyCalls, "the docker-host stream reads local rows without the snapshot cache")
}

func receiveActivityStreamEventInternal(t *testing.T, events <-chan activity.StreamEvent) activity.StreamEvent {
	t.Helper()

	select {
	case event := <-events:
		return event
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for an activity stream event")
		return activity.StreamEvent{}
	}
}

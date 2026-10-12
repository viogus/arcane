package environment

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/getarcaneapp/arcane/backend/v2/internal/config"
	dockersvc "github.com/getarcaneapp/arcane/backend/v2/internal/docker"
	"github.com/getarcaneapp/arcane/backend/v2/internal/event"
)

// Docker clients for docker-host environments are cached per endpoint. An environment whose
// endpoint changes - or that is deleted - must drop the client cached for the old endpoint,
// otherwise every edit leaks a client (and its keep-alive connections) for the process lifetime.
func TestEnvironmentService_DropsCachedDockerHostClientOnUpdateAndDelete(t *testing.T) {
	ctx := t.Context()
	db := setupEnvironmentServiceTestDB(t)

	newPingServer := func() (*httptest.Server, *atomic.Int64) {
		var pings atomic.Int64
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			pings.Add(1)
			w.Header().Set("API-Version", "1.45")
			w.Header().Set("Content-Type", "text/plain")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("OK"))
		}))
		t.Cleanup(server.Close)
		return server, &pings
	}

	endpointA, pingsA := newPingServer()
	endpointB, pingsB := newPingServer()

	dockerService := dockersvc.NewDockerClientService(ctx, db, &config.Config{DockerHost: "tcp://127.0.0.1:1"}, nil)
	svc := NewEnvironmentService(db, http.DefaultClient, dockerService, event.NewEventService(db, nil, nil), nil, nil)

	now := time.Now()
	require.NoError(t, db.WithContext(ctx).Create(&Environment{
		ID: "env-dockerhost", CreatedAt: now, UpdatedAt: &now, Name: "Remote Docker",
		DockerHost: new(endpointA.URL), Status: string(EnvironmentStatusOnline), Enabled: true,
	}).Error)

	// The endpoint connects once and is reused afterwards.
	for range 2 {
		_, err := dockerService.GetHostClient(ctx, endpointA.URL, "")
		require.NoError(t, err)
	}
	require.EqualValues(t, 1, pingsA.Load(), "the second lookup must reuse the cached client")

	// An update that leaves the endpoint alone must not drop (and reconnect) the client.
	_, err := svc.UpdateEnvironment(ctx, "env-dockerhost", map[string]any{"name": "Renamed"}, nil, nil)
	require.NoError(t, err)
	require.EqualValues(t, 1, pingsA.Load(), "an unrelated update must keep the cached client")

	// Re-pointing the environment at another endpoint releases the old client.
	_, err = svc.UpdateEnvironment(ctx, "env-dockerhost", map[string]any{"docker_host": endpointB.URL}, nil, nil)
	require.NoError(t, err)

	_, err = dockerService.GetHostClient(ctx, endpointA.URL, "")
	require.NoError(t, err)
	require.EqualValues(t, 2, pingsA.Load(), "changing the endpoint must drop the cached client")

	for range 2 {
		_, err = dockerService.GetHostClient(ctx, endpointB.URL, "")
		require.NoError(t, err)
	}
	require.EqualValues(t, 1, pingsB.Load(), "the new endpoint must be cached like any other")

	// Deleting the environment releases the client cached for its endpoint.
	require.NoError(t, svc.DeleteEnvironment(ctx, "env-dockerhost", nil, nil))

	_, err = dockerService.GetHostClient(ctx, endpointB.URL, "")
	require.NoError(t, err)
	require.EqualValues(t, 2, pingsB.Load(), "deleting the environment must drop the cached client")
}

// "Test connection" against an edited-but-unsaved endpoint of a docker-host environment must probe
// that endpoint as a Docker API, even though the request carries it in the custom-URL field.
func TestEnvironmentService_TestConnectionProbesCustomDockerHostAsDockerAPI(t *testing.T) {
	ctx := t.Context()
	db := setupEnvironmentServiceTestDB(t)

	var mu sync.Mutex
	var paths []string
	newDockerEndpoint := func() *httptest.Server {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			paths = append(paths, r.URL.Path)
			mu.Unlock()
			w.Header().Set("API-Version", "1.45")
			w.Header().Set("Content-Type", "text/plain")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("OK"))
		}))
		t.Cleanup(server.Close)
		return server
	}

	savedEndpoint := newDockerEndpoint()
	editedEndpoint := newDockerEndpoint()

	dockerService := dockersvc.NewDockerClientService(ctx, db, &config.Config{DockerHost: "tcp://127.0.0.1:1"}, nil)
	svc := NewEnvironmentService(db, http.DefaultClient, dockerService, event.NewEventService(db, nil, nil), nil, nil)

	now := time.Now()
	require.NoError(t, db.WithContext(ctx).Create(&Environment{
		ID: "env-dockerhost-custom", CreatedAt: now, UpdatedAt: &now, Name: "Remote Docker",
		DockerHost: new(savedEndpoint.URL), Status: string(EnvironmentStatusOnline), Enabled: true,
	}).Error)

	status, err := svc.TestConnection(ctx, "env-dockerhost-custom", &editedEndpoint.URL)
	require.NoError(t, err)
	require.Equal(t, string(EnvironmentStatusOnline), status)

	mu.Lock()
	defer mu.Unlock()
	require.NotEmpty(t, paths, "the edited endpoint must be contacted")
	require.NotContains(t, paths, "/api/health", "a docker-host endpoint must not be probed as an agent")
}

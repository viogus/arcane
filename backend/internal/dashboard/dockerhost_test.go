package dashboard

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/getarcaneapp/arcane/types/v2/dashboard"
	dockercontainer "github.com/moby/moby/api/types/container"
	dockerimage "github.com/moby/moby/api/types/image"
	dockervolume "github.com/moby/moby/api/types/volume"
	"github.com/stretchr/testify/require"

	"github.com/getarcaneapp/arcane/backend/v2/internal/config"
	"github.com/getarcaneapp/arcane/backend/v2/internal/environment"
	"github.com/getarcaneapp/arcane/backend/v2/internal/image"
	"github.com/getarcaneapp/arcane/backend/v2/internal/podman"
	"github.com/getarcaneapp/arcane/backend/v2/internal/project"
	"github.com/getarcaneapp/arcane/backend/v2/internal/version"
	"github.com/getarcaneapp/arcane/backend/v2/internal/volume"
	"github.com/getarcaneapp/arcane/backend/v2/pkg/libarcane/envcontext"
)

// dockerHostServerVersionInternal is the Docker engine version the fake docker-host daemon reports.
const dockerHostServerVersionInternal = "27.3.1"

// newDockerHostTestServerInternal serves just enough of the Docker API to build a dashboard
// snapshot, including the /_ping negotiation a docker-host client performs on first use.
func newDockerHostTestServerInternal(t *testing.T, containers []dockercontainer.Summary) *httptest.Server {
	t.Helper()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/_ping") {
			w.Header().Set("Api-Version", "1.41")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("OK"))
			return
		}

		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/version"):
			_ = json.NewEncoder(w).Encode(map[string]any{"Version": dockerHostServerVersionInternal, "ApiVersion": "1.41"})
		case strings.HasSuffix(r.URL.Path, "/containers/json"):
			_ = json.NewEncoder(w).Encode(containers)
		case strings.HasSuffix(r.URL.Path, "/images/json"):
			_ = json.NewEncoder(w).Encode([]dockerimage.Summary{})
		case strings.HasSuffix(r.URL.Path, "/volumes"):
			_ = json.NewEncoder(w).Encode(map[string]any{"Volumes": []dockervolume.Volume{}, "Warnings": []string{}})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)

	return server
}

func newDashboardDockerHostTestServiceInternal(t *testing.T, localContainers []dockercontainer.Summary) *DashboardService {
	t.Helper()

	db, settingsSvc := setupDashboardServiceTestDB(t)
	dockerSvc := newDashboardTestDockerService(t, settingsSvc, localContainers, nil, nil)
	imageSvc := image.NewImageService(db, nil, nil, nil, nil, nil, nil, nil)
	projectSvc := project.NewProjectService(db, settingsSvc, nil, imageSvc, nil, nil, nil, nil, config.Load(), nil, nil)

	return NewDashboardService(
		db,
		dockerSvc,
		nil,
		projectSvc,
		imageSvc,
		settingsSvc,
		nil,
		nil,
		version.NewVersionService(nil, true, "2.16.0-test", "revision", nil, nil, nil, nil),
		volume.NewVolumeService(db, nil, nil, nil, nil, nil, nil, nil, nil, nil),
		podman.NewService(podman.Config{Engine: dockerSvc.Engine}),
	)
}

func dockerHostTestContainerInternal(id string) dockercontainer.Summary {
	return dockercontainer.Summary{
		ID:      id,
		Names:   []string{"/" + id},
		Image:   "repo/app:stable",
		ImageID: "sha256:" + id,
		Created: 1700000000,
		State:   "running",
		Status:  "Up 1 hour",
		Labels:  map[string]string{},
	}
}

// TestDashboardSnapshotUsesDockerHostDaemon guards the docker-host environment contract: a snapshot
// built with an environment-scoped context must describe that environment's daemon, not the
// manager's own, and must not share the local snapshot cache slot.
func TestDashboardSnapshotUsesDockerHostDaemon(t *testing.T) {
	local := []dockercontainer.Summary{
		dockerHostTestContainerInternal("local-a"),
		dockerHostTestContainerInternal("local-b"),
		dockerHostTestContainerInternal("local-c"),
	}
	service := newDashboardDockerHostTestServiceInternal(t, local)

	dockerHostServer := newDockerHostTestServerInternal(t, []dockercontainer.Summary{
		dockerHostTestContainerInternal("remote-a"),
	})

	localSnapshot, err := service.GetSnapshot(t.Context(), DashboardActionItemsOptions{}, false)
	require.NoError(t, err)
	require.Equal(t, 3, localSnapshot.Containers.Counts.TotalContainers, "without a target the manager's own daemon answers")

	envCtx := envcontext.WithTarget(t.Context(), envcontext.Target{
		EnvironmentID: "env-docker-host",
		DockerHost:    dockerHostServer.URL,
	})
	remoteSnapshot, err := service.GetSnapshot(envCtx, DashboardActionItemsOptions{}, false)
	require.NoError(t, err)
	require.Equal(t, 1, remoteSnapshot.Containers.Counts.TotalContainers, "the environment-scoped context selects that environment's daemon")
	require.Equal(t, 1, remoteSnapshot.Containers.Counts.RunningContainers)
	require.Equal(t, dockerHostServerVersionInternal, remoteSnapshot.DockerVersion, "a docker-host environment reports its own daemon version")
	require.Nil(t, remoteSnapshot.VersionInfo, "a docker-host environment runs no Arcane instance to report a version for")
	require.Empty(t, localSnapshot.DockerVersion, "only docker-host targets report a daemon version")
	require.NotNil(t, localSnapshot.VersionInfo, "the manager's own environment still reports the application version")
}

// TestDashboardHandler_StreamsDockerHostEnvironmentInProcess reproduces the reported bug: the
// per-environment dashboard stream poller proxied every remote environment to an agent, so a
// docker-host environment streamed an error instead of a snapshot.
func TestDashboardHandler_StreamsDockerHostEnvironmentInProcess(t *testing.T) {
	service := newDashboardDockerHostTestServiceInternal(t, []dockercontainer.Summary{
		dockerHostTestContainerInternal("local-a"),
		dockerHostTestContainerInternal("local-b"),
		dockerHostTestContainerInternal("local-c"),
	})

	dockerHostServer := newDockerHostTestServerInternal(t, []dockercontainer.Summary{
		dockerHostTestContainerInternal("remote-a"),
	})

	// The handler's environment service is nil: the docker-host branch must not touch it, and the
	// agent proxy branch would panic on it.
	handler := &DashboardHandler{dashboardService: service}

	dockerHost := dockerHostServer.URL
	dockerHostEnvironment := environment.Environment{
		ID:         "env-docker-host",
		Name:       "arm-direct",
		IsEdge:     true,
		DockerHost: &dockerHost,
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	events := make(chan dashboard.StreamEvent, 16)
	go handler.runRemoteDashboardStreamPollerInternal(ctx, dockerHostEnvironment, false, func(event dashboard.StreamEvent) {
		select {
		case events <- event:
		default:
		}
	})

	require.Equal(t, "pending", receiveDashboardStreamEventInternal(t, events).Type)

	snapshotEvent := receiveDashboardStreamEventInternal(t, events)
	require.Equal(t, "snapshot", snapshotEvent.Type)
	require.Equal(t, dockerHostEnvironment.ID, snapshotEvent.EnvironmentID)
	require.NotNil(t, snapshotEvent.Snapshot)
	require.Equal(t, 1, snapshotEvent.Snapshot.Containers.Counts.TotalContainers)

	select {
	case event := <-events:
		t.Fatalf("unexpected extra dashboard stream event: %+v", event)
	case <-time.After(200 * time.Millisecond):
	}
}

// TestDashboardHandler_StreamsDockerHostEnvironmentWithoutCacheEntry covers the production shape of
// the same bug: the environment service is wired (non-nil) but its in-process snapshot cache has no
// entry for the docker-host environment, because nothing ever heartbeats it. The poller must keep
// using the environment it was handed instead of returning without publishing anything.
func TestDashboardHandler_StreamsDockerHostEnvironmentWithoutCacheEntry(t *testing.T) {
	service := newDashboardDockerHostTestServiceInternal(t, []dockercontainer.Summary{
		dockerHostTestContainerInternal("local-a"),
	})

	dockerHostServer := newDockerHostTestServerInternal(t, []dockercontainer.Summary{
		dockerHostTestContainerInternal("remote-a"),
		dockerHostTestContainerInternal("remote-b"),
	})

	db, _ := setupDashboardServiceTestDB(t)
	handler := &DashboardHandler{
		dashboardService:   service,
		environmentService: environment.NewEnvironmentService(db, nil, nil, nil, nil, nil),
	}

	dockerHost := dockerHostServer.URL
	dockerHostEnvironment := environment.Environment{
		ID:         "env-docker-host-cold",
		Name:       "arm-direct",
		IsEdge:     true,
		DockerHost: &dockerHost,
	}
	require.True(t, handler.environmentService.GetActiveRemoteEnvironmentSnapshot(dockerHostEnvironment.ID).IsAbsent(),
		"the test needs an environment the snapshot cache has never seen")

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	events := make(chan dashboard.StreamEvent, 16)
	go handler.runRemoteDashboardStreamPollerInternal(ctx, dockerHostEnvironment, false, func(event dashboard.StreamEvent) {
		select {
		case events <- event:
		default:
		}
	})

	require.Equal(t, "pending", receiveDashboardStreamEventInternal(t, events).Type)

	snapshotEvent := receiveDashboardStreamEventInternal(t, events)
	require.Equal(t, "snapshot", snapshotEvent.Type)
	require.NotNil(t, snapshotEvent.Snapshot)
	require.Equal(t, 2, snapshotEvent.Snapshot.Containers.Counts.TotalContainers)
}

func receiveDashboardStreamEventInternal(t *testing.T, events <-chan dashboard.StreamEvent) dashboard.StreamEvent {
	t.Helper()

	select {
	case event := <-events:
		return event
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for a dashboard stream event")
		return dashboard.StreamEvent{}
	}
}

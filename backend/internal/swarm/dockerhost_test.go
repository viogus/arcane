package swarm

import (
	"context"
	stdjson "encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	swarmtypes "github.com/getarcaneapp/arcane/types/v2/swarm"
	"github.com/libtnb/sqlite"
	"github.com/moby/moby/api/types/swarm"
	"github.com/moby/moby/api/types/system"
	"github.com/moby/moby/client"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"github.com/getarcaneapp/arcane/backend/v2/internal/config"
	"github.com/getarcaneapp/arcane/backend/v2/internal/database"
	"github.com/getarcaneapp/arcane/backend/v2/internal/docker"
	"github.com/getarcaneapp/arcane/backend/v2/internal/environment"
	"github.com/getarcaneapp/arcane/backend/v2/internal/kv"
	"github.com/getarcaneapp/arcane/backend/v2/internal/settings"
	"github.com/getarcaneapp/arcane/backend/v2/pkg/libarcane/envcontext"
)

// swarmDockerHostDaemonInternal is a fake Docker daemon for the docker-host
// swarm tests. It records the paths it served so a test can prove an operation
// ran against this daemon instead of being proxied to an agent.
type swarmDockerHostDaemonInternal struct {
	mu               sync.Mutex
	state            swarm.LocalNodeState
	controlAvailable bool
	paths            []string
}

func (d *swarmDockerHostDaemonInternal) setStateInternal(state swarm.LocalNodeState, controlAvailable bool) {
	d.mu.Lock()
	d.state = state
	d.controlAvailable = controlAvailable
	d.mu.Unlock()
}

func (d *swarmDockerHostDaemonInternal) recordPathInternal(path string) {
	d.mu.Lock()
	d.paths = append(d.paths, path)
	d.mu.Unlock()
}

func (d *swarmDockerHostDaemonInternal) sawPathSuffixInternal(suffix string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, path := range d.paths {
		if strings.HasSuffix(path, suffix) {
			return true
		}
	}
	return false
}

func (d *swarmDockerHostDaemonInternal) newServerInternal(t *testing.T) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		d.recordPathInternal(r.URL.Path)
		w.Header().Set("Content-Type", "application/json")

		switch {
		case r.URL.Path == "/_ping":
			w.Header().Set("Api-Version", "1.41")
		case strings.HasSuffix(r.URL.Path, "/info"):
			d.mu.Lock()
			info := system.Info{Swarm: swarm.Info{
				LocalNodeState:   d.state,
				ControlAvailable: d.controlAvailable,
			}}
			d.mu.Unlock()
			_ = stdjson.NewEncoder(w).Encode(info)
		case strings.HasSuffix(r.URL.Path, "/swarm/init"):
			_, _ = w.Write([]byte(`"node-1"`))
		case strings.HasSuffix(r.URL.Path, "/swarm/join"):
			w.WriteHeader(http.StatusOK)
		case strings.HasSuffix(r.URL.Path, "/swarm/leave"):
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	return server
}

type swarmDockerHostTestFixtureInternal struct {
	db                 *database.DB
	kvService          *kv.KVService
	docker             *docker.DockerClientService
	environmentService *environment.EnvironmentService
	daemon             *swarmDockerHostDaemonInternal
	server             *httptest.Server
}

// newSwarmDockerHostTestFixtureInternal builds a manager-like SwarmService
// backed by an in-memory DB and a fake daemon that serves both as the manager's
// own daemon (environment.LocalEnvironmentID) and as the daemon a docker-host
// environment points at.
func newSwarmDockerHostTestFixtureInternal(t *testing.T, state swarm.LocalNodeState, controlAvailable bool) *swarmDockerHostTestFixtureInternal {
	t.Helper()
	ctx := t.Context()

	gormDB, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, gormDB.AutoMigrate(
		&kv.KVEntry{},
		&settings.SettingVariable{},
		&settings.EnvironmentSetting{},
		&environment.Environment{},
	))
	db := &database.DB{DB: gormDB}

	settingsService, err := settings.NewSettingsService(ctx, db, &config.Config{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = settingsService.Stop(context.WithoutCancel(t.Context())) })

	environmentService := environment.NewEnvironmentService(db, nil, nil, nil, settingsService, nil)

	daemon := &swarmDockerHostDaemonInternal{state: state, controlAvailable: controlAvailable}
	server := daemon.newServerInternal(t)

	localClient, err := client.New(
		client.WithHost(server.URL),
		client.WithAPIVersion("1.41"),
		client.WithHTTPClient(server.Client()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = localClient.Close() })

	return &swarmDockerHostTestFixtureInternal{
		db:                 db,
		kvService:          kv.NewKVService(db),
		docker:             &docker.DockerClientService{Client: localClient},
		environmentService: environmentService,
		daemon:             daemon,
		server:             server,
	}
}

func dockerHostTargetContextInternal(ctx context.Context, dockerHost string) context.Context {
	return envcontext.WithTarget(ctx, envcontext.Target{EnvironmentID: "dh-1", DockerHost: dockerHost})
}

func TestSwarmService_IsEnabledForContext_DockerHostInactiveBeatsGlobalFlag(t *testing.T) {
	fixture := newSwarmDockerHostTestFixtureInternal(t, swarm.LocalNodeStateInactive, false)
	ctx := t.Context()
	require.NoError(t, fixture.kvService.SetBool(ctx, KVKeySwarmEnabled, true))

	svc := NewSwarmService(fixture.docker, nil, fixture.kvService, nil, fixture.environmentService)
	dockerHostCtx := dockerHostTargetContextInternal(ctx, fixture.server.URL)

	require.False(t, svc.IsEnabledForContext(dockerHostCtx),
		"an inactive docker-host daemon must report disabled even when the manager flag is true")
}

func TestSwarmService_IsEnabledForContext_DockerHostActiveBeatsGlobalFlag(t *testing.T) {
	fixture := newSwarmDockerHostTestFixtureInternal(t, swarm.LocalNodeStateActive, true)
	ctx := t.Context()
	require.NoError(t, fixture.kvService.SetBool(ctx, KVKeySwarmEnabled, false))

	svc := NewSwarmService(fixture.docker, nil, fixture.kvService, nil, fixture.environmentService)
	dockerHostCtx := dockerHostTargetContextInternal(ctx, fixture.server.URL)

	require.True(t, svc.IsEnabledForContext(dockerHostCtx),
		"an active docker-host daemon must report enabled even when the manager flag is false")
}

func TestSwarmService_IsEnabledForContext_LocalEnvironmentUsesOwnDaemon(t *testing.T) {
	fixture := newSwarmDockerHostTestFixtureInternal(t, swarm.LocalNodeStateInactive, false)
	ctx := t.Context()
	require.NoError(t, fixture.kvService.SetBool(ctx, KVKeySwarmEnabled, true))

	svc := NewSwarmService(fixture.docker, nil, fixture.kvService, nil, fixture.environmentService)

	require.False(t, svc.IsEnabledForContext(ctx), "the local environment follows the manager's own daemon")

	fixture.daemon.setStateInternal(swarm.LocalNodeStateActive, true)
	require.True(t, svc.IsEnabledForContext(ctx))
}

func TestSwarmService_IsEnabledForContext_DockerHostUnreachableNeverUsesGlobalFlag(t *testing.T) {
	fixture := newSwarmDockerHostTestFixtureInternal(t, swarm.LocalNodeStateInactive, false)
	ctx := t.Context()
	require.NoError(t, fixture.kvService.SetBool(ctx, KVKeySwarmEnabled, true))

	svc := NewSwarmService(fixture.docker, nil, fixture.kvService, nil, fixture.environmentService)
	unreachableCtx := dockerHostTargetContextInternal(ctx, "http://127.0.0.1:1")

	// The persisted flag describes the manager's own daemon, so it cannot answer for a docker-host
	// environment whose daemon could not be inspected.
	require.False(t, svc.IsEnabledForContext(unreachableCtx),
		"an unreachable docker-host daemon must report disabled instead of borrowing the manager flag")
}

func TestSwarmService_SwarmLifecycle_DockerHostDoesNotTouchGlobalFlag(t *testing.T) {
	fixture := newSwarmDockerHostTestFixtureInternal(t, swarm.LocalNodeStateInactive, false)
	ctx := t.Context()

	svc := NewSwarmService(fixture.docker, nil, fixture.kvService, nil, fixture.environmentService)
	dockerHostCtx := dockerHostTargetContextInternal(ctx, fixture.server.URL)

	_, err := svc.InitSwarm(dockerHostCtx, swarmtypes.SwarmInitRequest{Spec: stdjson.RawMessage("{}")})
	require.NoError(t, err)
	flag, err := fixture.kvService.GetBool(ctx, KVKeySwarmEnabled, false)
	require.NoError(t, err)
	require.False(t, flag, "swarm init against a docker-host must not set the manager's global flag")

	require.NoError(t, svc.JoinSwarm(dockerHostCtx, swarmtypes.SwarmJoinRequest{JoinToken: "join-token"}))
	flag, err = fixture.kvService.GetBool(ctx, KVKeySwarmEnabled, false)
	require.NoError(t, err)
	require.False(t, flag, "swarm join against a docker-host must not set the manager's global flag")

	fixture.daemon.setStateInternal(swarm.LocalNodeStateActive, true)
	require.NoError(t, svc.LeaveSwarm(dockerHostCtx, swarmtypes.SwarmLeaveRequest{Force: true}))
	flag, err = fixture.kvService.GetBool(ctx, KVKeySwarmEnabled, false)
	require.NoError(t, err)
	require.False(t, flag, "swarm leave against a docker-host must not clear the manager's global flag")

	// The operations really ran in-process against the environment's own daemon.
	require.True(t, fixture.daemon.sawPathSuffixInternal("/swarm/init"))
	require.True(t, fixture.daemon.sawPathSuffixInternal("/swarm/join"))
	require.True(t, fixture.daemon.sawPathSuffixInternal("/swarm/leave"))
}

func TestSwarmService_SwarmLifecycle_LocalEnvironmentKeepsPersistingGlobalFlag(t *testing.T) {
	fixture := newSwarmDockerHostTestFixtureInternal(t, swarm.LocalNodeStateInactive, false)
	ctx := t.Context()

	svc := NewSwarmService(fixture.docker, nil, fixture.kvService, nil, fixture.environmentService)

	_, err := svc.InitSwarm(ctx, swarmtypes.SwarmInitRequest{Spec: stdjson.RawMessage("{}")})
	require.NoError(t, err)
	flag, err := fixture.kvService.GetBool(ctx, KVKeySwarmEnabled, false)
	require.NoError(t, err)
	require.True(t, flag, "the local environment still persists the manager's swarm flag")

	fixture.daemon.setStateInternal(swarm.LocalNodeStateActive, true)
	require.NoError(t, svc.LeaveSwarm(ctx, swarmtypes.SwarmLeaveRequest{Force: true}))
	flag, err = fixture.kvService.GetBool(ctx, KVKeySwarmEnabled, false)
	require.NoError(t, err)
	require.False(t, flag, "leaving the local swarm still clears the manager's swarm flag")

	fixture.daemon.setStateInternal(swarm.LocalNodeStateInactive, false)
	require.NoError(t, svc.JoinSwarm(ctx, swarmtypes.SwarmJoinRequest{JoinToken: "join-token"}))
	flag, err = fixture.kvService.GetBool(ctx, KVKeySwarmEnabled, false)
	require.NoError(t, err)
	require.True(t, flag)
}

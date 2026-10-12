package nodes

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/moby/moby/api/types/swarm"
	"github.com/moby/moby/api/types/system"
	"github.com/moby/moby/client"
	"github.com/stretchr/testify/require"

	"github.com/getarcaneapp/arcane/backend/v2/internal/common"
	"github.com/getarcaneapp/arcane/backend/v2/internal/database"
	"github.com/getarcaneapp/arcane/backend/v2/internal/docker"
	"github.com/getarcaneapp/arcane/backend/v2/internal/environment"
	"github.com/getarcaneapp/arcane/backend/v2/pkg/pagination"
)

// swarmTestDaemonInternal is a fake Docker daemon for the docker-host tests: it
// answers the ping, info, and node endpoints the swarm node service uses.
type swarmTestDaemonInternal struct {
	mu               sync.Mutex
	state            swarm.LocalNodeState
	controlAvailable bool
	nodeID           string
	nodes            []swarm.Node
	paths            []string
}

func (d *swarmTestDaemonInternal) recordPathInternal(path string) {
	d.mu.Lock()
	d.paths = append(d.paths, path)
	d.mu.Unlock()
}

func (d *swarmTestDaemonInternal) sawPathSuffixInternal(suffix string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, path := range d.paths {
		if strings.HasSuffix(path, suffix) {
			return true
		}
	}
	return false
}

func (d *swarmTestDaemonInternal) newServerInternal(t *testing.T) *httptest.Server {
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
				NodeID:           d.nodeID,
			}}
			d.mu.Unlock()
			_ = json.NewEncoder(w).Encode(info)
		case strings.HasSuffix(r.URL.Path, "/nodes"):
			d.mu.Lock()
			nodes := append([]swarm.Node(nil), d.nodes...)
			d.mu.Unlock()
			_ = json.NewEncoder(w).Encode(nodes)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	return server
}

// swarmTestProxyRecorderInternal stands in for an agent environment's HTTP API.
// Counting its hits proves whether a request was proxied instead of served
// in-process.
type swarmTestProxyRecorderInternal struct {
	mu    sync.Mutex
	paths []string
}

func (p *swarmTestProxyRecorderInternal) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p.mu.Lock()
	p.paths = append(p.paths, r.Method+" "+r.URL.Path)
	p.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"success":true,"data":[]}`))
}

func (p *swarmTestProxyRecorderInternal) countInternal() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.paths)
}

// newSwarmTestEnsureManagerInternal mirrors the swarm service's ensureManager
// against the environment-scoped Docker client.
func newSwarmTestEnsureManagerInternal(dockerService *docker.DockerClientService) func(context.Context) error {
	return func(ctx context.Context) error {
		dockerClient, err := dockerService.GetClient(ctx)
		if err != nil {
			return err
		}
		infoResult, err := dockerClient.Info(ctx, client.InfoOptions{})
		if err != nil {
			return err
		}
		if infoResult.Info.Swarm.LocalNodeState != swarm.LocalNodeStateActive {
			return common.Classify(common.ErrSwarmNotEnabled, errors.New("Swarm mode is not enabled")) //nolint:staticcheck // Preserve the existing error message.
		}
		if !infoResult.Info.Swarm.ControlAvailable {
			return common.Classify(common.ErrSwarmManagerRequired, errors.New("Swarm manager access required")) //nolint:staticcheck // Preserve the existing error message.
		}
		return nil
	}
}

func createSwarmDockerHostTestEnvironmentInternal(t *testing.T, db *database.DB, id, apiURL, dockerHost string) {
	t.Helper()
	now := time.Now()
	require.NoError(t, db.Create(&environment.Environment{
		ID: id, CreatedAt: now, UpdatedAt: &now,
		Name:       "env-" + id,
		ApiUrl:     apiURL,
		Status:     string(environment.EnvironmentStatusOnline),
		Enabled:    true,
		DockerHost: &dockerHost,
	}).Error)
}

func TestService_ListNodesPaginated_DockerHostSwarmInactiveServedInProcess(t *testing.T) {
	ctx := t.Context()
	db := setupSwarmServiceTestDB(t)
	settingsSvc, err := newSettingsServiceForSwarmTest(t, ctx, db)
	require.NoError(t, err)
	envSvc := environment.NewEnvironmentService(db, nil, nil, nil, settingsSvc, nil)

	daemon := &swarmTestDaemonInternal{state: swarm.LocalNodeStateInactive}
	daemonServer := daemon.newServerInternal(t)

	proxy := &swarmTestProxyRecorderInternal{}
	proxyServer := httptest.NewServer(proxy)
	t.Cleanup(proxyServer.Close)

	createSwarmDockerHostTestEnvironmentInternal(t, db, "dh-1", proxyServer.URL, daemonServer.URL)

	dockerSvc := &docker.DockerClientService{}
	svc := NewService(dockerSvc, newSwarmTestEnsureManagerInternal(dockerSvc), nil, nil, nil, envSvc)

	items, _, err := svc.ListNodesPaginated(ctx, "dh-1", pagination.QueryParams{Limit: -1})
	require.Error(t, err)
	require.True(t, errors.Is(err, common.ErrSwarmNotEnabled), "expected a classified ErrSwarmNotEnabled, got %v", err)
	require.Nil(t, items)
	require.Equal(t, 0, proxy.countInternal(), "docker-host requests must not be proxied to an agent")
	require.True(t, daemon.sawPathSuffixInternal("/info"), "expected the environment's own daemon to be inspected")
}

func TestService_GetNode_DockerHostSwarmInactiveServedInProcess(t *testing.T) {
	ctx := t.Context()
	db := setupSwarmServiceTestDB(t)
	settingsSvc, err := newSettingsServiceForSwarmTest(t, ctx, db)
	require.NoError(t, err)
	envSvc := environment.NewEnvironmentService(db, nil, nil, nil, settingsSvc, nil)

	daemon := &swarmTestDaemonInternal{state: swarm.LocalNodeStateInactive}
	daemonServer := daemon.newServerInternal(t)

	proxy := &swarmTestProxyRecorderInternal{}
	proxyServer := httptest.NewServer(proxy)
	t.Cleanup(proxyServer.Close)

	createSwarmDockerHostTestEnvironmentInternal(t, db, "dh-1", proxyServer.URL, daemonServer.URL)

	dockerSvc := &docker.DockerClientService{}
	svc := NewService(dockerSvc, newSwarmTestEnsureManagerInternal(dockerSvc), nil, nil, nil, envSvc)

	node, err := svc.GetNode(ctx, "dh-1", "node-1")
	require.Error(t, err)
	require.True(t, errors.Is(err, common.ErrSwarmNotEnabled), "expected a classified ErrSwarmNotEnabled, got %v", err)
	require.Nil(t, node)
	require.Equal(t, 0, proxy.countInternal(), "docker-host requests must not be proxied to an agent")
	require.True(t, daemon.sawPathSuffixInternal("/info"), "expected the environment's own daemon to be inspected")
}

func TestService_GetJoinCandidates_DockerHostSwarmInactiveServedInProcess(t *testing.T) {
	ctx := t.Context()
	db := setupSwarmServiceTestDB(t)
	settingsSvc, err := newSettingsServiceForSwarmTest(t, ctx, db)
	require.NoError(t, err)
	envSvc := environment.NewEnvironmentService(db, nil, nil, nil, settingsSvc, nil)

	daemon := &swarmTestDaemonInternal{state: swarm.LocalNodeStateInactive}
	daemonServer := daemon.newServerInternal(t)

	proxy := &swarmTestProxyRecorderInternal{}
	proxyServer := httptest.NewServer(proxy)
	t.Cleanup(proxyServer.Close)

	createSwarmDockerHostTestEnvironmentInternal(t, db, "dh-1", proxyServer.URL, daemonServer.URL)

	dockerSvc := &docker.DockerClientService{}
	svc := NewService(dockerSvc, newSwarmTestEnsureManagerInternal(dockerSvc), nil, nil, nil, envSvc)

	candidates, err := svc.GetJoinCandidates(ctx, "dh-1")
	require.NoError(t, err, "an inactive daemon yields no candidates instead of a generic failure")
	require.Empty(t, candidates)
	require.Equal(t, 0, proxy.countInternal(), "docker-host requests must not be proxied to an agent")
	require.True(t, daemon.sawPathSuffixInternal("/info"), "expected the environment's own daemon to be inspected")
}

func TestService_ListNodesPaginated_DockerHostSwarmActiveServedInProcess(t *testing.T) {
	ctx := t.Context()
	db := setupSwarmServiceTestDB(t)
	settingsSvc, err := newSettingsServiceForSwarmTest(t, ctx, db)
	require.NoError(t, err)
	envSvc := environment.NewEnvironmentService(db, nil, nil, nil, settingsSvc, nil)

	daemon := &swarmTestDaemonInternal{
		state:            swarm.LocalNodeStateActive,
		controlAvailable: true,
		nodeID:           "node-1",
		nodes:            []swarm.Node{{ID: "node-1", Description: swarm.NodeDescription{Hostname: "dh-1-host"}}},
	}
	daemonServer := daemon.newServerInternal(t)

	proxy := &swarmTestProxyRecorderInternal{}
	proxyServer := httptest.NewServer(proxy)
	t.Cleanup(proxyServer.Close)

	createSwarmDockerHostTestEnvironmentInternal(t, db, "dh-1", proxyServer.URL, daemonServer.URL)

	dockerSvc := &docker.DockerClientService{}
	svc := NewService(dockerSvc, newSwarmTestEnsureManagerInternal(dockerSvc), nil, nil, nil, envSvc)

	items, _, err := svc.ListNodesPaginated(ctx, "dh-1", pagination.QueryParams{Limit: -1})
	require.NoError(t, err)
	require.Len(t, items, 1)
	require.Equal(t, "node-1", items[0].ID)
	require.Equal(t, 0, proxy.countInternal(), "docker-host requests must not be proxied to an agent")

	// The docker-host environment's own swarm node is attributed to that
	// environment, not to the manager's hardcoded local ID.
	require.NotNil(t, items[0].Agent.EnvironmentID)
	require.Equal(t, "dh-1", *items[0].Agent.EnvironmentID)
	require.NotNil(t, items[0].Agent.EnvironmentType)
	require.Equal(t, "direct", *items[0].Agent.EnvironmentType)
}

func TestService_ListNodesPaginated_AgentEnvironmentStillProxies(t *testing.T) {
	ctx := t.Context()
	db := setupSwarmServiceTestDB(t)
	settingsSvc, err := newSettingsServiceForSwarmTest(t, ctx, db)
	require.NoError(t, err)
	envSvc := environment.NewEnvironmentService(db, nil, nil, nil, settingsSvc, nil)

	proxy := &swarmTestProxyRecorderInternal{}
	proxyServer := httptest.NewServer(proxy)
	t.Cleanup(proxyServer.Close)

	createSwarmTestEnvironment(t, db, "agent-1", proxyServer.URL, string(environment.EnvironmentStatusOnline), false, nil)

	dockerSvc := &docker.DockerClientService{}
	svc := NewService(dockerSvc, newSwarmTestEnsureManagerInternal(dockerSvc), nil, nil, nil, envSvc)

	items, _, err := svc.ListNodesPaginated(ctx, "agent-1", pagination.QueryParams{Limit: -1})
	require.NoError(t, err)
	require.Empty(t, items)
	require.Equal(t, 1, proxy.countInternal(), "agent environments must keep proxying the node listing exactly once")
}

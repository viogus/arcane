package environment

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/moby/moby/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/getarcaneapp/arcane/backend/v2/internal/common"
	"github.com/getarcaneapp/arcane/backend/v2/internal/config"
	"github.com/getarcaneapp/arcane/backend/v2/internal/docker"
)

// TestEnvironmentService_EnvironmentEventBusServesOnlyDockerHostEnvironments pins the lookup the image
// watcher relies on: a docker-host environment yields the bus of its own daemon (so its events are
// attributed to it), while the manager and agent environments yield nothing because their daemon is
// streamed elsewhere or by someone else.
func TestEnvironmentService_EnvironmentEventBusServesOnlyDockerHostEnvironments(t *testing.T) {
	ctx := t.Context()
	db := setupEnvironmentServiceTestDB(t)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/_ping":
			w.Header().Set("Api-Version", client.MinAPIVersion)
			w.WriteHeader(http.StatusOK)
		case "/events":
			w.WriteHeader(http.StatusOK)
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
			<-r.Context().Done()
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)

	dockerService := docker.NewDockerClientService(ctx, nil, &config.Config{DockerHost: "unix:///var/run/docker.sock"}, nil)
	t.Cleanup(func() { dockerService.Close(t.Context()) })
	service := NewEnvironmentService(db, nil, dockerService, nil, nil, nil)

	dockerHost := server.URL
	now := time.Now()
	require.NoError(t, db.WithContext(ctx).Create(&Environment{
		ID:         "env-event-bus-remote",
		CreatedAt:  now,
		UpdatedAt:  &now,
		Name:       "event-bus-remote",
		Status:     string(EnvironmentStatusOnline),
		Enabled:    true,
		DockerHost: &dockerHost,
	}).Error)
	createNamedTestEnvironment(t, db, "env-event-bus-agent", "event-bus-agent", "http://agent.example", new("arc_token"))

	eventBus, ok := service.EnvironmentEventBus(ctx, "env-event-bus-remote")
	require.True(t, ok)
	require.NotNil(t, eventBus)
	assert.NotSame(t, dockerService.EventBus(), eventBus, "a docker-host daemon must publish on its own bus")

	_, ok = service.EnvironmentEventBus(ctx, "env-event-bus-agent")
	assert.False(t, ok, "an agent environment is streamed by its agent, not here")

	_, ok = service.EnvironmentEventBus(ctx, LocalEnvironmentID)
	assert.False(t, ok, "the manager's own daemon keeps the shared bus")

	// A docker-host environment bound to the manager's own daemon is the manager's daemon.
	require.NoError(t, db.WithContext(ctx).Model(&Environment{}).Where("id = ?", "env-event-bus-remote").
		Update("docker_host", "unix:///var/run/docker.sock").Error)
	service.invalidateEnvironmentCache("env-event-bus-remote")
	sameBus, ok := service.EnvironmentEventBus(ctx, "env-event-bus-remote")
	require.True(t, ok)
	assert.Same(t, dockerService.EventBus(), sameBus)
}

func TestNormalizeDockerHost(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    string
		wantErr bool
	}{
		{name: "empty clears the binding", in: "   ", want: ""},
		{name: "tcp endpoint", in: "tcp://192.168.2.33:6001", want: "tcp://192.168.2.33:6001"},
		{name: "http endpoint", in: " http://docker.internal:2375 ", want: "http://docker.internal:2375"},
		{name: "https endpoint", in: "https://docker.example.com:2376", want: "https://docker.example.com:2376"},
		{name: "unix socket", in: "unix:///var/run/docker.sock", want: "unix:///var/run/docker.sock"},
		{name: "npipe socket", in: "npipe:////./pipe/docker_engine", want: "npipe:////./pipe/docker_engine"},
		{name: "ssh endpoint", in: "ssh://root@docker.example.com", wantErr: true},
		{name: "unsupported scheme", in: "ftp://192.168.2.33:6001", wantErr: true},
		{name: "missing scheme", in: "192.168.2.33:6001", wantErr: true},
		{name: "unix without path", in: "unix://", wantErr: true},
		{name: "tcp without host", in: "tcp://", wantErr: true},
		{name: "tcp without port", in: "tcp://192.168.2.33", wantErr: true},
		{name: "unix with a host instead of an absolute path", in: "unix://var/run/docker.sock", wantErr: true},
		{name: "embedded credentials", in: "tcp://user:pass@192.168.2.33:2375", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NormalizeDockerHost(tt.in)
			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, common.ErrBadRequest)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestValidateEnvironmentTargetInternal(t *testing.T) {
	dockerHost := "tcp://192.168.2.33:6001"
	apiURL := "http://arcane-agent:3553"
	token := "arc_token"

	t.Run("docker host only", func(t *testing.T) {
		env := &Environment{Name: "direct", DockerHost: new(dockerHost), IsEdge: true}
		require.NoError(t, validateEnvironmentTargetInternal(env))
		assert.False(t, env.IsEdge, "docker-host environments are never edge agents")
		assert.Equal(t, dockerHost, env.DockerHostURL())
	})

	t.Run("agent url only", func(t *testing.T) {
		env := &Environment{Name: "agent", ApiUrl: apiURL, AccessToken: &token}
		require.NoError(t, validateEnvironmentTargetInternal(env))
	})

	t.Run("neither target", func(t *testing.T) {
		err := validateEnvironmentTargetInternal(&Environment{Name: "empty"})
		require.Error(t, err)
		assert.ErrorIs(t, err, common.ErrBadRequest)
	})

	t.Run("both targets", func(t *testing.T) {
		err := validateEnvironmentTargetInternal(&Environment{Name: "both", ApiUrl: apiURL, DockerHost: &dockerHost})
		require.Error(t, err)
		assert.ErrorIs(t, err, common.ErrBadRequest)
	})

	t.Run("docker host with access token", func(t *testing.T) {
		err := validateEnvironmentTargetInternal(&Environment{Name: "mixed", DockerHost: &dockerHost, AccessToken: &token})
		require.Error(t, err)
		assert.ErrorIs(t, err, common.ErrBadRequest)
	})

	t.Run("docker host normalizes input", func(t *testing.T) {
		raw := "  tcp://192.168.2.33:6001  "
		env := &Environment{Name: "padded", DockerHost: &raw}
		require.NoError(t, validateEnvironmentTargetInternal(env))
		assert.Equal(t, dockerHost, env.DockerHostURL())
	})
}

func TestApplyDockerHostUpdateInternal(t *testing.T) {
	agentEnv := &Environment{Name: "agent", ApiUrl: "http://arcane-agent:3553"}
	dockerEnv := &Environment{Name: "direct", DockerHost: new("tcp://192.168.2.33:6001")}
	token := "arc_token"
	agentWithToken := &Environment{Name: "agent", ApiUrl: "http://arcane-agent:3553", AccessToken: &token}

	t.Run("switching to a docker host drops the agent binding", func(t *testing.T) {
		updates := map[string]any{"docker_host": "tcp://192.168.2.33:6002", "is_edge": true}
		require.NoError(t, applyDockerHostUpdateInternal(updates, agentEnv))
		assert.Equal(t, "tcp://192.168.2.33:6002", updates["docker_host"])
		assert.Empty(t, updates["api_url"])
		assert.Empty(t, updates["access_token"])
		assert.Equal(t, false, updates["is_edge"], "is_edge must be cleared, not dropped")
	})

	t.Run("clearing the docker host returns the environment to the agent flow", func(t *testing.T) {
		updates := map[string]any{"docker_host": "", "api_url": "http://arcane-agent:3553"}
		require.NoError(t, applyDockerHostUpdateInternal(updates, dockerEnv))
		assert.Nil(t, updates["docker_host"])
	})

	t.Run("clearing the docker host without an api url is rejected", func(t *testing.T) {
		err := applyDockerHostUpdateInternal(map[string]any{"docker_host": ""}, dockerEnv)
		require.Error(t, err)
		assert.ErrorIs(t, err, common.ErrBadRequest)
	})

	t.Run("docker host plus api url is rejected", func(t *testing.T) {
		err := applyDockerHostUpdateInternal(map[string]any{"docker_host": "tcp://192.168.2.33:6001", "api_url": "http://x:1"}, agentEnv)
		require.Error(t, err)
		assert.ErrorIs(t, err, common.ErrBadRequest)
	})

	t.Run("docker-host environment cannot gain an api url", func(t *testing.T) {
		err := applyDockerHostUpdateInternal(map[string]any{"api_url": "http://x:1"}, dockerEnv)
		require.Error(t, err)
		assert.ErrorIs(t, err, common.ErrBadRequest)
	})

	t.Run("docker-host environment cannot gain an access token", func(t *testing.T) {
		err := applyDockerHostUpdateInternal(map[string]any{"access_token": "arc_token"}, dockerEnv)
		require.Error(t, err)
		assert.ErrorIs(t, err, common.ErrBadRequest)
	})

	t.Run("clearing the docker host and the api url together is rejected", func(t *testing.T) {
		err := applyDockerHostUpdateInternal(map[string]any{"docker_host": "", "api_url": ""}, dockerEnv)
		require.Error(t, err)
		assert.ErrorIs(t, err, common.ErrBadRequest)
	})

	t.Run("docker-host environment is never marked as an edge environment", func(t *testing.T) {
		updates := map[string]any{"is_edge": true}
		require.NoError(t, applyDockerHostUpdateInternal(updates, dockerEnv))
		assert.Equal(t, false, updates["is_edge"], "is_edge must be cleared, not dropped")
	})

	t.Run("unrelated update on an agent environment with a token keeps the binding", func(t *testing.T) {
		updates := map[string]any{"name": "renamed"}
		require.NoError(t, applyDockerHostUpdateInternal(updates, agentWithToken))
		assert.NotContains(t, updates, "api_url")
		assert.NotContains(t, updates, "access_token")
		assert.NotContains(t, updates, "is_edge")
	})

	t.Run("unrelated update on an agent environment is untouched", func(t *testing.T) {
		updates := map[string]any{"name": "renamed"}
		require.NoError(t, applyDockerHostUpdateInternal(updates, agentEnv))
		assert.NotContains(t, updates, "api_url")
	})
}

// Docker-host environments are served in-process by the manager, so agent-only pushes (the
// global variable sync) must not target them - there is no agent on the other side to accept
// the request. Regression test for the "has no agent to proxy to" startup warnings.
func TestEnvironmentService_ListEnabledAgentEnvironmentIDs_SkipsDockerHostEnvironments(t *testing.T) {
	ctx := t.Context()
	db := setupEnvironmentServiceTestDB(t)
	svc := NewEnvironmentService(db, nil, nil, nil, nil, nil)

	now := time.Now()
	envs := []Environment{
		{
			ID: "env-agent", CreatedAt: now, UpdatedAt: &now,
			Name: "Agent", ApiUrl: "http://arcane-agent:3553",
			Status: string(EnvironmentStatusOnline), Enabled: true,
		},
		{
			ID: "env-dockerhost", CreatedAt: now, UpdatedAt: &now,
			Name: "Remote Docker", DockerHost: new("tcp://192.168.2.33:6001"),
			Status: string(EnvironmentStatusOnline), Enabled: true,
		},
		{
			ID: "env-disabled-agent", CreatedAt: now, UpdatedAt: &now,
			Name: "Disabled Agent", ApiUrl: "http://arcane-agent:3554",
			Status: string(EnvironmentStatusOffline), Enabled: false,
		},
		{
			ID: "env-disabled-dockerhost", CreatedAt: now, UpdatedAt: &now,
			Name: "Disabled Docker Host", DockerHost: new("tcp://192.168.2.33:6002"),
			Status: string(EnvironmentStatusOffline), Enabled: false,
		},
		{
			// An explicit empty docker_host (what a client sending dockerHost:"" leaves behind)
			// must still count as an agent environment, and a disabled one must stay excluded:
			// this pins down the AND/OR parenthesisation of the query predicate.
			ID: "env-agent-empty-host", CreatedAt: now, UpdatedAt: &now,
			Name: "Agent With Empty Host Column", ApiUrl: "http://arcane-agent:3555",
			DockerHost: new(""),
			Status:     string(EnvironmentStatusOnline), Enabled: true,
		},
		{
			ID: "env-disabled-agent-empty-host", CreatedAt: now, UpdatedAt: &now,
			Name: "Disabled Agent With Empty Host Column", ApiUrl: "http://arcane-agent:3556",
			DockerHost: new(""),
			Status:     string(EnvironmentStatusOffline), Enabled: false,
		},
	}
	require.NoError(t, db.WithContext(ctx).Create(&envs).Error)

	agentIDs, err := svc.ListEnabledAgentEnvironmentIDs(ctx)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"env-agent", "env-agent-empty-host"}, agentIDs)

	// Health checks still care about every enabled environment, docker-host included.
	allIDs, err := svc.ListEnabledEnvironmentIDs(ctx)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"env-agent", "env-dockerhost", "env-agent-empty-host"}, allIDs)
}

// TestEnvironmentHandler_DockerHostHasNoAgentDeploymentAssets pins the user-facing half of the
// docker-host cleanup: asking for agent snippets or mTLS material for a daemon the manager talks to
// itself used to answer "Environment does not have an API key configured", which reads like a broken
// agent. The answer must name the environment type instead.
func TestEnvironmentHandler_DockerHostHasNoAgentDeploymentAssets(t *testing.T) {
	ctx := t.Context()
	db := setupEnvironmentServiceTestDB(t)
	service := NewEnvironmentService(db, nil, nil, nil, nil, nil)
	handler := NewHandler(service, nil, nil, nil, &config.Config{}, nil)

	dockerHost := "tcp://192.168.2.33:6002"
	now := time.Now()
	require.NoError(t, db.WithContext(ctx).Create(&Environment{
		ID: "env-dockerhost-snippets", CreatedAt: now, UpdatedAt: &now,
		Name: "Docker Host Snippets", Status: string(EnvironmentStatusOnline),
		Enabled: true, DockerHost: &dockerHost,
	}).Error)
	createNamedTestEnvironment(t, db, "env-agent-snippets", "agent-snippets", "http://arcane-agent:3555", nil)

	_, err := handler.GetDeploymentSnippets(ctx, &GetDeploymentSnippetsInput{ID: "env-dockerhost-snippets"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "no agent deployment snippets")

	_, _, err = handler.loadEnvironmentMTLSFiles(ctx, "env-dockerhost-snippets")
	require.Error(t, err)
	require.Contains(t, err.Error(), "no agent mTLS assets")

	// An agent environment without credentials keeps its own, accurate message.
	_, err = handler.GetDeploymentSnippets(ctx, &GetDeploymentSnippetsInput{ID: "env-agent-snippets"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "API key")
}

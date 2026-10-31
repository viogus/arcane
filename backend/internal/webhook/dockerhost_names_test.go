package webhook

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/getarcaneapp/arcane/backend/v2/internal/config"
	"github.com/getarcaneapp/arcane/backend/v2/internal/container"
	"github.com/getarcaneapp/arcane/backend/v2/internal/database"
	"github.com/getarcaneapp/arcane/backend/v2/internal/docker"
	"github.com/getarcaneapp/arcane/backend/v2/internal/environment"
	"github.com/getarcaneapp/arcane/backend/v2/internal/event"
	"github.com/getarcaneapp/arcane/backend/v2/internal/podman"
	"github.com/getarcaneapp/arcane/backend/v2/internal/settings"
	"github.com/getarcaneapp/arcane/backend/v2/pkg/libarcane/envcontext"
)

// webhookDockerStub is a minimal Docker daemon that answers the calls the webhook target-name
// resolution path makes: API negotiation, container inspect, and container lifecycle actions.
type webhookDockerStub struct {
	server        *httptest.Server
	inspectName   string
	inspectID     string
	inspectStatus int

	requests atomic.Int64
	stopHits atomic.Int64
}

type webhookDockerStubOptions struct {
	containerName string
	containerID   string
	inspectStatus int
}

func newWebhookDockerStub(t *testing.T, opts webhookDockerStubOptions) *webhookDockerStub {
	t.Helper()

	stub := &webhookDockerStub{
		inspectName:   opts.containerName,
		inspectID:     opts.containerID,
		inspectStatus: opts.inspectStatus,
	}
	stub.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		stub.requests.Add(1)

		path := r.URL.Path
		switch {
		case strings.HasSuffix(path, "/_ping"):
			w.Header().Set("API-Version", "1.41")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("OK"))
		case strings.HasSuffix(path, "/stop"):
			stub.stopHits.Add(1)
			w.WriteHeader(http.StatusNoContent)
		case strings.HasSuffix(path, "/json") && strings.Contains(path, "/containers/"):
			if stub.inspectStatus != 0 && stub.inspectStatus != http.StatusOK {
				w.WriteHeader(stub.inspectStatus)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"Id":   stub.inspectID,
				"Name": stub.inspectName,
			})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(stub.server.Close)

	return stub
}

func createDockerHostEnvironmentRow(t *testing.T, db *database.DB, id, dockerHost string) {
	t.Helper()

	require.NoError(t, db.Create(&environment.Environment{
		ID:         id,
		Name:       id,
		Enabled:    true,
		DockerHost: &dockerHost,
	}).Error)
}

func createWebhookRow(t *testing.T, db *database.DB, wh Webhook) {
	t.Helper()

	if wh.TokenHash == "" {
		wh.TokenHash = "hash-" + wh.ID
	}
	if wh.TokenPrefix == "" {
		wh.TokenPrefix = "arc_wh_" + wh.ID
	}
	require.NoError(t, db.Create(&wh).Error)
}

func summariesByID(t *testing.T, service *WebhookService, environmentID string) map[string]string {
	t.Helper()

	summaries, err := service.ListWebhookSummaries(t.Context(), environmentID)
	require.NoError(t, err)

	names := make(map[string]string, len(summaries))
	for _, summary := range summaries {
		names[summary.ID] = summary.TargetName
	}

	return names
}

// TestListWebhookSummariesResolvesDockerHostContainerNameInProcess guards the docker-host
// contract: a container webhook for a docker-host environment must resolve its container name
// against that environment's own daemon, without proxying and without touching the manager's daemon.
func TestListWebhookSummariesResolvesDockerHostContainerNameInProcess(t *testing.T) {
	db := setupWebhookServiceTestDB(t)
	require.NoError(t, db.AutoMigrate(&environment.Environment{}))

	target := newWebhookDockerStub(t, webhookDockerStubOptions{containerName: "/resolved-name", containerID: "cid-1"})
	manager := newWebhookDockerStub(t, webhookDockerStubOptions{containerName: "/manager-name", containerID: "cid-1"})

	createDockerHostEnvironmentRow(t, db, "env-docker-host", target.server.URL)
	createWebhookRow(t, db, Webhook{
		ID:            "wh-container",
		Name:          "container-hook",
		TargetType:    WebhookTargetTypeContainer,
		ActionType:    WebhookActionTypeStop,
		TargetID:      "cid-1",
		TargetRef:     "stale-ref",
		EnvironmentID: "env-docker-host",
		Enabled:       true,
	})

	dockerService := docker.NewDockerClientService(t.Context(), nil, &config.Config{DockerHost: manager.server.URL}, nil)
	service := &WebhookService{
		db:                 db,
		containerService:   container.NewContainerService(nil, dockerService, nil, nil, nil, nil, podman.NewService(podman.Config{Engine: dockerService.Engine})),
		environmentService: environment.NewEnvironmentService(db, nil, dockerService, nil, nil, nil),
	}

	names := summariesByID(t, service, "env-docker-host")

	// "stale-ref" is what the proxied fallback would have returned, so resolving the real container
	// name proves the lookup ran in-process against the environment's daemon.
	require.Contains(t, names, "wh-container")
	assert.Equal(t, "resolved-name", names["wh-container"])
	require.Positive(t, target.requests.Load(), "the environment's daemon must serve the inspect")
	assert.Zero(t, manager.requests.Load(), "the manager's own daemon must not be touched")
}

// TestListWebhookSummariesFallsBackWhenDockerHostContainerMissing ensures a deleted container never
// turns the webhook list into an error: the trimmed target ref (or the target ID) is used instead.
func TestListWebhookSummariesFallsBackWhenDockerHostContainerMissing(t *testing.T) {
	db := setupWebhookServiceTestDB(t)
	require.NoError(t, db.AutoMigrate(&environment.Environment{}))

	target := newWebhookDockerStub(t, webhookDockerStubOptions{inspectStatus: http.StatusNotFound})
	manager := newWebhookDockerStub(t, webhookDockerStubOptions{containerName: "/manager-name", containerID: "cid-1"})

	createDockerHostEnvironmentRow(t, db, "env-docker-host", target.server.URL)
	createWebhookRow(t, db, Webhook{
		ID:            "wh-ref",
		Name:          "ref-hook",
		TargetType:    WebhookTargetTypeContainer,
		ActionType:    WebhookActionTypeStop,
		TargetID:      "cid-missing",
		TargetRef:     "  missing-ref  ",
		EnvironmentID: "env-docker-host",
		Enabled:       true,
	})
	createWebhookRow(t, db, Webhook{
		ID:            "wh-id",
		Name:          "id-hook",
		TargetType:    WebhookTargetTypeContainer,
		ActionType:    WebhookActionTypeStop,
		TargetID:      "cid-only",
		TargetRef:     "",
		EnvironmentID: "env-docker-host",
		Enabled:       true,
	})

	dockerService := docker.NewDockerClientService(t.Context(), nil, &config.Config{DockerHost: manager.server.URL}, nil)
	service := &WebhookService{
		db:                 db,
		containerService:   container.NewContainerService(nil, dockerService, nil, nil, nil, nil, podman.NewService(podman.Config{Engine: dockerService.Engine})),
		environmentService: environment.NewEnvironmentService(db, nil, dockerService, nil, nil, nil),
	}

	names := summariesByID(t, service, "env-docker-host")

	assert.Equal(t, "missing-ref", names["wh-ref"], "a missing container falls back to the trimmed target ref")
	assert.Equal(t, "cid-only", names["wh-id"], "without a ref the target id is used")
	assert.Positive(t, target.requests.Load())
	assert.Zero(t, manager.requests.Load())
}

// TestRemoteWebhookTargetNamesSkipsDockerHostWithoutProxy pins requirement 2: a docker-host target
// must not attempt the environment proxy. The service has a nil environmentService, so any proxy
// attempt would panic.
func TestRemoteWebhookTargetNamesSkipsDockerHostWithoutProxy(t *testing.T) {
	service := &WebhookService{}

	ctx := envcontext.WithTarget(t.Context(), envcontext.Target{
		EnvironmentID: "env-docker-host",
		DockerHost:    "tcp://192.168.2.33:6001",
	})

	names := service.remoteWebhookTargetNamesInternal(ctx, "env-docker-host", []Webhook{
		{TargetType: WebhookTargetTypeContainer},
		{TargetType: WebhookTargetTypeProject},
		{TargetType: WebhookTargetTypeGitOps},
	})

	assert.Empty(t, names)
}

// TestRemoteWebhookTargetNamesStillProxiesAgentEnvironments is the agent regression guard: an agent
// environment keeps resolving remote target names through the environment proxy.
func TestRemoteWebhookTargetNamesStillProxiesAgentEnvironments(t *testing.T) {
	db := setupWebhookServiceTestDB(t)
	require.NoError(t, db.AutoMigrate(&environment.Environment{}))

	var agentHits atomic.Int64
	agent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		agentHits.Add(1)
		if !strings.HasSuffix(r.URL.Path, "/projects/references") {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"success":true,"data":[{"id":"project-1","name":"Proxied Project"}]}`))
	}))
	t.Cleanup(agent.Close)

	require.NoError(t, db.Create(&environment.Environment{
		ID:      "env-agent",
		Name:    "agent",
		Enabled: true,
		ApiUrl:  agent.URL,
	}).Error)
	createWebhookRow(t, db, Webhook{
		ID:            "wh-agent",
		Name:          "agent-hook",
		TargetType:    WebhookTargetTypeProject,
		ActionType:    WebhookActionTypeUp,
		TargetID:      "project-1",
		EnvironmentID: "env-agent",
		Enabled:       true,
	})

	require.NoError(t, db.AutoMigrate(&settings.SettingVariable{}, &settings.EnvironmentSetting{}))
	settingsService, err := settings.NewSettingsService(t.Context(), db, &config.Config{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = settingsService.Stop(context.WithoutCancel(t.Context())) })

	service := &WebhookService{
		db:                 db,
		environmentService: environment.NewEnvironmentService(db, nil, nil, event.NewEventService(db, &config.Config{}, nil), settingsService, nil),
	}

	names := service.remoteWebhookTargetNamesInternal(t.Context(), "env-agent", []Webhook{{TargetType: WebhookTargetTypeProject}})
	assert.Equal(t, "Proxied Project", names[WebhookTargetTypeProject]["project-1"])
	require.Positive(t, agentHits.Load(), "agent environments must still be proxied")

	summaries, err := service.ListWebhookSummaries(t.Context(), "env-agent")
	require.NoError(t, err)
	require.Len(t, summaries, 1)
	assert.Equal(t, "Proxied Project", summaries[0].TargetName)
}

// TestExecuteWebhookActionRunsInProcessForDockerHostContainer is the trigger regression guard: a
// docker-host container webhook action must execute in-process against that environment's daemon
// (and never be sent through the environment proxy).
func TestExecuteWebhookActionRunsInProcessForDockerHostContainer(t *testing.T) {
	db := setupWebhookServiceTestDB(t)
	require.NoError(t, db.AutoMigrate(&environment.Environment{}, &event.Event{}))

	target := newWebhookDockerStub(t, webhookDockerStubOptions{containerName: "/my-container", containerID: "cid-1"})
	manager := newWebhookDockerStub(t, webhookDockerStubOptions{containerName: "/manager-container", containerID: "cid-1"})

	createDockerHostEnvironmentRow(t, db, "env-docker-host", target.server.URL)
	createWebhookRow(t, db, Webhook{
		ID:            "wh-trigger",
		Name:          "trigger-hook",
		TargetType:    WebhookTargetTypeContainer,
		ActionType:    WebhookActionTypeStop,
		TargetID:      "cid-1",
		TargetRef:     "my-container",
		EnvironmentID: "env-docker-host",
		Enabled:       true,
	})

	dockerService := docker.NewDockerClientService(t.Context(), nil, &config.Config{DockerHost: manager.server.URL}, nil)
	eventService := event.NewEventService(db, &config.Config{}, nil)
	service := &WebhookService{
		db:                 db,
		containerService:   container.NewContainerService(eventService, dockerService, nil, nil, nil, nil, podman.NewService(podman.Config{Engine: dockerService.Engine})),
		eventService:       eventService,
		environmentService: environment.NewEnvironmentService(db, nil, dockerService, eventService, nil, nil),
	}

	wh := &Webhook{
		ID:            "wh-trigger",
		Name:          "trigger-hook",
		TargetType:    WebhookTargetTypeContainer,
		ActionType:    WebhookActionTypeStop,
		TargetID:      "cid-1",
		TargetRef:     "my-container",
		EnvironmentID: "env-docker-host",
		Enabled:       true,
	}

	result, err := service.executeWebhookActionInternal(t.Context(), wh, WebhookActionTypeStop)
	require.NoError(t, err)
	assert.Nil(t, result)
	require.Equal(t, int64(1), target.stopHits.Load(), "the stop must run against the environment's daemon")
	assert.Zero(t, manager.requests.Load(), "the manager's own daemon must not be touched")
}

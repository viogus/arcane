package webhook

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/getarcaneapp/arcane/backend/v2/internal/environment"
	"github.com/getarcaneapp/arcane/backend/v2/pkg/libarcane/envcontext"
)

// TestDockerHostActionContextScopesEnvironmentDaemon guards the docker-host webhook contract: an
// action for a docker-host environment must run in-process against that environment's daemon, while
// agent environments keep going through the environment proxy.
func TestDockerHostActionContextScopesEnvironmentDaemon(t *testing.T) {
	db := setupWebhookServiceTestDB(t)
	require.NoError(t, db.AutoMigrate(&environment.Environment{}))

	dockerHost := "tcp://192.168.2.33:6001"
	require.NoError(t, db.Create(&environment.Environment{
		ID:         "env-docker-host",
		Name:       "tyo-direct",
		Enabled:    true,
		DockerHost: &dockerHost,
	}).Error)
	require.NoError(t, db.Create(&environment.Environment{
		ID:      "env-agent",
		Name:    "agent",
		Enabled: true,
		ApiUrl:  "http://192.168.2.33:3554",
	}).Error)

	service := &WebhookService{db: db, environmentService: environment.NewEnvironmentService(db, nil, nil, nil, nil, nil)}

	t.Run("docker-host environment is scoped to its own daemon", func(t *testing.T) {
		ctx, ok := service.dockerHostActionContextInternal(t.Context(), "env-docker-host")
		require.True(t, ok)
		assert.Equal(t, dockerHost, envcontext.DockerHostFromContext(ctx))
		assert.Equal(t, "env-docker-host", envcontext.EnvironmentIDFromContext(ctx))
		assert.True(t, envcontext.IsDockerHostTarget(ctx))
	})

	t.Run("agent environment keeps using the proxy", func(t *testing.T) {
		_, ok := service.dockerHostActionContextInternal(t.Context(), "env-agent")
		assert.False(t, ok)
	})

	t.Run("unknown environment keeps using the proxy", func(t *testing.T) {
		_, ok := service.dockerHostActionContextInternal(t.Context(), "env-missing")
		assert.False(t, ok)
	})

	t.Run("an already-scoped request needs no environment lookup", func(t *testing.T) {
		scoped := envcontext.WithTarget(t.Context(), envcontext.Target{
			EnvironmentID: "env-docker-host",
			DockerHost:    dockerHost,
		})

		ctx, ok := (&WebhookService{}).dockerHostActionContextInternal(scoped, "env-docker-host")

		require.True(t, ok)
		assert.Equal(t, dockerHost, envcontext.DockerHostFromContext(ctx))
	})
}

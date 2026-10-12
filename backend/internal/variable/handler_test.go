package variable

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/getarcaneapp/arcane/backend/v2/pkg/libarcane/envcontext"
)

func TestVariableHandlerServesDockerHostEnvironmentsLocally(t *testing.T) {
	// A docker-host environment is served in-process but has no agent to proxy to, so its
	// materialized .env.global is the manager's own file.
	service, _, _ := setupVariableServiceTest(t)

	proxyCalls := 0
	proxy := func(_ context.Context, _, _, _ string, _ []byte, _ any) error {
		proxyCalls++
		return errors.New("a docker-host environment has no agent to proxy to")
	}
	handler := &VariableHandler{variableService: service, proxyRemoteJSON: proxy}

	ctx := envcontext.WithTarget(t.Context(), envcontext.Target{
		EnvironmentID: "env-docker-host",
		DockerHost:    "tcp://192.168.2.33:6001",
	})

	output, err := handler.GetMaterializedVariables(ctx, &GetGlobalVariablesInput{EnvironmentID: "env-docker-host"})
	require.NoError(t, err)
	require.NotNil(t, output)

	require.Zero(t, proxyCalls)
}

package envcontext

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEnvironmentIDOrDefault(t *testing.T) {
	t.Run("target environment wins over the local environment", func(t *testing.T) {
		ctx := WithTarget(t.Context(), Target{
			EnvironmentID: "env-docker-host",
			DockerHost:    "tcp://192.168.2.33:6001",
		})

		require.Equal(t, "env-docker-host", EnvironmentIDOrDefault(ctx))
	})

	t.Run("plain context falls back to the local environment", func(t *testing.T) {
		require.Equal(t, "0", EnvironmentIDOrDefault(t.Context()))
	})

	t.Run("target without an environment id falls back to the local environment", func(t *testing.T) {
		ctx := WithTarget(t.Context(), Target{DockerHost: "tcp://192.168.2.33:6001"})

		require.Equal(t, "0", EnvironmentIDOrDefault(ctx))
	})
}

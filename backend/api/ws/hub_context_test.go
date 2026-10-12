package ws

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type hubContextTestKey struct{}

// Shared hubs must keep request values, because the request context carries the
// target environment (and therefore the Docker daemon a docker-host environment
// points at) while the hub producer lives on after the request ends.
func TestNewHubContextPreservesRequestValues(t *testing.T) {
	base, cancelBase := context.WithCancel(context.WithValue(context.Background(), hubContextTestKey{}, "environment-target"))
	defer cancelBase()

	hubCtx, cancelHub := newHubContext(base)
	defer cancelHub()

	require.Equal(t, "environment-target", hubCtx.Value(hubContextTestKey{}))

	// The hub context outlives the request: cancelling the request must not tear
	// down the shared producer.
	cancelBase()
	select {
	case <-hubCtx.Done():
		t.Fatal("hub context cancelled with its request context")
	case <-time.After(50 * time.Millisecond):
	}

	// Only the hub's own cancel (idle teardown) ends it.
	cancelHub()
	select {
	case <-hubCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("hub context not cancelled by its own cancel function")
	}
}

// Container IDs are unique per daemon, not globally, so a hub key without the
// environment id would let two environments share one producer and leak samples.
func TestContainerStatsHubKeyScopesHubToEnvironment(t *testing.T) {
	const containerID = "0123456789abcdef"

	assert.NotEqual(t,
		containerStatsHubKey("tyo-direct", containerID),
		containerStatsHubKey("arm-direct", containerID))
	assert.Equal(t,
		containerStatsHubKey("tyo-direct", containerID),
		containerStatsHubKey("tyo-direct", containerID))
}

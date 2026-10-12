package correlation

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.getarcane.app/kit/pkg"
)

func TestDockerExpectationIdentityAndExpiry(t *testing.T) {
	t.Parallel()
	id := strings.Repeat("abcdef12", 8)
	for _, tc := range []struct {
		name, kind, expectedID, expectedName, actorID, actorName string
		want                                                     bool
	}{
		{"exact ID", "container", id, "", id, "", true},
		{"short ID", "container", id[:12], "", id, "", true},
		{"too short ID", "container", id[:11], "", id, "", false},
		{"name", "container", "", "web", "different", "web", true},
		{"placeholder name", "container", "", "name", "different", "name", false},
		{"image digest", "image", "sha256:" + id[:12], "", "sha256:" + id, "", true},
		{"image reference prefix", "image", "registry/image", "", "registry/image:latest", "", false},
		{"volume hex prefix", "volume", id[:12], "", id, "", false},
		{"exact volume name", "volume", "data", "", "data", "", true},
		{"invalid hex", "container", strings.Repeat("z", 12), "", strings.Repeat("z", 64), "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now()
			svc := NewService(func() time.Time { return now })
			svc.MarkExpectation("0", tc.kind, tc.expectedID, tc.expectedName)
			require.Equal(t, tc.want, svc.ShouldSuppress("0", tc.kind, tc.actorID, tc.actorName, ""))
			require.False(t, svc.ShouldSuppress("0", "other", tc.actorID, tc.actorName, ""))
			now = now.Add(59 * time.Second)
			require.Equal(t, tc.want, svc.ShouldSuppress("0", tc.kind, tc.actorID, tc.actorName, ""))
			now = now.Add(time.Second)
			require.False(t, svc.ShouldSuppress("0", tc.kind, tc.actorID, tc.actorName, ""))
		})
	}
}

func TestDockerSuppressionWindowsOverlapAndGrace(t *testing.T) {
	t.Parallel()
	for _, compose := range []bool{false, true} {
		name := kit.Ternary(compose, "compose", "resource")
		t.Run(name, func(t *testing.T) {
			now := time.Now()
			svc := NewService(func() time.Time { return now })
			open := func() func() {
				if compose {
					return svc.BeginComposeWindow("0", "demo")
				}
				return svc.BeginResourceWindow("0", "container", "id", "web")
			}
			first := open()
			second := open()
			first()
			first()
			now = now.Add(time.Hour)
			require.True(t, svc.ShouldSuppress("0", "container", "id", "web", "demo"))
			if compose {
				require.False(t, svc.ShouldSuppress("0", "container", "id", "web", "other"))
				require.False(t, svc.ShouldSuppress("0", "container", "id", "web", ""))
				for _, kind := range []string{"image", "volume", "network"} {
					require.False(t, svc.ShouldSuppress("0", kind, "id", "name", ""))
					require.True(t, svc.ShouldSuppress("0", kind, "id", "name", "demo"))
					require.False(t, svc.ShouldSuppress("0", kind, "id", "name", "other"))
				}
			}
			require.False(t, svc.ShouldSuppress("0", "container", "unrelated", "other", "other"))
			second()
			second()
			now = now.Add(9 * time.Second)
			for _, kind := range []string{"image", "volume", "network"} {
				require.False(t, svc.ShouldSuppress("0", kind, "unrelated", "other", ""))
			}
			require.True(t, svc.ShouldSuppress("0", "container", "id", "web", "demo"))
			now = now.Add(time.Second)
			require.False(t, svc.ShouldSuppress("0", "container", "id", "web", "demo"))
			require.False(t, svc.ShouldSuppress("0", "image", "id", "name", ""))
		})
	}
}

func TestDockerResourceWindowsCoverLongOperationsInternal(t *testing.T) {
	now := time.Now()
	svc := NewService(func() time.Time { return now })
	closeFirst := svc.BeginResourceWindow("0", "image", "", "nginx:latest")
	closeSecond := svc.BeginResourceWindow("0", "image", "", "nginx:latest")
	closeFirst()
	closeFirst()
	now = now.Add(time.Hour)
	require.True(t, svc.ShouldSuppress("0", "image", "nginx:latest", "", ""))
	require.False(t, svc.ShouldSuppress("0", "image", "redis:latest", "", ""))
	closeSecond()
	now = now.Add(9 * time.Second)
	require.True(t, svc.ShouldSuppress("0", "image", "nginx:latest", "", ""))
	now = now.Add(time.Second)
	require.False(t, svc.ShouldSuppress("0", "image", "nginx:latest", "", ""))
}

// TestDockerSuppressionIsScopedToEnvironment is the regression guard for the reported gap: two
// daemons can each hold a container, image or volume with the same name, so an action against one
// environment's daemon must never hide the other environment's daemon events.
func TestDockerSuppressionIsScopedToEnvironment(t *testing.T) {
	t.Parallel()
	const environmentA, environmentB = "env-a", "env-b"
	id := strings.Repeat("abcdef12", 8)

	t.Run("expectations", func(t *testing.T) {
		t.Parallel()
		now := time.Now()
		svc := NewService(func() time.Time { return now })
		// A resource that cannot be told apart by ID or name across daemons.
		svc.MarkExpectation(environmentA, "container", id, "web")
		require.True(t, svc.ShouldSuppress(environmentA, "container", id, "web", ""))
		require.False(t, svc.ShouldSuppress(environmentB, "container", id, "web", ""))
		require.False(t, svc.ShouldSuppress("0", "container", id, "web", ""))

		// The manager's own daemon stays isolated from every docker-host environment.
		svc.MarkExpectation("0", "image", "", "nginx:latest")
		require.True(t, svc.ShouldSuppress("0", "image", "", "nginx:latest", ""))
		require.False(t, svc.ShouldSuppress(environmentA, "image", "", "nginx:latest", ""))
	})

	t.Run("windows", func(t *testing.T) {
		t.Parallel()
		now := time.Now()
		svc := NewService(func() time.Time { return now })
		closeCompose := svc.BeginComposeWindow(environmentA, "demo")
		require.True(t, svc.ShouldSuppress(environmentA, "container", "whatever", "web", "demo"))
		require.False(t, svc.ShouldSuppress(environmentB, "container", "whatever", "web", "demo"))

		closeResource := svc.BeginResourceWindow(environmentB, "volume", "data", "data")
		require.True(t, svc.ShouldSuppress(environmentB, "volume", "data", "data", ""))
		require.False(t, svc.ShouldSuppress(environmentA, "volume", "data", "data", ""))

		closeType := svc.BeginTypeWindow(environmentB, "image")
		require.True(t, svc.ShouldSuppress(environmentB, "image", "", "anything:latest", ""))
		require.False(t, svc.ShouldSuppress(environmentA, "image", "", "anything:latest", ""))

		closeCompose()
		closeResource()
		closeType()
	})
}

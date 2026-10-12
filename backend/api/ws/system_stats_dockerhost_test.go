package ws

import (
	"context"
	"encoding/json/v2"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	systemtypes "github.com/getarcaneapp/arcane/types/v2/system"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/getarcaneapp/arcane/backend/v2/internal/config"
	"github.com/getarcaneapp/arcane/backend/v2/internal/container"
	dockerInternal "github.com/getarcaneapp/arcane/backend/v2/internal/docker"
	"github.com/getarcaneapp/arcane/backend/v2/internal/system"
	"github.com/getarcaneapp/arcane/backend/v2/pkg/libarcane/envcontext"
)

var dockerHostStatsDaemonPathInternal = regexp.MustCompile(`^/v\d+\.\d+`)

// newDockerHostStatsDaemonInternal serves the subset of the Docker API a docker-host environment
// reading needs, reporting a two-core 12.5 GB host that is not the machine running the tests.
func newDockerHostStatsDaemonInternal(t *testing.T) *httptest.Server {
	t.Helper()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := dockerHostStatsDaemonPathInternal.ReplaceAllString(r.URL.Path, "")
		w.Header().Set("Content-Type", "application/json")
		switch {
		case path == "/_ping":
			w.Header().Set("Api-Version", "1.41")
			w.WriteHeader(http.StatusOK)
		case path == "/info":
			writeDockerHostStatsPayloadInternal(w, map[string]any{
				"NCPU": 2, "MemTotal": 12530077696, "Name": "arm", "OSType": "linux", "Architecture": "aarch64",
			})
		case path == "/containers/json":
			writeDockerHostStatsPayloadInternal(w, []map[string]any{
				{"Id": "c1", "Names": []string{"/snell-server"}, "Image": "img:latest", "State": "running"},
				{"Id": "c2", "Names": []string{"/qbittorrent"}, "Image": "img:latest", "State": "running"},
			})
		case strings.HasPrefix(path, "/containers/") && strings.HasSuffix(path, "/stats"):
			memoryUsage := 104857600
			totalUsage := 2000
			if strings.Contains(path, "/c2/") {
				memoryUsage = 52428800
				totalUsage = 1500
			}
			writeDockerHostStatsPayloadInternal(w, map[string]any{
				"read":         "2026-10-11T10:00:00Z",
				"cpu_stats":    map[string]any{"cpu_usage": map[string]any{"total_usage": totalUsage}, "system_cpu_usage": 2000},
				"precpu_stats": map[string]any{"cpu_usage": map[string]any{"total_usage": 1000}, "system_cpu_usage": 1000},
				"memory_stats": map[string]any{"usage": memoryUsage, "limit": 12530077696},
			})
		case path == "/system/df":
			writeDockerHostStatsPayloadInternal(w, map[string]any{
				"LayersSize": 2000000000,
				"Images":     []map[string]any{},
				"Containers": []map[string]any{{"Id": "c1", "SizeRw": 1000000}, {"Id": "c2", "SizeRw": 2000000}},
				"Volumes":    []map[string]any{{"Name": "v1", "UsageData": map[string]any{"Size": 900000000, "RefCount": 1}}},
				"BuildCache": []map[string]any{{"ID": "b1", "Size": 100000000, "Shared": false}},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)

	return server
}

func writeDockerHostStatsPayloadInternal(w http.ResponseWriter, payload any) {
	body, err := json.Marshal(payload)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_, _ = w.Write(body)
}

// TestSystemStatsStreamsDockerHostEnvironmentStatsFromDockerAPI is the regression test for the
// dashboard showing the manager's own resources on a docker-host environment card: the stats
// websocket must stream readings taken from the environment's Docker API instead of the local
// sampler.
func TestSystemStatsStreamsDockerHostEnvironmentStatsFromDockerAPI(t *testing.T) {
	daemon := newDockerHostStatsDaemonInternal(t)

	dockerService := dockerInternal.NewDockerClientService(t.Context(), nil, &config.Config{}, nil)
	containerService := container.NewContainerService(nil, dockerService, nil, nil, nil, nil, nil)
	systemService := system.NewSystemService(
		nil, dockerService, containerService,
		nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, //nolint:govet // optional collaborators the stats path never touches
		&config.Config{},
	)

	handler := &WebSocketHandler{
		systemService: systemService,
		checkWSOrigin: func(*http.Request) bool { return true },
	}

	echoServer := echo.New()
	echoServer.GET("/api/environments/:id/ws/system/stats", func(c *echo.Context) error {
		// The environment middleware attaches the resolved docker-host target to the request
		// context; this reproduces exactly that.
		ctx := envcontext.WithTarget(c.Request().Context(), envcontext.Target{
			EnvironmentID: c.Param("id"),
			DockerHost:    daemon.URL,
		})
		c.SetRequest(c.Request().WithContext(ctx))
		return handler.SystemStats(c)
	})
	httpServer := httptest.NewServer(echoServer)
	t.Cleanup(httpServer.Close)

	readCtx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()

	conn, _, err := websocket.Dial(readCtx, "ws"+strings.TrimPrefix(httpServer.URL, "http")+"/api/environments/env-arm/ws/system/stats", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.CloseNow() })

	_, payload, err := conn.Read(readCtx)
	require.NoError(t, err)

	var stats systemtypes.SystemStats
	require.NoError(t, json.Unmarshal(payload, &stats))

	// Host facts and capacity belong to the remote daemon's machine, never to the test machine.
	require.Equal(t, 2, stats.CPUCount)
	require.Equal(t, uint64(12530077696), stats.MemoryTotal)
	require.Equal(t, "arm", stats.Hostname)
	// 100% + 50% of a core on two cores, and both containers' resident memory.
	require.InDelta(t, 75, stats.CPUUsage, 0.01)
	require.Equal(t, uint64(157286400), stats.MemoryUsage)
	require.Equal(t, uint64(3003000000), stats.DiskUsage)
}

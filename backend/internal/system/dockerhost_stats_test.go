package system

import (
	"encoding/json/v2"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/getarcaneapp/arcane/backend/v2/internal/config"
	"github.com/getarcaneapp/arcane/backend/v2/internal/container"
	dockerInternal "github.com/getarcaneapp/arcane/backend/v2/internal/docker"
	"github.com/getarcaneapp/arcane/backend/v2/pkg/libarcane/envcontext"
)

var dockerHostStatsTestPathInternal = regexp.MustCompile(`^/v\d+\.\d+`)

// dockerHostStatsServerInternal is a fake Docker daemon for one docker-host environment: two running
// containers and one exited one, with host facts that differ from the machine running the tests.
type dockerHostStatsServerInternal struct {
	mu            sync.Mutex
	infoCalls     int
	diskUsageCall int
	failDiskUsage bool
}

func newDockerHostStatsServerInternal(t *testing.T, server *dockerHostStatsServerInternal) *httptest.Server {
	t.Helper()

	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := dockerHostStatsTestPathInternal.ReplaceAllString(r.URL.Path, "")
		switch {
		case path == "/_ping":
			w.Header().Set("Api-Version", "1.41")
			w.WriteHeader(http.StatusOK)
		case path == "/info":
			server.mu.Lock()
			server.infoCalls++
			server.mu.Unlock()
			writeDockerHostStatsJSONInternal(w, map[string]any{
				"NCPU":         2,
				"MemTotal":     12530077696,
				"Name":         "arm",
				"OSType":       "linux",
				"Architecture": "aarch64",
			})
		case path == "/containers/json":
			writeDockerHostStatsJSONInternal(w, []map[string]any{
				{"Id": "c1", "Names": []string{"/snell-server"}, "Image": "img:latest", "State": "running"},
				{"Id": "c2", "Names": []string{"/qbittorrent"}, "Image": "img:latest", "State": "running"},
				{"Id": "c3", "Names": []string{"/vertex"}, "Image": "img:latest", "State": "exited"},
			})
		case strings.HasPrefix(path, "/containers/") && strings.HasSuffix(path, "/stats"):
			writeDockerHostStatsJSONInternal(w, dockerHostStatsSampleInternal(strings.TrimSuffix(strings.TrimPrefix(path, "/containers/"), "/stats")))
		case path == "/system/df":
			server.mu.Lock()
			server.diskUsageCall++
			fail := server.failDiskUsage
			server.mu.Unlock()
			if fail {
				http.Error(w, "disk usage unavailable", http.StatusInternalServerError)
				return
			}
			writeDockerHostStatsJSONInternal(w, map[string]any{
				"LayersSize": 2000000000,
				"Images":     []map[string]any{},
				"Containers": []map[string]any{
					{"Id": "c1", "SizeRw": 1000000},
					{"Id": "c2", "SizeRw": 2000000},
					{"Id": "c3", "SizeRw": 500000},
				},
				"Volumes":    []map[string]any{{"Name": "v1", "UsageData": map[string]any{"Size": 900000000, "RefCount": 1}}},
				"BuildCache": []map[string]any{{"ID": "b1", "Size": 100000000, "Shared": false}},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(httpServer.Close)

	return httpServer
}

// dockerHostStatsSampleInternal reports 100% of one core for c1, 50% for c2 and 100 MiB / 50 MiB of
// resident memory, the shape the Docker stats endpoint returns for a non-streaming request.
func dockerHostStatsSampleInternal(id string) map[string]any {
	totalUsage := 2000
	memoryUsage := 104857600
	if id == "c2" {
		totalUsage = 1500
		memoryUsage = 52428800
	}
	return map[string]any{
		"read": "2026-10-11T10:00:00Z",
		"cpu_stats": map[string]any{
			"cpu_usage":        map[string]any{"total_usage": totalUsage},
			"system_cpu_usage": 2000,
		},
		"precpu_stats": map[string]any{
			"cpu_usage":        map[string]any{"total_usage": 1000},
			"system_cpu_usage": 1000,
		},
		"memory_stats": map[string]any{"usage": memoryUsage, "limit": 12530077696},
	}
}

func writeDockerHostStatsJSONInternal(w http.ResponseWriter, payload any) {
	body, err := json.Marshal(payload)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(body)
}

func newDockerHostStatsSystemServiceInternal(t *testing.T) *SystemService {
	t.Helper()

	dockerService := dockerInternal.NewDockerClientService(t.Context(), nil, &config.Config{}, nil)
	return &SystemService{
		dockerService:    dockerService,
		containerService: container.NewContainerService(nil, dockerService, nil, nil, nil, nil, nil),
	}
}

func dockerHostStatsContextInternal(t *testing.T, dockerHost string) envcontext.Target {
	t.Helper()

	return envcontext.Target{EnvironmentID: "env-arm", DockerHost: dockerHost}
}

func TestDockerHostSystemStatsReadsRemoteDockerAPI(t *testing.T) {
	server := newDockerHostStatsServerInternal(t, &dockerHostStatsServerInternal{})
	service := newDockerHostStatsSystemServiceInternal(t)

	ctx := envcontext.WithTarget(t.Context(), dockerHostStatsContextInternal(t, server.URL))
	stats, err := service.DockerHostSystemStats(ctx)
	require.NoError(t, err)

	// Host facts come from the remote daemon, never from the machine running the manager.
	require.Equal(t, 2, stats.CPUCount)
	require.Equal(t, uint64(12530077696), stats.MemoryTotal)
	require.Equal(t, "arm", stats.Hostname)
	require.Equal(t, "aarch64", stats.Architecture)
	require.Equal(t, "linux", stats.Platform)

	// 100% + 50% of a core on a two-core host is 75% host-wide; only the running containers count.
	require.InDelta(t, 75, stats.CPUUsage, 0.01)
	require.Equal(t, uint64(157286400), stats.MemoryUsage)

	// The Docker footprint mirrors `docker system df`; a daemon cannot report the host disk size.
	require.Equal(t, uint64(3003500000), stats.DiskUsage)
	require.Equal(t, uint64(0), stats.DiskTotal)
	require.Equal(t, 0, stats.GPUCount)
}

func TestDockerHostSystemStatsCachesHostFactsAndDiskUsage(t *testing.T) {
	daemon := &dockerHostStatsServerInternal{}
	server := newDockerHostStatsServerInternal(t, daemon)
	service := newDockerHostStatsSystemServiceInternal(t)

	ctx := envcontext.WithTarget(t.Context(), dockerHostStatsContextInternal(t, server.URL))
	_, err := service.DockerHostSystemStats(ctx)
	require.NoError(t, err)
	_, err = service.DockerHostSystemStats(ctx)
	require.NoError(t, err)

	daemon.mu.Lock()
	defer daemon.mu.Unlock()
	require.Equal(t, 1, daemon.infoCalls)
	require.Equal(t, 1, daemon.diskUsageCall)
}

func TestDockerHostSystemStatsKeepsCPUMemoryWhenDiskUsageFails(t *testing.T) {
	server := newDockerHostStatsServerInternal(t, &dockerHostStatsServerInternal{failDiskUsage: true})
	service := newDockerHostStatsSystemServiceInternal(t)

	ctx := envcontext.WithTarget(t.Context(), dockerHostStatsContextInternal(t, server.URL))
	stats, err := service.DockerHostSystemStats(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(0), stats.DiskUsage)
	require.InDelta(t, 75, stats.CPUUsage, 0.01)
	require.Equal(t, uint64(12530077696), stats.MemoryTotal)
}

func TestDockerHostSystemStatsRequiresDockerHostTarget(t *testing.T) {
	service := newDockerHostStatsSystemServiceInternal(t)

	_, err := service.DockerHostSystemStats(t.Context())
	require.ErrorContains(t, err, "no Docker API endpoint in context")
}

func TestDockerHostSystemStatsKeepsEndpointsApart(t *testing.T) {
	armDaemon := &dockerHostStatsServerInternal{}
	armServer := newDockerHostStatsServerInternal(t, armDaemon)
	tyoServer := newDockerHostStatsServerInternal(t, &dockerHostStatsServerInternal{})
	service := newDockerHostStatsSystemServiceInternal(t)

	firstCtx := envcontext.WithTarget(t.Context(), dockerHostStatsContextInternal(t, tyoServer.URL))
	secondCtx := envcontext.WithTarget(t.Context(), dockerHostStatsContextInternal(t, armServer.URL))

	first, err := service.DockerHostSystemStats(firstCtx)
	require.NoError(t, err)
	second, err := service.DockerHostSystemStats(secondCtx)
	require.NoError(t, err)

	// Both fake daemons report the same facts, so this only proves the caches are keyed by endpoint:
	// the second call must have asked its own daemon for /info.
	require.Equal(t, first.MemoryTotal, second.MemoryTotal)
	armDaemon.mu.Lock()
	defer armDaemon.mu.Unlock()
	require.Equal(t, 1, armDaemon.infoCalls)
}

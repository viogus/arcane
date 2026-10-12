package system

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	systemtypes "github.com/getarcaneapp/arcane/types/v2/system"
	"github.com/moby/moby/client"

	"github.com/getarcaneapp/arcane/backend/v2/pkg/libarcane/envcontext"
)

const (
	// A daemon's CPU count, memory and hostname only change on restart or reconfiguration.
	dockerHostReadingTTL = time.Minute
	// Disk usage makes the daemon walk its bookkeeping, so a dashboard poll tolerates a stale view.
	dockerHostDiskUsageTTL = 30 * time.Second
)

// dockerHostReadingInternal caches the host facts of one Docker API endpoint.
type dockerHostReadingInternal struct {
	readAt       time.Time
	cpuCount     int
	memoryTotal  uint64
	hostname     string
	platform     string
	architecture string
}

// dockerHostDiskUsageInternal caches the Docker disk footprint of one Docker API endpoint.
type dockerHostDiskUsageInternal struct {
	readAt    time.Time
	footprint int64
}

// DockerHostSystemStats reads environment stats for a docker-host environment from its Docker API.
//
// An agent samples the machine it runs on, but a docker-host environment has no agent: the manager
// talks to a daemon, and the manager's own sampler reports the machine the manager runs on, which is
// the wrong machine. CPU and memory are therefore the running containers' usage measured against the
// remote host's capacity, and disk is that daemon's Docker footprint. The API exposes no host
// filesystem size, so DiskTotal stays zero and the UI shows the footprint without a percentage.
func (s *SystemService) DockerHostSystemStats(ctx context.Context) (systemtypes.SystemStats, error) {
	host := envcontext.DockerHostFromContext(ctx)
	if host == "" {
		return systemtypes.SystemStats{}, errors.New("docker-host environment has no Docker API endpoint in context")
	}

	reading, err := s.dockerHostReadingInternal(ctx, host)
	if err != nil {
		return systemtypes.SystemStats{}, err
	}
	samples, err := s.containerService.SumRunningResourceSamples(ctx)
	if err != nil {
		return systemtypes.SystemStats{}, fmt.Errorf("failed to sample containers on %s: %w", host, err)
	}

	// CPU and memory alone are still a useful reading, so a daemon that refuses /system/df only
	// degrades the disk tile.
	footprint, diskErr := s.dockerHostDiskUsageInternal(ctx, host)
	if diskErr != nil {
		slog.WarnContext(ctx, "Failed to read Docker disk usage for docker-host environment", "dockerHost", host, "error", diskErr)
	}

	// Per-container CPU percent is measured against a single core, so summing gives core-worth of
	// load; dividing by the host's core count turns it into the host-wide usage the dashboard tile
	// means, and no host can exceed its own capacity.
	cpuUsage := 0.0
	if reading.cpuCount > 0 {
		cpuUsage = min(samples.CPUPercent/float64(reading.cpuCount), 100)
	}

	return systemtypes.SystemStats{
		CPUUsage:     cpuUsage,
		MemoryUsage:  samples.MemoryUsageBytes,
		MemoryTotal:  reading.memoryTotal,
		DiskUsage:    uint64(max(footprint, 0)),
		DiskTotal:    0,
		CPUCount:     reading.cpuCount,
		Architecture: reading.architecture,
		Platform:     reading.platform,
		Hostname:     reading.hostname,
	}, nil
}

// dockerHostReadingInternal returns the cached host facts for host, refreshing them when stale.
func (s *SystemService) dockerHostReadingInternal(ctx context.Context, host string) (dockerHostReadingInternal, error) {
	s.dockerHostStatsMu.Lock()
	cached, ok := s.dockerHostReadings[host]
	s.dockerHostStatsMu.Unlock()
	if ok && time.Since(cached.readAt) < dockerHostReadingTTL {
		return cached, nil
	}

	dockerClient, err := s.dockerService.GetClient(ctx)
	if err != nil {
		return dockerHostReadingInternal{}, fmt.Errorf("failed to connect to Docker host %s: %w", host, err)
	}
	infoResult, err := dockerClient.Info(ctx, client.InfoOptions{})
	if err != nil {
		return dockerHostReadingInternal{}, fmt.Errorf("failed to read Docker info from %s: %w", host, err)
	}
	info := infoResult.Info

	reading := dockerHostReadingInternal{
		readAt:       time.Now(),
		cpuCount:     info.NCPU,
		memoryTotal:  uint64(max(info.MemTotal, 0)),
		hostname:     info.Name,
		platform:     info.OSType,
		architecture: info.Architecture,
	}
	s.dockerHostStatsMu.Lock()
	if s.dockerHostReadings == nil {
		s.dockerHostReadings = make(map[string]dockerHostReadingInternal)
	}
	s.dockerHostReadings[host] = reading
	s.dockerHostStatsMu.Unlock()
	return reading, nil
}

// dockerHostDiskUsageInternal returns the cached Docker disk footprint for host, refreshing it when
// stale. The footprint mirrors `docker system df`: images (deduplicated layers), containers, local
// volumes and build cache.
func (s *SystemService) dockerHostDiskUsageInternal(ctx context.Context, host string) (int64, error) {
	s.dockerHostStatsMu.Lock()
	cached, ok := s.dockerHostDiskUsages[host]
	s.dockerHostStatsMu.Unlock()
	if ok && time.Since(cached.readAt) < dockerHostDiskUsageTTL {
		return cached.footprint, nil
	}

	dockerClient, err := s.dockerService.GetClient(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to connect to Docker host %s: %w", host, err)
	}
	// Verbose stays off: only the per-type totals are needed, and they are what the daemon computes
	// for its own `docker system df`.
	usage, err := dockerClient.DiskUsage(ctx, client.DiskUsageOptions{Containers: true, Images: true, Volumes: true, BuildCache: true})
	if err != nil {
		return 0, fmt.Errorf("failed to read Docker disk usage from %s: %w", host, err)
	}

	footprint := usage.Images.TotalSize + usage.Containers.TotalSize + usage.Volumes.TotalSize + usage.BuildCache.TotalSize
	entry := dockerHostDiskUsageInternal{readAt: time.Now(), footprint: footprint}
	s.dockerHostStatsMu.Lock()
	if s.dockerHostDiskUsages == nil {
		s.dockerHostDiskUsages = make(map[string]dockerHostDiskUsageInternal)
	}
	s.dockerHostDiskUsages[host] = entry
	s.dockerHostStatsMu.Unlock()
	return footprint, nil
}

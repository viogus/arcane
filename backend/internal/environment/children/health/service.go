// Package health probes environment connectivity over direct HTTP, edge
// tunnels or the local Docker socket.
package health

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/moby/moby/client"

	"github.com/getarcaneapp/arcane/backend/v2/internal/docker"
	"github.com/getarcaneapp/arcane/backend/v2/pkg/libarcane/edge"
	"github.com/getarcaneapp/arcane/backend/v2/pkg/utils/httpx"
)

// Service runs connectivity probes; the parent persists the resulting status.
type Service struct {
	httpClient    *http.Client
	dockerService *docker.DockerClientService
}

func New(httpClient *http.Client, dockerService *docker.DockerClientService) *Service {
	return &Service{httpClient: httpClient, dockerService: dockerService}
}

// Target describes the environment being probed.
type Target struct {
	// ID is the environment UUID ("0" is the manager's own environment).
	ID string
	// APIURL is the agent base URL, used for agent and edge environments.
	APIURL string
	// IsEdge selects tunnel-based probing.
	IsEdge bool
	// CustomURL forces a direct HTTP probe against APIURL (connection test with an override).
	CustomURL bool
	// DockerHost is the Docker API endpoint of a "docker-host" environment, probed with a Docker ping.
	DockerHost string
}

// Probe reports "online", "offline" or "error" for one environment. A custom
// API URL always probes directly over HTTP.
func (s *Service) Probe(ctx context.Context, target Target, authenticated func(context.Context) (int, error)) (string, error) {
	if target.ID == "0" && !target.CustomURL {
		reqCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		if _, pingErr := s.dockerService.Call(reqCtx, func(ctx context.Context, cli *client.Client) (client.PingResult, error) {
			return cli.Ping(ctx, client.PingOptions{})
		}); pingErr != nil {
			return "offline", fmt.Errorf("docker ping failed: %w", pingErr)
		}
		return "online", nil
	}
	if target.DockerHost != "" && !target.CustomURL {
		return s.probeDockerHostInternal(ctx, target.DockerHost)
	}
	if target.IsEdge && !target.CustomURL {
		// The probe is standing tunnel demand, keeping idle poll-mode tunnels
		// open across check cycles instead of letting them flap.
		edge.TouchTunnelDemand(target.ID, edge.DefaultTunnelDemandTTL)
		if !edge.HasActiveTunnel(target.ID) {
			if _, ok := edge.RequestTunnelAndWait(ctx, target.ID, edge.DefaultTunnelDemandTTL, edge.DefaultTunnelAcquireTimeout()).Get(); !ok {
				return "offline", errors.New("edge agent is not connected")
			}
		}
	}
	if !target.CustomURL {
		reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()

		statusCode, err := authenticated(reqCtx)
		switch {
		case err != nil:
			return "offline", fmt.Errorf("health check failed: %w", err)
		case statusCode == http.StatusOK:
			return "online", nil
		case statusCode == http.StatusUnauthorized || statusCode == http.StatusForbidden:
			return "error", fmt.Errorf("environment rejected the access token: status code %d", statusCode)
		default:
			return "error", fmt.Errorf("unexpected status code: %d", statusCode)
		}
	}

	baseURL, err := httpx.NormalizeBaseURL(target.APIURL)
	if err != nil {
		return "offline", fmt.Errorf("invalid environment API URL: %w", err)
	}

	reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, strings.TrimRight(baseURL, "/")+"/api/health", http.NoBody)
	if err != nil {
		return "offline", fmt.Errorf("failed to create request: %w", err)
	}
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return "offline", fmt.Errorf("connection failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusOK {
		return "online", nil
	}
	return "error", fmt.Errorf("unexpected status code: %d", resp.StatusCode)
}

// probeDockerHostInternal pings a "docker-host" environment's Docker API endpoint. The manager
// talks to that daemon directly, so Docker API reachability is the entire health signal.
func (s *Service) probeDockerHostInternal(ctx context.Context, host string) (string, error) {
	reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	dockerClient, err := s.dockerService.GetHostClient(reqCtx, host)
	if err != nil {
		return "offline", fmt.Errorf("failed to connect to Docker host %q: %w", host, err)
	}
	if _, pingErr := dockerClient.Ping(reqCtx, client.PingOptions{}); pingErr != nil {
		return "offline", fmt.Errorf("docker ping failed for %q: %w", host, pingErr)
	}
	return "online", nil
}

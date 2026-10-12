package swarm

import (
	"bytes"
	"context"
	stdjson "encoding/json"
	"encoding/json/v2"
	"errors"
	"fmt"
	"log/slog"
	"net/netip"
	"strings"

	swarmtypes "github.com/getarcaneapp/arcane/types/v2/swarm"
	"github.com/moby/moby/api/types/swarm"
	"github.com/moby/moby/api/types/system"
	"github.com/moby/moby/client"
	"go.getarcane.app/kit/pkg"

	"github.com/getarcaneapp/arcane/backend/v2/internal/common"
	"github.com/getarcaneapp/arcane/backend/v2/internal/docker"
	"github.com/getarcaneapp/arcane/backend/v2/internal/environment"
	"github.com/getarcaneapp/arcane/backend/v2/internal/kv"
	"github.com/getarcaneapp/arcane/backend/v2/internal/registry"
	"github.com/getarcaneapp/arcane/backend/v2/internal/settings"
	"github.com/getarcaneapp/arcane/backend/v2/internal/swarm/children/configs"
	"github.com/getarcaneapp/arcane/backend/v2/internal/swarm/children/nodes"
	"github.com/getarcaneapp/arcane/backend/v2/internal/swarm/children/secrets"
	"github.com/getarcaneapp/arcane/backend/v2/internal/swarm/children/services"
	"github.com/getarcaneapp/arcane/backend/v2/internal/swarm/children/stacks"
	"github.com/getarcaneapp/arcane/backend/v2/pkg/libarcane/envcontext"
	"github.com/getarcaneapp/arcane/backend/v2/pkg/pagination"
)

const (
	KVKeySwarmEnabled      = "swarm.enabled"
	defaultSwarmListenAddr = "0.0.0.0:2377"
)

// SwarmService provides Docker Swarm cluster operations and coordinates its
// services, nodes, stacks, configs, and secrets features.
type SwarmService struct {
	dockerService      *docker.DockerClientService
	kvService          *kv.KVService
	environmentService *environment.EnvironmentService

	services *services.Service
	nodes    *nodes.Service
	stacks   *stacks.Service
	configs  *configs.Service
	secrets  *secrets.Service
}

func NewSwarmService(
	dockerService *docker.DockerClientService,
	settingsService *settings.SettingsService,
	kvService *kv.KVService,
	registryService *registry.ContainerRegistryService,
	environmentService *environment.EnvironmentService,
) *SwarmService {
	s := &SwarmService{dockerService: dockerService, kvService: kvService, environmentService: environmentService}

	var registryAuth func(context.Context, string) (string, error)
	if registryService != nil {
		registryAuth = registryService.GetRegistryAuthForImage
	}

	s.services = services.NewService(dockerService.GetClient, s.ensureSwarmManagerInternal, s.listTasksPaginatedWithFiltersInternal)
	s.nodes = nodes.NewService(dockerService.GetClient, s.ensureSwarmManagerInternal, s.listTasksPaginatedWithFiltersInternal, s.GetSwarmJoinTokens, s.joinSwarmInternal, environmentService)
	s.stacks = stacks.NewService(dockerService.GetClient, s.ensureSwarmManagerInternal, s.listTasksPaginatedWithFiltersInternal, s.services.PaginateSummaries, registryAuth, settingsService)
	s.configs = configs.NewService(dockerService.GetClient, s.ensureSwarmManagerInternal)
	s.secrets = secrets.NewService(dockerService.GetClient, s.ensureSwarmManagerInternal)
	return s
}

// DeployStack deploys a swarm stack; GitOps sync uses it as the domain API.
func (s *SwarmService) DeployStack(ctx context.Context, environmentID string, req swarmtypes.StackDeployRequest) (*swarmtypes.StackDeployResponse, error) {
	return s.stacks.DeployStack(ctx, environmentID, req)
}

// StreamServiceLogs streams swarm service logs for the WebSocket log endpoint.
func (s *SwarmService) StreamServiceLogs(ctx context.Context, serviceID string, logsChan chan<- string, follow bool, tail, since string, timestamps bool) error {
	return s.services.StreamServiceLogs(ctx, serviceID, logsChan, follow, tail, since, timestamps)
}

func (s *SwarmService) IsEnabled(ctx context.Context) (bool, error) {
	if s.kvService == nil {
		return false, nil
	}

	enabled, err := s.kvService.GetBool(ctx, KVKeySwarmEnabled, false)
	if err != nil {
		return false, fmt.Errorf("failed to read swarm enabled state: %w", err)
	}

	return enabled, nil
}

// IsEnabledForContext reports whether the environment the request is scoped to
// is in a swarm. The manager's own daemon answers for the local environment,
// while a docker-host environment reports its own daemon's state, so the flag is
// not shared between environments. When the daemon cannot be inspected it falls
// back to the persisted manager-wide flag.
func (s *SwarmService) IsEnabledForContext(ctx context.Context) bool {
	if s == nil {
		return false
	}

	if s.dockerService != nil {
		if dockerClient, err := s.dockerService.GetClient(ctx); err == nil {
			if infoResult, infoErr := dockerClient.Info(ctx, client.InfoOptions{}); infoErr == nil {
				return infoResult.Info.Swarm.LocalNodeState == swarm.LocalNodeStateActive
			}
		}
	}

	enabled, err := s.IsEnabled(ctx)
	if err != nil {
		return false
	}
	return enabled
}

func (s *SwarmService) ListTasksPaginated(ctx context.Context, params pagination.QueryParams) ([]swarmtypes.TaskSummary, pagination.Response, error) {
	if err := s.ensureSwarmManagerInternal(ctx); err != nil {
		return nil, pagination.Response{}, err
	}

	return s.listTasksPaginatedWithFiltersInternal(ctx, nil, params)
}

func (s *SwarmService) GetSwarmInfo(ctx context.Context) (*swarmtypes.SwarmInfo, error) {
	if err := s.ensureSwarmManagerInternal(ctx); err != nil {
		return nil, err
	}

	dockerClient, err := s.dockerService.GetClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Docker: %w", err)
	}

	infoResult, err := dockerClient.SwarmInspect(ctx, client.SwarmInspectOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to inspect swarm: %w", err)
	}

	return new(swarmtypes.NewSwarmInfo(infoResult.Swarm)), nil
}

func (s *SwarmService) InitSwarm(ctx context.Context, req swarmtypes.SwarmInitRequest) (*swarmtypes.SwarmInitResponse, error) {
	dockerClient, err := s.dockerService.GetClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Docker: %w", err)
	}

	spec, err := decodeSwarmSpecInternal(req.Spec)
	if err != nil {
		return nil, err
	}

	defaultAddrPool := make([]netip.Prefix, 0, len(req.DefaultAddrPool))
	for _, raw := range req.DefaultAddrPool {
		prefix, parsePrefixErr := netip.ParsePrefix(raw)
		if parsePrefixErr != nil {
			return nil, fmt.Errorf("failed to parse default address pool %q: %w", raw, parsePrefixErr)
		}
		defaultAddrPool = append(defaultAddrPool, prefix.Masked())
	}

	initResult, err := dockerClient.SwarmInit(ctx, client.SwarmInitOptions{
		ListenAddr:       defaultSwarmListenAddrInternal(req.ListenAddr),
		AdvertiseAddr:    req.AdvertiseAddr,
		DataPathAddr:     req.DataPathAddr,
		DataPathPort:     req.DataPathPort,
		ForceNewCluster:  req.ForceNewCluster,
		Spec:             spec,
		AutoLockManagers: req.AutoLockManagers,
		Availability:     req.Availability,
		DefaultAddrPool:  defaultAddrPool,
		SubnetSize:       req.SubnetSize,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to initialize swarm: %w", err)
	}

	s.persistSwarmEnabledStateInternal(ctx, true)

	return &swarmtypes.SwarmInitResponse{NodeID: initResult.NodeID}, nil
}

func (s *SwarmService) JoinSwarm(ctx context.Context, req swarmtypes.SwarmJoinRequest) error {
	if err := s.joinSwarmInternal(ctx, req); err != nil {
		return err
	}

	s.persistSwarmEnabledStateInternal(ctx, true)

	return nil
}

// joinSwarmInternal joins the swarm cluster the request is scoped to without
// persisting the manager's global swarm flag. nodes.Service uses it to join a
// docker-host environment in-process against that environment's own daemon.
func (s *SwarmService) joinSwarmInternal(ctx context.Context, req swarmtypes.SwarmJoinRequest) error {
	dockerClient, err := s.dockerService.GetClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect to Docker: %w", err)
	}

	if _, swarmJoinErr := dockerClient.SwarmJoin(ctx, client.SwarmJoinOptions{
		ListenAddr:    defaultSwarmListenAddrInternal(req.ListenAddr),
		AdvertiseAddr: req.AdvertiseAddr,
		DataPathAddr:  req.DataPathAddr,
		RemoteAddrs:   req.RemoteAddrs,
		JoinToken:     req.JoinToken,
		Availability:  req.Availability,
	}); swarmJoinErr != nil {
		return fmt.Errorf("failed to join swarm: %w", swarmJoinErr)
	}

	return nil
}

func (s *SwarmService) LeaveSwarm(ctx context.Context, req swarmtypes.SwarmLeaveRequest) error {
	if err := s.ensureSwarmActiveInternal(ctx); err != nil {
		return err
	}

	dockerClient, err := s.dockerService.GetClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect to Docker: %w", err)
	}

	if _, swarmLeaveErr := dockerClient.SwarmLeave(ctx, client.SwarmLeaveOptions{Force: req.Force}); swarmLeaveErr != nil {
		return fmt.Errorf("failed to leave swarm: %w", swarmLeaveErr)
	}

	s.persistSwarmEnabledStateInternal(ctx, false)

	return nil
}

func (s *SwarmService) UnlockSwarm(ctx context.Context, req swarmtypes.SwarmUnlockRequest) error {
	if err := s.ensureSwarmActiveInternal(ctx); err != nil {
		return err
	}

	dockerClient, err := s.dockerService.GetClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect to Docker: %w", err)
	}

	if _, swarmUnlockErr := dockerClient.SwarmUnlock(ctx, client.SwarmUnlockOptions{Key: req.Key}); swarmUnlockErr != nil {
		return fmt.Errorf("failed to unlock swarm: %w", swarmUnlockErr)
	}

	return nil
}

func (s *SwarmService) GetSwarmUnlockKey(ctx context.Context) (*swarmtypes.SwarmUnlockKeyResponse, error) {
	if err := s.ensureSwarmManagerInternal(ctx); err != nil {
		return nil, err
	}

	dockerClient, err := s.dockerService.GetClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Docker: %w", err)
	}

	unlockResult, err := dockerClient.SwarmGetUnlockKey(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get swarm unlock key: %w", err)
	}

	return &swarmtypes.SwarmUnlockKeyResponse{UnlockKey: unlockResult.Key}, nil
}

func (s *SwarmService) GetSwarmJoinTokens(ctx context.Context) (*swarmtypes.SwarmJoinTokensResponse, error) {
	if err := s.ensureSwarmManagerInternal(ctx); err != nil {
		return nil, err
	}

	dockerClient, err := s.dockerService.GetClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Docker: %w", err)
	}

	infoResult, err := dockerClient.SwarmInspect(ctx, client.SwarmInspectOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to inspect swarm: %w", err)
	}

	return &swarmtypes.SwarmJoinTokensResponse{
		Worker:  infoResult.Swarm.JoinTokens.Worker,
		Manager: infoResult.Swarm.JoinTokens.Manager,
	}, nil
}

func (s *SwarmService) RotateSwarmJoinTokens(ctx context.Context, req swarmtypes.SwarmRotateJoinTokensRequest) error {
	if err := s.ensureSwarmManagerInternal(ctx); err != nil {
		return err
	}

	dockerClient, err := s.dockerService.GetClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect to Docker: %w", err)
	}

	infoResult, err := dockerClient.SwarmInspect(ctx, client.SwarmInspectOptions{})
	if err != nil {
		return fmt.Errorf("failed to inspect swarm: %w", err)
	}

	rotateWorker := req.RotateWorkerToken
	rotateManager := req.RotateManagerToken
	if !rotateWorker && !rotateManager {
		rotateWorker = true
		rotateManager = true
	}

	if _, swarmUpdateErr := dockerClient.SwarmUpdate(ctx, client.SwarmUpdateOptions{
		Version:            infoResult.Swarm.Version,
		Spec:               infoResult.Swarm.Spec,
		RotateWorkerToken:  rotateWorker,
		RotateManagerToken: rotateManager,
	}); swarmUpdateErr != nil {
		return fmt.Errorf("failed to rotate swarm join tokens: %w", swarmUpdateErr)
	}

	return nil
}

func (s *SwarmService) UpdateSwarmSpec(ctx context.Context, req swarmtypes.SwarmUpdateRequest) error {
	if err := s.ensureSwarmManagerInternal(ctx); err != nil {
		return err
	}

	dockerClient, err := s.dockerService.GetClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect to Docker: %w", err)
	}

	version := req.Version
	if version == 0 {
		infoResult, swarmInspectErr := dockerClient.SwarmInspect(ctx, client.SwarmInspectOptions{})
		if swarmInspectErr != nil {
			return fmt.Errorf("failed to inspect swarm: %w", swarmInspectErr)
		}
		version = infoResult.Swarm.Version.Index
	}

	spec, err := decodeSwarmSpecInternal(req.Spec)
	if err != nil {
		return err
	}

	if _, swarmUpdateErr := dockerClient.SwarmUpdate(ctx, client.SwarmUpdateOptions{
		Version:                swarm.Version{Index: version},
		Spec:                   spec,
		RotateWorkerToken:      req.RotateWorkerToken,
		RotateManagerToken:     req.RotateManagerToken,
		RotateManagerUnlockKey: req.RotateManagerUnlockKey,
	}); swarmUpdateErr != nil {
		return fmt.Errorf("failed to update swarm spec: %w", swarmUpdateErr)
	}

	return nil
}

func (s *SwarmService) listTasksPaginatedWithFiltersInternal(ctx context.Context, filters client.Filters, params pagination.QueryParams) ([]swarmtypes.TaskSummary, pagination.Response, error) {
	dockerClient, err := s.dockerService.GetClient(ctx)
	if err != nil {
		return nil, pagination.Response{}, fmt.Errorf("failed to connect to Docker: %w", err)
	}

	servicesResult, err := dockerClient.ServiceList(ctx, client.ServiceListOptions{})
	if err != nil {
		return nil, pagination.Response{}, fmt.Errorf("failed to list swarm services: %w", err)
	}

	serviceNameByID := make(map[string]string, len(servicesResult.Items))
	for _, service := range servicesResult.Items {
		serviceNameByID[service.ID] = service.Spec.Name
	}

	nodesResult, err := dockerClient.NodeList(ctx, client.NodeListOptions{})
	if err != nil {
		return nil, pagination.Response{}, fmt.Errorf("failed to list swarm nodes: %w", err)
	}

	nodeNameByID := make(map[string]string, len(nodesResult.Items))
	for _, node := range nodesResult.Items {
		nodeNameByID[node.ID] = node.Description.Hostname
	}

	if filters == nil {
		filters = make(client.Filters)
	}
	tasksResult, err := dockerClient.TaskList(ctx, client.TaskListOptions{Filters: filters})
	if err != nil {
		return nil, pagination.Response{}, fmt.Errorf("failed to list swarm tasks: %w", err)
	}

	items := make([]swarmtypes.TaskSummary, 0, len(tasksResult.Items))
	for _, task := range tasksResult.Items {
		items = append(items, swarmtypes.NewTaskSummary(task, serviceNameByID[task.ServiceID], nodeNameByID[task.NodeID]))
	}

	config := s.buildTaskPaginationConfigInternal()
	result := config.SearchOrderAndPaginate(items, params)
	paginationResp := pagination.BuildResponse(result.TotalCount, result.TotalAvailable, params)
	return result.Items, paginationResp, nil
}

func decodeSwarmSpecInternal(raw stdjson.RawMessage) (swarm.Spec, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return swarm.Spec{}, errors.New("swarm spec is required")
	}

	var spec swarm.Spec
	if err := json.Unmarshal(trimmed, &spec); err != nil {
		return swarm.Spec{}, fmt.Errorf("failed to parse swarm spec: %w", err)
	}

	if spec.Labels == nil {
		spec.Labels = map[string]string{}
	}

	return spec, nil
}

func defaultSwarmListenAddrInternal(listenAddr string) string {
	trimmed := strings.TrimSpace(listenAddr)
	return kit.Ternary(trimmed == "", defaultSwarmListenAddr, trimmed)
}

func (s *SwarmService) ensureSwarmManagerInternal(ctx context.Context) error {
	info, err := s.getDockerInfoInternal(ctx)
	if err != nil {
		return err
	}

	if info.Swarm.LocalNodeState != swarm.LocalNodeStateActive {
		return common.Classify(common.ErrSwarmNotEnabled, errors.New("Swarm mode is not enabled")) //nolint:staticcheck // Preserve the existing error message.
	}
	if !info.Swarm.ControlAvailable {
		return common.Classify(common.ErrSwarmManagerRequired, errors.New("Swarm manager access required")) //nolint:staticcheck // Preserve the existing error message.
	}

	return nil
}

func (s *SwarmService) ensureSwarmActiveInternal(ctx context.Context) error {
	info, err := s.getDockerInfoInternal(ctx)
	if err != nil {
		return err
	}

	if info.Swarm.LocalNodeState != swarm.LocalNodeStateActive {
		return common.Classify(common.ErrSwarmNotEnabled, errors.New("Swarm mode is not enabled")) //nolint:staticcheck // Preserve the existing error message.
	}

	return nil
}

func (s *SwarmService) getDockerInfoInternal(ctx context.Context) (system.Info, error) {
	dockerClient, err := s.dockerService.GetClient(ctx)
	if err != nil {
		return system.Info{}, fmt.Errorf("failed to connect to Docker: %w", err)
	}

	infoResult, err := dockerClient.Info(ctx, client.InfoOptions{})
	if err != nil {
		return system.Info{}, fmt.Errorf("failed to get Docker info: %w", err)
	}

	return infoResult.Info, nil
}

func (s *SwarmService) SyncSwarmEnabledState(ctx context.Context) error {
	info, err := s.getDockerInfoInternal(ctx)
	if err != nil {
		return err
	}

	enabled := info.Swarm.LocalNodeState == swarm.LocalNodeStateActive && strings.TrimSpace(info.Swarm.NodeID) != ""
	if s.kvService == nil {
		return nil
	}

	if setBoolErr := s.kvService.SetBool(ctx, KVKeySwarmEnabled, enabled); setBoolErr != nil {
		return fmt.Errorf("persist swarm enabled state: %w", setBoolErr)
	}

	return nil
}

// persistSwarmEnabledStateInternal records the manager's own swarm flag.
//
// A docker-host environment is served in-process by this manager, but its swarm
// lifecycle applies to that environment's daemon, so it must not move the
// manager's global flag. The flag is only written for the manager's own
// environment (and for callers that carry no environment target, such as the
// startup sync).
func (s *SwarmService) persistSwarmEnabledStateInternal(ctx context.Context, enabled bool) {
	if s == nil || s.kvService == nil {
		return
	}
	if s.servesNonLocalEnvironmentInternal(ctx) {
		return
	}

	if err := s.kvService.SetBool(ctx, KVKeySwarmEnabled, enabled); err != nil {
		slog.WarnContext(ctx, "Failed to persist swarm enabled state", "enabled", enabled, "error", err)
	}
}

// servesNonLocalEnvironmentInternal reports whether ctx is scoped to an
// environment other than the manager's own. Agent environments are proxied and
// never reach this service, so a non-local request is a docker-host environment
// served in-process; its daemon state must not be written to the manager flag.
func (s *SwarmService) servesNonLocalEnvironmentInternal(ctx context.Context) bool {
	environmentID := envcontext.EnvironmentIDFromContext(ctx)
	if environmentID == "" || environmentID == environment.LocalEnvironmentID {
		return false
	}
	if s == nil || s.environmentService == nil {
		return true
	}
	_, inProcess := s.environmentService.InProcessTargetContext(ctx, environmentID)
	return inProcess
}

func (s *SwarmService) buildTaskPaginationConfigInternal() pagination.Config[swarmtypes.TaskSummary] {
	return pagination.Config[swarmtypes.TaskSummary]{
		SearchAccessors: []pagination.SearchAccessor[swarmtypes.TaskSummary]{
			func(task swarmtypes.TaskSummary) (string, error) { return task.Name, nil },
			func(task swarmtypes.TaskSummary) (string, error) { return task.ServiceName, nil },
			func(task swarmtypes.TaskSummary) (string, error) { return task.NodeName, nil },
			func(task swarmtypes.TaskSummary) (string, error) { return task.ID, nil },
			func(task swarmtypes.TaskSummary) (string, error) { return task.CurrentState, nil },
		},
		SortBindings: []pagination.SortBinding[swarmtypes.TaskSummary]{
			{Key: "service", Fn: func(a, b swarmtypes.TaskSummary) int { return strings.Compare(a.ServiceName, b.ServiceName) }},
			{Key: "node", Fn: func(a, b swarmtypes.TaskSummary) int { return strings.Compare(a.NodeName, b.NodeName) }},
			{Key: "state", Fn: func(a, b swarmtypes.TaskSummary) int { return strings.Compare(a.CurrentState, b.CurrentState) }},
			{Key: "created", Fn: func(a, b swarmtypes.TaskSummary) int { return a.CreatedAt.Compare(b.CreatedAt) }},
			{Key: "updated", Fn: func(a, b swarmtypes.TaskSummary) int { return a.UpdatedAt.Compare(b.UpdatedAt) }},
		},
	}
}

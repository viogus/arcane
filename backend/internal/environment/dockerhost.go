package environment

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/getarcaneapp/arcane/backend/v2/internal/common"
)

// badRequestError reports invalid environment target configuration as an HTTP 400 instead of a 500.
type badRequestError struct{ msg string }

func (e *badRequestError) Error() string { return e.msg }

// Unwrap maps the error onto the shared bad-request sentinel so handlers translate it to HTTP 400.
func (e *badRequestError) Unwrap() error { return common.ErrBadRequest }

func invalidTargetError(format string, args ...any) error {
	return &badRequestError{msg: fmt.Sprintf(format, args...)}
}

// dockerHostSchemes are the Docker API endpoint schemes the manager can talk to directly.
// ssh:// is deliberately absent: moby's ssh transport shells out to a local `ssh` binary,
// which the manager image does not ship (and it needs a home directory for keys), so
// accepting it would only defer the failure to request time.
var dockerHostSchemes = map[string]struct{}{
	"tcp":   {},
	"http":  {},
	"https": {},
	"unix":  {},
	"npipe": {},
}

// NormalizeDockerHost validates a Docker API endpoint for a docker-host environment.
// An empty value clears the docker-host binding and returns "".
func NormalizeDockerHost(value string) (string, error) {
	host := strings.TrimSpace(value)
	if host == "" {
		return "", nil
	}

	parsed, err := url.Parse(host)
	if err != nil {
		return "", invalidTargetError("invalid Docker host %q: %v", host, err)
	}
	if _, ok := dockerHostSchemes[parsed.Scheme]; !ok {
		if parsed.Scheme == "ssh" {
			return "", invalidTargetError("Docker host %q uses ssh, which the manager cannot tunnel by itself; forward the port and use tcp:// instead", host)
		}
		return "", invalidTargetError("unsupported Docker host scheme %q (use tcp://, http://, https://, unix:// or npipe://)", parsed.Scheme)
	}
	if parsed.User != nil {
		return "", invalidTargetError("Docker host %q must not embed credentials; the manager does not forward them", host)
	}
	if parsed.Scheme == "unix" || parsed.Scheme == "npipe" {
		if parsed.Path == "" {
			return "", invalidTargetError("Docker host %q must include a socket path", host)
		}
		if parsed.Host != "" || !strings.HasPrefix(parsed.Path, "/") {
			return "", invalidTargetError("Docker host %q must use an absolute socket path, for example unix:///var/run/docker.sock", host)
		}
		return host, nil
	}
	if parsed.Host == "" {
		return "", invalidTargetError("Docker host %q must include a host and port", host)
	}
	if parsed.Port() == "" {
		return "", invalidTargetError("Docker host %q must include a port, for example tcp://192.168.2.33:2375", host)
	}
	return host, nil
}

// validateEnvironmentTargetInternal rejects ambiguous or malformed environment connection settings.
// An environment points at either an agent API URL or a Docker API endpoint, never both.
func validateEnvironmentTargetInternal(env *Environment) error {
	host := env.DockerHostURL()
	if host == "" {
		if strings.TrimSpace(env.ApiUrl) == "" {
			return invalidTargetError("either an API URL or a Docker host is required")
		}
		return nil
	}

	normalized, err := NormalizeDockerHost(host)
	if err != nil {
		return err
	}
	env.DockerHost = &normalized

	if strings.TrimSpace(env.ApiUrl) != "" {
		return invalidTargetError("a Docker host environment cannot also have an API URL")
	}
	if env.AccessToken != nil && strings.TrimSpace(*env.AccessToken) != "" {
		return invalidTargetError("a Docker host environment cannot use an agent access token")
	}
	env.IsEdge = false
	return nil
}

// applyDockerHostUpdateInternal normalizes a docker_host entry in an update map and keeps the
// agent binding consistent with it. Switching an environment to a docker host drops the agent
// URL/token binding, since the manager now talks to the daemon directly and no agent is involved;
// an environment that keeps its docker host must not gain an agent URL or token.
func applyDockerHostUpdateInternal(updates map[string]any, current *Environment) error {
	raw, hasDockerHost := updates["docker_host"]
	currentHost := current.DockerHostURL()

	if !hasDockerHost {
		if currentHost == "" {
			// The environment stays agent-backed: the update must not blank the API URL, which
			// would leave it without any target at all.
			if apiURL, isString := updates["api_url"].(string); isString && strings.TrimSpace(apiURL) == "" {
				return invalidTargetError("either an API URL or a Docker host is required")
			}
			return nil
		}
		if apiURL, isString := updates["api_url"].(string); isString && strings.TrimSpace(apiURL) != "" {
			return invalidTargetError("a Docker host environment cannot also have an API URL")
		}
		if _, ok := updates["api_url"]; !ok && strings.TrimSpace(current.ApiUrl) != "" {
			return invalidTargetError("a Docker host environment cannot also have an API URL")
		}
		if token, isString := updates["access_token"].(string); isString && strings.TrimSpace(token) != "" {
			return invalidTargetError("a Docker host environment cannot use an agent access token")
		}
		if _, ok := updates["access_token"]; !ok && current.AccessToken != nil && strings.TrimSpace(*current.AccessToken) != "" {
			return invalidTargetError("a Docker host environment cannot use an agent access token")
		}
		// A docker-host environment is never an edge environment; keep the flag from
		// contradicting the target when it is sent on its own.
		if edge, isBool := updates["is_edge"].(bool); isBool && edge {
			updates["is_edge"] = false
		}
		return nil
	}

	host, _ := raw.(string)
	normalized, err := NormalizeDockerHost(host)
	if err != nil {
		return err
	}

	// The API URL this update leaves behind, used to reject target-less environments.
	nextAPIURL := current.ApiUrl
	if rawAPIURL, ok := updates["api_url"]; ok {
		nextAPIURL, _ = rawAPIURL.(string)
	}

	if normalized != "" {
		// Pointing at a Docker host replaces the agent binding, so only an api_url carried
		// by the same update conflicts with it.
		if rawAPIURL, ok := updates["api_url"]; ok {
			if apiURL, isString := rawAPIURL.(string); isString && strings.TrimSpace(apiURL) != "" {
				return invalidTargetError("a Docker host environment cannot also have an API URL")
			}
		}
	} else if strings.TrimSpace(nextAPIURL) == "" {
		return invalidTargetError("either an API URL or a Docker host is required")
	}

	if token, isString := updates["access_token"].(string); isString && strings.TrimSpace(token) != "" {
		return invalidTargetError("a Docker host environment cannot use an agent access token")
	}

	if normalized == "" {
		// Clearing the docker host hands the environment back to the agent flow.
		updates["docker_host"] = nil
		return nil
	}

	updates["docker_host"] = normalized
	// The environment now talks straight to the daemon, so any previous agent binding is dropped.
	updates["api_url"] = ""
	updates["access_token"] = nil
	updates["is_edge"] = false
	return nil
}

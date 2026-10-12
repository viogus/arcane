package environment

import (
	"strings"
	"time"

	"github.com/getarcaneapp/arcane/backend/v2/internal/database"
)

type Environment struct {
	database.BaseModel

	Name                string     `json:"name" sortable:"true" unorm:"nfc" trim:"true"`
	ApiUrl              string     `json:"apiUrl" gorm:"column:api_url" sortable:"true"`
	Status              string     `json:"status" sortable:"true"`
	Enabled             bool       `json:"enabled" sortable:"true"`
	IsEdge              bool       `json:"isEdge" gorm:"column:is_edge;default:false"`
	Hidden              bool       `json:"hidden" gorm:"column:hidden;default:false"`
	LastSeen            *time.Time `json:"lastSeen" gorm:"column:last_seen"`
	LastEdgeTransport   *string    `json:"lastEdgeTransport" gorm:"column:last_edge_transport"`
	DockerHost          *string    `json:"dockerHost" gorm:"column:docker_host" sortable:"true"`
	AccessToken         *string    `json:"-" gorm:"column:access_token"`
	ApiKeyID            *string    `json:"-" gorm:"column:api_key_id"`
	ParentEnvironmentID *string    `json:"-" gorm:"column:parent_environment_id"`
	SwarmNodeID         *string    `json:"-" gorm:"column:swarm_node_id"`
}

func (Environment) TableName() string { return "environments" }

// DockerHostURL returns the configured Docker API endpoint for a "docker-host"
// environment, or an empty string for agent-backed environments.
func (e Environment) DockerHostURL() string {
	if e.DockerHost == nil {
		return ""
	}
	return strings.TrimSpace(*e.DockerHost)
}

// IsDockerHost reports whether the manager talks to this environment's Docker
// API directly instead of proxying to an Arcane agent.
func (e Environment) IsDockerHost() bool {
	return e.DockerHostURL() != ""
}

type EnvironmentStatus string

const (
	EnvironmentStatusOnline  EnvironmentStatus = "online"
	EnvironmentStatusStandby EnvironmentStatus = "standby"
	EnvironmentStatusOffline EnvironmentStatus = "offline"
	EnvironmentStatusError   EnvironmentStatus = "error"
	EnvironmentStatusPending EnvironmentStatus = "pending"
)

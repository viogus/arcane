package rustic

import (
	"errors"
	"os"
	"strings"
)

// EnvDisabled switches the Rustic-backed backup engine off. Every backup and
// recovery entry point returns ErrDisabled instead of inspecting, pulling or
// running DefaultImage, so a host that never backs anything up does not pay for
// the engine: no image pull, no helper container, no repository volume.
const EnvDisabled = "ARCANE_BACKUPS_DISABLED"

// ErrDisabled is returned by every Rustic entry point while Disabled is true.
var ErrDisabled = errors.New("backup engine is disabled: " + EnvDisabled + " is set")

// Disabled reports whether the backup engine is switched off for this process.
func Disabled() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(EnvDisabled))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

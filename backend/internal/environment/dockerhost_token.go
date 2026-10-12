package environment

import (
	"context"
	"log/slog"
	"strings"

	"go.getarcane.app/sys/crypto"
)

// DecryptedDockerHostToken returns the plaintext bearer token this environment presents to its
// "docker-host" daemon, or an empty string when it has none.
//
// A stored value that no longer decrypts (for example after an encryption-key change) is logged and
// treated as absent: the daemon then answers 401, which is a clearer signal than failing every
// manager-side request with a decryption error.
func (e Environment) DecryptedDockerHostToken(ctx context.Context) string {
	if !e.HasDockerHostToken() {
		return ""
	}
	token, err := crypto.Decrypt(*e.DockerHostToken)
	if err != nil {
		slog.WarnContext(ctx, "Failed to decrypt docker-host token", "environmentId", e.ID, "error", err)
		return ""
	}
	return strings.TrimSpace(token)
}

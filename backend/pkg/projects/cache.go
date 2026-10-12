package projects

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	composetypes "github.com/compose-spec/compose-go/v2/types"
	projecttypes "github.com/getarcaneapp/arcane/types/v2/project"
	"github.com/samber/hot"
)

// composeCacheVariantsInternal bounds how many fingerprints one project keeps.
// A single manager serves every environment, and each environment loads the same
// project with its own paths and mapper, so keying a project by ID alone made the
// environments evict each other and re-parse on every request.
const composeCacheVariantsInternal = 8

type composeCacheInternal[T any] struct {
	entries *hot.HotCache[string, []composeCacheEntryInternal[T]]
	clone   func(T) T
}

type composeCacheEntryInternal[T any] struct {
	fingerprint string
	mtimes      map[string]time.Time
	value       T
}

// NewComposeCache caches Compose-derived values and their file dependencies.
// clone, when supplied, isolates stored values from mutations by callers.
func NewComposeCache[T any](capacity int, clone func(T) T) projecttypes.ComposeCache[T] {
	return &composeCacheInternal[T]{entries: hot.NewHotCache[string, []composeCacheEntryInternal[T]](hot.LRU, capacity).Build(), clone: clone}
}

// NewParsedComposeCache keeps operation-local models separate from cached state.
func NewParsedComposeCache() projecttypes.ComposeCache[*composetypes.Project] {
	return NewComposeCache(2048, cloneComposeProjectInternal)
}

func cloneComposeProjectInternal(project *composetypes.Project) *composetypes.Project {
	if project == nil {
		return nil
	}
	// With no names this compose-go operation deep-copies without disabling services.
	return project.WithServicesDisabled()
}

func (c *composeCacheInternal[T]) Get(projectID, fingerprint string) (T, bool) {
	variants, found := c.entries.Peek(projectID)
	if found {
		for index := range variants {
			variant := &variants[index]
			if variant.fingerprint != fingerprint || !validComposeCacheEntryInternal(variant.mtimes) {
				continue
			}
			if c.clone != nil {
				return c.clone(variant.value), true
			}
			return variant.value, true
		}
	}
	var zero T
	return zero, false
}

func (c *composeCacheInternal[T]) Set(projectID, fingerprint, projectPath, projectsDirectory, composePath string, composeFiles, envFiles []string, value T) error {
	if strings.TrimSpace(projectID) == "" {
		return nil
	}
	mtimes := make(map[string]time.Time)
	for _, path := range append([]string{composePath}, composeFiles...) {
		if path == "" {
			continue
		}
		info, err := os.Stat(path)
		if err != nil {
			return fmt.Errorf("stat compose file %s: %w", path, err)
		}
		mtimes[path] = info.ModTime()
	}
	dependencies := append([]string{filepath.Join(projectPath, EffectiveEnvFileName)}, envFiles...)
	if projectsDirectory != "" {
		dependencies = append(dependencies, filepath.Join(projectsDirectory, GlobalEnvFileName))
	}
	// A later standard override must invalidate a model loaded before it existed.
	for _, name := range composeOverrideFileCandidates {
		dependencies = append(dependencies, filepath.Join(filepath.Dir(composePath), name))
	}
	for _, path := range dependencies {
		info, err := os.Stat(path)
		if err == nil {
			mtimes[path] = info.ModTime()
			continue
		}
		if !os.IsNotExist(err) {
			return fmt.Errorf("stat compose dependency %s: %w", path, err)
		}
		mtimes[path] = time.Time{}
	}
	if c.clone != nil {
		value = c.clone(value)
	}
	entry := composeCacheEntryInternal[T]{fingerprint: fingerprint, mtimes: mtimes, value: value}
	previous, _ := c.entries.Peek(projectID)
	variants := make([]composeCacheEntryInternal[T], 0, len(previous)+1)
	variants = append(variants, entry)
	for _, variant := range previous {
		if variant.fingerprint == fingerprint {
			continue
		}
		if len(variants) == composeCacheVariantsInternal {
			break
		}
		variants = append(variants, variant)
	}
	c.entries.Set(projectID, variants)
	return nil
}

func (c *composeCacheInternal[T]) Invalidate(projectID string) {
	c.entries.Delete(projectID)
}

func validComposeCacheEntryInternal(mtimes map[string]time.Time) bool {
	for path, mtime := range mtimes {
		info, err := os.Stat(path)
		if err != nil {
			if !os.IsNotExist(err) || !mtime.IsZero() {
				return false
			}
			continue
		}
		if mtime.IsZero() || !info.ModTime().Equal(mtime) {
			return false
		}
		// A chmod does not bump mtime, so confirm the file is still readable.
		file, err := os.Open(path)
		if err != nil {
			return false
		}
		if closeErr := file.Close(); closeErr != nil {
			return false
		}
	}
	return true
}

// LoadCachedComposeProject resolves an executable model with isolated cached state.
func LoadCachedComposeProject(ctx context.Context,
	cache projecttypes.ComposeCache[*composetypes.Project],
	projectID,
	projectPath,
	composePath,
	projectName,
	projectsDirectory string,
	autoInject bool,
	pathMapper *PathMapper) (*composetypes.Project,
	error,
) {
	fingerprint := fmt.Sprintf("%q|%q|%q|%t|%#v", composePath, projectName, projectsDirectory, autoInject, pathMapper)
	if cache != nil {
		if cached, ok := cache.Get(projectID, fingerprint); ok {
			return cached, nil
		}
	}
	dependencies := projecttypes.ComposeDependencies{}
	model, err := LoadComposeProject(ctx, composePath, projectName, projectsDirectory, autoInject, pathMapper, nil, nil, false, &dependencies, nil, nil)
	if err != nil {
		return nil, err
	}
	if cache == nil {
		return model, nil
	}
	composeFiles, envFiles, ok := ComposeCacheDependencies(ctx, model, dependencies, composePath, projectsDirectory, autoInject)
	if !ok {
		// Metadata discovery failure disables caching; the executable model is valid.
		return model, nil
	}
	if setErr := cache.Set(projectID, fingerprint, projectPath, projectsDirectory, composePath, composeFiles, envFiles, model); setErr != nil {
		return nil, setErr
	}
	return model, nil
}

// ComposeCacheDependencies returns the compose and env files a model loaded
// from composePath depends on. ok is false when they cannot be discovered.
func ComposeCacheDependencies(
	ctx context.Context,
	model *composetypes.Project,
	dependencies projecttypes.ComposeDependencies,
	composePath, projectsDirectory string,
	autoInject bool,
) ([]string, []string, bool) {
	envFiles := dependencies.EnvFiles
	if envOpts, parseErr := ParseComposeEnvOptions(model.WorkingDir, EnvMap(model.Environment)); parseErr == nil {
		envFiles = append(envFiles, envOpts.EnvFiles...)
	}
	// Metadata discovery already follows recursive includes and their interpolation env files.
	meta, err := ParseArcaneComposeMetadata(ctx, composePath, projectsDirectory, autoInject)
	if err != nil {
		return nil, nil, false
	}
	composeFiles := append(append([]string{}, model.ComposeFiles...), meta.ComposeFiles...)
	return composeFiles, append(envFiles, meta.EnvFiles...), true
}

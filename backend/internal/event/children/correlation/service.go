package correlation

import (
	"slices"
	"strings"
	"sync"
	"time"

	"go.getarcane.app/kit/pkg"
)

const (
	expectationTTL         = 60 * time.Second
	suppressionWindowGrace = 10 * time.Second
)

// Correlation is keyed by environment as well as resource: two daemons can each have a container,
// image or volume with the same name, and a mutation on one must not hide the other's activity.
type expectation struct {
	environmentID                          string
	resourceType, resourceID, resourceName string
}

type windowKey struct {
	environmentID string
	kind, name    string
	resource      expectation
}

type window struct {
	active  int
	expires time.Time
}

// Service correlates Docker daemon observations with local Arcane actions.
type Service struct {
	mu                 sync.Mutex
	expectations       map[expectation]time.Time
	windows            map[windowKey]window
	now                func() time.Time
	updatingContainers func() []string
}

func NewService(now func() time.Time) *Service {
	return &Service{now: now}
}

func (c *Service) timeInternal() time.Time {
	if c.now != nil {
		return c.now()
	}
	return time.Now()
}

func (c *Service) pruneInternal(now time.Time) {
	for key, expiry := range c.expectations {
		if !now.Before(expiry) {
			delete(c.expectations, key)
		}
	}
	for key, w := range c.windows {
		if w.active == 0 && !now.Before(w.expires) {
			delete(c.windows, key)
		}
	}
}

// Prune drops expired expectations and closed windows past their grace period.
func (c *Service) Prune() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pruneInternal(c.timeInternal())
}

// MarkExpectation correlates subsequent daemon observations with a local Arcane action. environmentID
// names the daemon the action ran against; a daemon event is only correlated with its own environment.
func (c *Service) MarkExpectation(environmentID, resourceType, resourceID, resourceName string) {
	switch resourceType {
	case "container", "image", "volume", "network":
	default:
		return
	}
	if resourceName == "name" {
		resourceName = ""
	}
	if resourceID == "" && resourceName == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.timeInternal()
	c.pruneInternal(now)
	if c.expectations == nil {
		c.expectations = make(map[expectation]time.Time)
	}
	c.expectations[expectation{environmentID, resourceType, resourceID, resourceName}] = now.Add(expectationTTL)
}

// BeginComposeWindow covers daemon events labeled with the Compose project.
// Unlabeled resources require an explicit resource expectation to avoid hiding unrelated activity.
func (c *Service) BeginComposeWindow(environmentID, composeProject string) func() {
	if composeProject == "" {
		return func() {}
	}
	return c.beginWindowsInternal([]windowKey{{environmentID: environmentID, kind: "compose", name: composeProject}})
}

// BeginResourceWindow covers a mutation for its full duration and cleanup grace.
func (c *Service) BeginResourceWindow(environmentID, resourceType, resourceID, resourceName string) func() {
	if resourceName == "name" {
		resourceName = ""
	}
	if resourceID == "" && resourceName == "" {
		return func() {}
	}
	return c.beginWindowsInternal([]windowKey{{
		environmentID: environmentID,
		kind:          "resource",
		resource:      expectation{environmentID, resourceType, resourceID, resourceName},
	}})
}

// BeginTypeWindow covers bulk daemon operations whose per-item IDs are unknown until they finish.
func (c *Service) BeginTypeWindow(environmentID, resourceType string) func() {
	if resourceType == "" {
		return func() {}
	}
	return c.beginWindowsInternal([]windowKey{{environmentID: environmentID, kind: "type", name: resourceType}})
}

func (c *Service) beginWindowsInternal(keys []windowKey) func() {
	c.mu.Lock()
	c.pruneInternal(c.timeInternal())
	if c.windows == nil {
		c.windows = make(map[windowKey]window)
	}
	for _, key := range keys {
		w := c.windows[key]
		w.active++
		c.windows[key] = w
	}
	c.mu.Unlock()
	return sync.OnceFunc(func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		for _, key := range keys {
			w := c.windows[key]
			w.active--
			w.expires = c.timeInternal().Add(suppressionWindowGrace)
			c.windows[key] = w
		}
	})
}

// SetUpdatingContainers supplies the updater's live identities without coupling domains.
func (c *Service) SetUpdatingContainers(source func() []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.updatingContainers = source
}

// ShouldSuppress reports whether a daemon observation matches a recent local action against the same
// environment's daemon.
func (c *Service) ShouldSuppress(environmentID, resourceType, actorID, actorName, composeProject string) bool {
	c.mu.Lock()
	updating := c.updatingContainers
	c.mu.Unlock()
	// The updater records stop after Docker returns, so active updates also need correlation.
	if resourceType == "container" && updating != nil {
		for _, id := range updating() {
			if id != "" && (id == actorID || idMatchesInternal(resourceType, id, actorID)) {
				return true
			}
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pruneInternal(c.timeInternal())

	for exp := range c.expectations {
		if exp.environmentID == environmentID && exp.matchesInternal(resourceType, actorID, actorName) {
			return true
		}
	}
	for key := range c.windows {
		if key.environmentID != environmentID {
			continue
		}
		switch key.kind {
		case "resource":
			if key.resource.matchesInternal(resourceType, actorID, actorName) {
				return true
			}
		case "compose":
			if composeProject == key.name {
				return true
			}
		case "type":
			if key.name == resourceType {
				return true
			}
		}
	}

	return false
}

func (exp expectation) matchesInternal(resourceType, actorID, actorName string) bool {
	if exp.resourceType != resourceType {
		return false
	}
	actors := kit.TrimNonEmpty([]string{actorID, actorName})
	return slices.Contains(actors, exp.resourceID) ||
		slices.Contains(actors, exp.resourceName) ||
		idMatchesInternal(resourceType, exp.resourceID, actorID)
}

func idMatchesInternal(resourceType, expected, actual string) bool {
	if resourceType == "volume" {
		return false
	}
	if resourceType == "image" {
		expected = strings.TrimPrefix(expected, "sha256:")
		actual = strings.TrimPrefix(actual, "sha256:")
	}
	if len(expected) < 12 || len(expected) > 64 || len(actual) != 64 {
		return false
	}
	for _, id := range []string{expected, actual} {
		for _, ch := range id {
			if ch < '0' || ch > '9' && ch < 'a' || ch > 'f' {
				return false
			}
		}
	}
	return strings.HasPrefix(actual, expected)
}

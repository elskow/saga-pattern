package faultinjection

import (
	"strings"
	"sync"
	"time"
)

type Config struct {
	Enabled    bool   `json:"enabled"`
	RunLabel   string `json:"runLabel,omitempty"`
	FailNext   int    `json:"failNext,omitempty"`
	TTLSeconds int    `json:"ttlSeconds,omitempty"`
}

type Snapshot struct {
	Enabled   bool       `json:"enabled"`
	RunLabel  string     `json:"runLabel,omitempty"`
	Remaining int        `json:"remaining"`
	ExpiresAt *time.Time `json:"expiresAt,omitempty"`
}

type Controller struct {
	mu        sync.Mutex
	enabled   bool
	runLabel  string
	remaining int
	expiresAt *time.Time
}

func (c *Controller) Configure(config Config, now time.Time) Snapshot {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !config.Enabled {
		c.enabled = false
		c.runLabel = ""
		c.remaining = 0
		c.expiresAt = nil
		return c.snapshotLocked(now.UTC())
	}

	c.enabled = true
	c.runLabel = strings.TrimSpace(config.RunLabel)
	c.remaining = 1
	if config.FailNext > 0 {
		c.remaining = config.FailNext
	}
	c.expiresAt = nil
	if config.TTLSeconds > 0 {
		expiresAt := now.UTC().Add(time.Duration(config.TTLSeconds) * time.Second)
		c.expiresAt = &expiresAt
	}
	return c.snapshotLocked(now.UTC())
}

func (c *Controller) SetEnabled(enabled bool, now time.Time) Snapshot {
	return c.Configure(Config{Enabled: enabled}, now)
}

func (c *Controller) Snapshot(now time.Time) Snapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.snapshotLocked(now.UTC())
}

func (c *Controller) ShouldFail(now time.Time) (bool, Snapshot) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now = now.UTC()
	c.applyExpiryLocked(now)
	if !c.enabled {
		return false, c.snapshotLocked(now)
	}
	if c.remaining == 0 {
		c.enabled = false
		return false, c.snapshotLocked(now)
	}
	if c.remaining > 0 {
		c.remaining--
		if c.remaining == 0 {
			defer func() { c.enabled = false }()
		}
	}
	return true, c.snapshotLocked(now)
}

func (c *Controller) snapshotLocked(now time.Time) Snapshot {
	c.applyExpiryLocked(now)
	var expiresAt *time.Time
	if c.expiresAt != nil {
		copy := *c.expiresAt
		expiresAt = &copy
	}
	return Snapshot{Enabled: c.enabled, RunLabel: c.runLabel, Remaining: c.remaining, ExpiresAt: expiresAt}
}

func (c *Controller) applyExpiryLocked(now time.Time) {
	if c.expiresAt != nil && !now.Before(*c.expiresAt) {
		c.enabled = false
		c.remaining = 0
		c.expiresAt = nil
	}
}

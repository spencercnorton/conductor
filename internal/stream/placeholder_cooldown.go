package stream

import (
	"sync"
	"time"
)

// placeholderCooldownTTL is how long a black-placeholder verdict for one
// upstream URL is reused before the provider is asked again.
//
// A provider answers an off-air channel with a finite all-black file, and
// nothing about that answer changes on a scale of seconds. Without memory,
// every retry, every re-tune and every other viewer of that channel re-sent
// the panel request, followed the redirect, downloaded the whole file and
// ran the classifier again: one channel left on black cost 10-20 provider
// requests a minute. Providers rate-limit per client IP, so that noise
// competes with every real stream.
//
// Deliberately simple: one fixed window, shared per process. Ceiling: at most one
// provider request per black URL per window, and a channel that comes back
// on air is noticed up to one window late (a DVR can lose up to a minute of
// an event's first minutes). Grow it exponentially only if one request a minute per
// URL is still too much.
const placeholderCooldownTTL = time.Minute

// placeholderCooldown remembers upstream URLs whose last answer was the
// provider's black placeholder. A nil *placeholderCooldown remembers nothing,
// so streamers built outside a Pool keep asking the provider every time.
type placeholderCooldown struct {
	mu    sync.Mutex
	until map[string]time.Time
}

func newPlaceholderCooldown() *placeholderCooldown {
	return &placeholderCooldown{until: make(map[string]time.Time)}
}

// active reports whether rawURL was classified as the black placeholder
// within the last placeholderCooldownTTL.
func (c *placeholderCooldown) active(rawURL string, now time.Time) bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return now.Before(c.until[rawURL])
}

// mark records a fresh black-placeholder verdict for rawURL and drops expired
// entries, so the map stays bounded by the URLs that are black right now.
func (c *placeholderCooldown) mark(rawURL string, now time.Time) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for u, t := range c.until {
		if !now.Before(t) {
			delete(c.until, u)
		}
	}
	c.until[rawURL] = now.Add(placeholderCooldownTTL)
}

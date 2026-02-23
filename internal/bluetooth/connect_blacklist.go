package bluetooth

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// ConnectBlacklist is a simple, low-overhead filter to skip connection attempts
// for devices whose name contains one of the configured keywords.
//
// Matching rules:
// - Case-insensitive
// - Substring match (so "ResMed" matches "ResMed 027506" and "ResMed027506")
// - One keyword per line, empty lines ignored
// - Lines starting with #, ; or // are treated as comments
//
// The blacklist can be edited while the program is running; it is reloaded
// periodically (best-effort).
type ConnectBlacklist struct {
	path string

	mu       sync.RWMutex
	keywords []string // lowercased
	modTime  time.Time

	// To avoid stat() on every loop tick.
	lastStat  time.Time
	statEvery time.Duration
}

// LoadConnectBlacklist loads blacklist keywords from a file.
// If the file does not exist, (nil, nil) is returned.
func LoadConnectBlacklist(path string) (*ConnectBlacklist, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, nil
	}
	st, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	kw, err := readBlacklistKeywords(path)
	if err != nil {
		return nil, err
	}

	b := &ConnectBlacklist{
		path:      path,
		keywords:  kw,
		modTime:   st.ModTime(),
		lastStat:  time.Now(),
		statEvery: 30 * time.Second,
	}
	return b, nil
}

func (b *ConnectBlacklist) Path() string {
	if b == nil {
		return ""
	}
	return b.path
}

func (b *ConnectBlacklist) Keywords() []string {
	if b == nil {
		return nil
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make([]string, 0, len(b.keywords))
	out = append(out, b.keywords...)
	return out
}

// Match returns true if the given device name should be skipped for connection.
func (b *ConnectBlacklist) Match(deviceName string) bool {
	if b == nil {
		return false
	}
	name := strings.ToLower(strings.TrimSpace(deviceName))
	if name == "" {
		return false
	}

	b.mu.RLock()
	defer b.mu.RUnlock()
	for _, kw := range b.keywords {
		if kw == "" {
			continue
		}
		if strings.Contains(name, kw) {
			return true
		}
	}
	return false
}

// MaybeReload reloads the file if it has changed. Best-effort; errors are ignored.
func (b *ConnectBlacklist) MaybeReload() {
	if b == nil {
		return
	}
	now := time.Now()
	b.mu.RLock()
	last := b.lastStat
	interval := b.statEvery
	path := b.path
	b.mu.RUnlock()

	if !last.IsZero() && now.Sub(last) < interval {
		return
	}

	st, err := os.Stat(path)
	if err != nil {
		return
	}

	b.mu.Lock()
	b.lastStat = now
	prevMod := b.modTime
	b.mu.Unlock()

	if st.ModTime().Equal(prevMod) {
		return
	}

	kw, err := readBlacklistKeywords(path)
	if err != nil {
		return
	}

	b.mu.Lock()
	b.keywords = kw
	b.modTime = st.ModTime()
	b.lastStat = now
	b.mu.Unlock()
}

func readBlacklistKeywords(path string) ([]string, error) {
	f, err := os.Open(filepath.Clean(path))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	seen := map[string]bool{}
	out := make([]string, 0, 32)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") || strings.HasPrefix(line, "//") {
			continue
		}
		kw := strings.ToLower(strings.TrimSpace(line))
		if kw == "" {
			continue
		}
		if seen[kw] {
			continue
		}
		seen[kw] = true
		out = append(out, kw)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

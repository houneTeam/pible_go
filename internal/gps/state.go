package gps

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"pible/internal/util"
)

type Config struct {
	// Mode: auto|gpsd|serial
	Mode string

	// GPSDAddr: host:port, e.g. 127.0.0.1:2947
	GPSDAddr string

	// SerialDev: e.g. /dev/ttyUSB0
	SerialDev string
	// SerialBaud: typical 9600
	SerialBaud int
}

type State struct {
	mu sync.RWMutex

	useGPS bool
	status string

	latestLat  float64
	latestLon  float64
	lastFix    time.Time
	lastPacket time.Time

	received        bool
	scanningStarted bool

	timeout time.Duration

	// activeCloser is set while a reader is running (gpsd or serial).
	// It is used by the watchdog to force a reconnect when packets stop.
	activeCloser func()
	activeKind   string
}

// Source returns the active GPS reader kind: "gpsd", "serial", or "".
func (s *State) Source() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.activeKind
}

// FixSnapshot returns the last known position fix information.
// ok is true when at least one fix has been received.
// cached is true when the fix is older than the configured freshness timeout.
func (s *State) FixSnapshot() (lat float64, lon float64, ok bool, cached bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.useGPS {
		return 0, 0, false, false
	}
	if s.lastFix.IsZero() {
		return 0, 0, false, false
	}
	lat = s.latestLat
	lon = s.latestLon
	ok = true
	cached = time.Since(s.lastFix) > s.timeout
	return lat, lon, ok, cached
}

// Stop forces the active GPS reader (gpsd or serial) to close immediately.
// It is safe to call multiple times.
func (s *State) Stop() {
	s.mu.RLock()
	closer := s.activeCloser
	s.mu.RUnlock()
	if closer != nil {
		closer()
	}
}

func NewState(useGPS bool, timeout time.Duration) *State {
	st := &State{useGPS: useGPS, timeout: timeout}
	if useGPS {
		st.status = "offline"
	} else {
		st.status = "offline"
		st.received = true
	}
	return st
}

func (s *State) SetScanningStarted(v bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.scanningStarted = v
}

func (s *State) WaitForFirstFix(ctx context.Context) error {
	if !s.useGPS {
		return nil
	}
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			s.mu.RLock()
			recv := s.received
			s.mu.RUnlock()
			if recv {
				return nil
			}
		}
	}
}

// WaitForFirstPacket waits until at least one GPS packet has been received.
// It does not require a valid position fix.
func (s *State) WaitForFirstPacket(ctx context.Context, timeout time.Duration) bool {
	if !s.useGPS {
		return true
	}
	deadline := time.Now().Add(timeout)
	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return false
		case <-t.C:
			s.mu.RLock()
			lp := s.lastPacket
			s.mu.RUnlock()
			if !lp.IsZero() {
				return true
			}
			if time.Now().After(deadline) {
				return false
			}
		}
	}
}

func (s *State) HasRecentPacket(maxAge time.Duration) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.useGPS {
		return false
	}
	if s.lastPacket.IsZero() {
		return false
	}
	return time.Since(s.lastPacket) <= maxAge
}

func (s *State) IsFresh() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.useGPS {
		return false
	}
	if s.lastFix.IsZero() {
		return false
	}
	return time.Since(s.lastFix) <= s.timeout
}

func (s *State) GPSStringIfFresh() *string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.useGPS {
		return nil
	}
	if s.lastFix.IsZero() {
		return nil
	}
	if time.Since(s.lastFix) > s.timeout {
		return nil
	}
	v := fmt.Sprintf("%f, %f", s.latestLat, s.latestLon)
	return &v
}

// GPSStringForRecord returns the last known GPS fix.
// If the fix is fresh it returns "lat, lon".
// If the fix is stale it returns "(lat, lon)" to mark that it's cached.
// If no fix has ever been received it returns nil.
func (s *State) GPSStringForRecord() *string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.useGPS {
		return nil
	}
	if s.lastFix.IsZero() {
		return nil
	}
	if time.Since(s.lastFix) <= s.timeout {
		v := fmt.Sprintf("%f, %f", s.latestLat, s.latestLon)
		return &v
	}
	// Cached/last known.
	v := fmt.Sprintf("(%f, %f)", s.latestLat, s.latestLon)
	return &v
}

func (s *State) Status() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.status
}

func (s *State) Start(ctx context.Context, cfg Config) error {
	if !s.useGPS {
		return nil
	}
	cfg = normalizeConfig(cfg)

	go s.updateStatusLoop(ctx)
	go s.watchdogLoop(ctx)

	switch cfg.Mode {
	case "gpsd":
		go s.runGPSDLoop(ctx, cfg.GPSDAddr)
	case "serial":
		if cfg.SerialDev == "" {
			return errors.New("gps serial mode requires a device path (e.g., --gps-device /dev/ttyUSB0)")
		}
		go s.runSerialLoop(ctx, cfg.SerialDev, cfg.SerialBaud)
	case "auto":
		// Prefer gpsd if reachable; otherwise fall back to serial if possible.
		if canConnectGPSD(cfg.GPSDAddr, 800*time.Millisecond) {
			go s.runGPSDLoop(ctx, cfg.GPSDAddr)
			return nil
		}
		if cfg.SerialDev == "" {
			if guessed := GuessSerialDevice(); guessed != "" {
				cfg.SerialDev = guessed
			}
		}
		if cfg.SerialDev == "" {
			return fmt.Errorf("gps auto mode: gpsd not reachable at %s and no serial device detected", cfg.GPSDAddr)
		}
		go s.runSerialLoop(ctx, cfg.SerialDev, cfg.SerialBaud)
	default:
		return fmt.Errorf("invalid gps mode: %q (expected auto|gpsd|serial)", cfg.Mode)
	}

	return nil
}

func normalizeConfig(cfg Config) Config {
	cfg.Mode = strings.ToLower(strings.TrimSpace(cfg.Mode))
	if cfg.Mode == "" {
		cfg.Mode = "auto"
	}
	cfg.GPSDAddr = strings.TrimSpace(cfg.GPSDAddr)
	if cfg.GPSDAddr == "" {
		cfg.GPSDAddr = "127.0.0.1:2947"
	}
	cfg.SerialDev = strings.TrimSpace(cfg.SerialDev)
	if cfg.SerialBaud <= 0 {
		cfg.SerialBaud = 9600
	}
	return cfg
}

func canConnectGPSD(addr string, timeout time.Duration) bool {
	c, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return false
	}
	_ = c.Close()
	return true
}

func (s *State) updateFix(lat, lon float64) {
	s.mu.Lock()
	s.latestLat = lat
	s.latestLon = lon
	s.lastFix = time.Now()
	s.received = true
	s.mu.Unlock()
}

func (s *State) updatePacket() {
	s.mu.Lock()
	s.lastPacket = time.Now()
	s.mu.Unlock()
}

func (s *State) setActiveCloser(kind string, closer func()) {
	s.mu.Lock()
	s.activeKind = kind
	s.activeCloser = closer
	// Treat connection establishment as "we have packets" to avoid immediate watchdog close.
	s.lastPacket = time.Now()
	s.mu.Unlock()
}

func (s *State) clearActiveCloser() {
	s.mu.Lock()
	s.activeKind = ""
	s.activeCloser = nil
	s.mu.Unlock()
}

func (s *State) updateStatusLoop(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	prev := ""
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.mu.Lock()
			if !s.useGPS {
				s.status = "offline"
			} else if !s.lastFix.IsZero() && time.Since(s.lastFix) <= s.timeout {
				s.status = "online"
			} else {
				s.status = "offline"
			}
			cur := s.status
			s.mu.Unlock()

			// Emit transitions once.
			if prev != "" && cur != prev {
				if cur == "online" {
					util.Line("[GPS]", util.ColorGreen, "signal acquired")
					log.Printf("gps: signal acquired")
				} else {
					// Include cached position in the console/log if we have one.
					cached := s.GPSStringForRecord()
					if cached != nil {
						util.Linef("[GPS]", util.ColorYellow, "signal lost (using last known %s)", *cached)
						log.Printf("gps: signal lost (using last known %s)", *cached)
					} else {
						util.Line("[GPS]", util.ColorYellow, "signal lost (no last known fix)")
						log.Printf("gps: signal lost")
					}
				}
			}
			prev = cur
		}
	}
}

// watchdogLoop forces a reconnect when GPS packets stop arriving.
// This is primarily to handle USB hot-unplug or stalled GPSD/serial streams.
func (s *State) watchdogLoop(ctx context.Context) {
	// If no packets for this duration while scanning, force reconnect.
	const noPacketTimeout = 12 * time.Second
	// Don't spam reconnects.
	const minReconnectPeriod = 10 * time.Second

	t := time.NewTicker(1 * time.Second)
	defer t.Stop()

	lastKick := time.Time{}
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}

		s.mu.RLock()
		use := s.useGPS
		scanning := s.scanningStarted
		lp := s.lastPacket
		closer := s.activeCloser
		kind := s.activeKind
		s.mu.RUnlock()

		if !use || !scanning {
			continue
		}
		if closer == nil {
			continue
		}
		if lp.IsZero() {
			continue
		}
		if time.Since(lp) <= noPacketTimeout {
			continue
		}
		if !lastKick.IsZero() && time.Since(lastKick) < minReconnectPeriod {
			continue
		}
		lastKick = time.Now()
		util.Linef("[GPS]", util.ColorYellow, "no packets for %s (%s) -> reconnecting", noPacketTimeout.String(), kind)
		log.Printf("gps: no packets for %s (%s) -> reconnecting", noPacketTimeout.String(), kind)
		// Best-effort close to trigger outer retry loop.
		closer()
	}
}

// runGPSDLoop connects to gpsd and reads JSON reports.
// It looks for TPV messages with mode>=2 and lat/lon fields.
func (s *State) runGPSDLoop(ctx context.Context, addr string) {
	connected := false
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if !connected {
			util.Linef("[GPS]", util.ColorGray, "connecting to gpsd %s", addr)
			log.Printf("gps: connecting to gpsd %s", addr)
		}
		connected = true
		if err := s.readGPSD(ctx, addr); err != nil {
			connected = false
			util.Linef("[GPS]", util.ColorYellow, "gpsd disconnected: %v", err)
			log.Printf("gps: gpsd disconnected: %v", err)
			// Backoff and retry.
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
			}
		}
	}
}

type gpsdTPV struct {
	Class string       `json:"class"`
	Mode  *json.Number `json:"mode"`
	Lat   *float64     `json:"lat"`
	Lon   *float64     `json:"lon"`
}

func (s *State) readGPSD(ctx context.Context, addr string) error {
	conn, err := (&net.Dialer{Timeout: 2 * time.Second}).DialContext(ctx, "tcp", addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	s.setActiveCloser("gpsd", func() {
		_ = conn.Close()
	})
	defer s.clearActiveCloser()

	// Enable watcher mode and JSON reports.
	// gpsd expects lines ending with \n.
	_, _ = conn.Write([]byte("?WATCH={\"enable\":true,\"json\":true}\n"))

	scanner := bufio.NewScanner(conn)
	// gpsd JSON can be longer than default 64K in some modes; bump to 256K.
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 256*1024)

	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		s.updatePacket()

		// Decode only what we need.
		var tpv gpsdTPV
		if err := json.Unmarshal([]byte(line), &tpv); err != nil {
			continue
		}
		if tpv.Class != "TPV" {
			continue
		}
		if tpv.Mode == nil {
			continue
		}
		modeInt, err := tpv.Mode.Int64()
		if err != nil || modeInt < 2 {
			continue
		}
		if tpv.Lat == nil || tpv.Lon == nil {
			continue
		}

		s.updateFix(*tpv.Lat, *tpv.Lon)
	}

	if err := scanner.Err(); err != nil {
		return err
	}
	return errors.New("gpsd connection closed")
}

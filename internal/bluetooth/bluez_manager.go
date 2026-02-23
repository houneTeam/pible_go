package bluetooth

import (
	"context"
	"log"
	"strings"
	"time"

	"github.com/godbus/dbus/v5"

	"pible/internal/db"
	"pible/internal/gps"
	"pible/internal/ids"
	"pible/internal/util"
)

// runManagedAdapterLoop keeps scanning on an adapter with hot-plug support.
// If the adapter disappears (USB unplug), it logs the event and waits until it reappears,
// then resumes discovery and scanning.
func runManagedAdapterLoop(
	ctx context.Context,
	adapterID string,
	store *db.Store,
	gpsState *gps.State,
	resolver *ids.Resolver,
	patterns *DeviceTypePatterns,
	sessionID int64,
	maxConnect int,
	tag *string,
	blacklist *ConnectBlacklist,
) {
	adapterID = strings.TrimSpace(adapterID)
	if adapterID == "" {
		return
	}
	if maxConnect < 1 {
		maxConnect = 1
	}

	conn, err := dbus.SystemBus()
	if err != nil {
		util.Linef("[ERROR]", util.ColorYellow, "dbus SystemBus failed: %v", err)
		return
	}

	// Remember adapter controller address to allow rebinding if the OS renames hciN after hot-unplug.
	// Example: user selected hci1, unplug/replug results in hci2.
	knownAddr := ""
	if a := bluezAdapterAddress(ctx, conn, adapterID); a != "" {
		knownAddr = a
	}

	var wasPresent bool
	backoff := 1 * time.Second
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		present := bluezAdapterExists(ctx, conn, adapterID)
		if !present && knownAddr != "" {
			// Try to rebind by controller address.
			if newID := bluezFindAdapterByAddress(ctx, conn, knownAddr); newID != "" && newID != adapterID {
				util.Linef("[ADAPTER]", util.ColorYellow, "%s remapped to %s (addr=%s)", adapterID, newID, knownAddr)
				log.Printf("adapter: %s remapped to %s (addr=%s)", adapterID, newID, knownAddr)
				adapterID = newID
				present = bluezAdapterExists(ctx, conn, adapterID)
			}
		}
		if present != wasPresent {
			if present {
				util.Linef("[ADAPTER]", util.ColorGreen, "%s connected", adapterID)
				log.Printf("adapter: %s connected", adapterID)
				// Capture address (in case it wasn't available before).
				if knownAddr == "" {
					if a := bluezAdapterAddress(ctx, conn, adapterID); a != "" {
						knownAddr = a
					}
				}
				// Reset backoff after successful reconnect.
				backoff = 1 * time.Second
			} else {
				util.Linef("[ADAPTER]", util.ColorYellow, "%s disconnected", adapterID)
				log.Printf("adapter: %s disconnected", adapterID)
			}
			wasPresent = present
		}
		if !present {
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
			}
			continue
		}

		// Adapter is present. Ensure it's powered (best-effort).
		_ = bluezEnsureAdapterPowered(ctx, conn, adapterID)

		// Run a worker context that gets cancelled if the adapter disappears.
		workerCtx, cancel := context.WithCancel(ctx)
		monDone := make(chan struct{})
		go func() {
			defer close(monDone)
			t := time.NewTicker(2 * time.Second)
			defer t.Stop()
			for {
				select {
				case <-workerCtx.Done():
					return
				case <-t.C:
					if !bluezAdapterExists(workerCtx, conn, adapterID) {
						cancel()
						return
					}
				}
			}
		}()

		_ = runBlueZDiscoveryLoop(workerCtx, adapterID, store, gpsState, resolver, patterns, sessionID, maxConnect, tag, blacklist)
		cancel()
		<-monDone

		// If we're stopping, exit. Otherwise, restart with a small backoff.
		if ctx.Err() != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < 8*time.Second {
			backoff *= 2
		}
	}
}

func bluezAdapterExists(ctx context.Context, conn *dbus.Conn, adapterID string) bool {
	root := conn.Object("org.bluez", dbus.ObjectPath("/"))
	call := root.CallWithContext(ctx, "org.freedesktop.DBus.ObjectManager.GetManagedObjects", 0)
	if call.Err != nil {
		return false
	}
	var managed map[dbus.ObjectPath]map[string]map[string]dbus.Variant
	if err := call.Store(&managed); err != nil {
		return false
	}
	path := "/org/bluez/" + strings.TrimSpace(adapterID)
	ifaces, ok := managed[dbus.ObjectPath(path)]
	if !ok {
		return false
	}
	_, ok = ifaces["org.bluez.Adapter1"]
	return ok
}

func bluezAdapterAddress(ctx context.Context, conn *dbus.Conn, adapterID string) string {
	root := conn.Object("org.bluez", dbus.ObjectPath("/"))
	call := root.CallWithContext(ctx, "org.freedesktop.DBus.ObjectManager.GetManagedObjects", 0)
	if call.Err != nil {
		return ""
	}
	var managed map[dbus.ObjectPath]map[string]map[string]dbus.Variant
	if err := call.Store(&managed); err != nil {
		return ""
	}
	path := dbus.ObjectPath("/org/bluez/" + strings.TrimSpace(adapterID))
	ifaces, ok := managed[path]
	if !ok {
		return ""
	}
	ad, ok := ifaces["org.bluez.Adapter1"]
	if !ok {
		return ""
	}
	if v, ok := ad["Address"]; ok {
		if s, ok := v.Value().(string); ok {
			return strings.ToUpper(strings.TrimSpace(s))
		}
	}
	return ""
}

// bluezFindAdapterByAddress finds an adapter ID (e.g., hci0) by controller Address.
func bluezFindAdapterByAddress(ctx context.Context, conn *dbus.Conn, addr string) string {
	addr = strings.ToUpper(strings.TrimSpace(addr))
	if addr == "" {
		return ""
	}
	root := conn.Object("org.bluez", dbus.ObjectPath("/"))
	call := root.CallWithContext(ctx, "org.freedesktop.DBus.ObjectManager.GetManagedObjects", 0)
	if call.Err != nil {
		return ""
	}
	var managed map[dbus.ObjectPath]map[string]map[string]dbus.Variant
	if err := call.Store(&managed); err != nil {
		return ""
	}
	for path, ifaces := range managed {
		ad, ok := ifaces["org.bluez.Adapter1"]
		if !ok {
			continue
		}
		v, ok := ad["Address"]
		if !ok {
			continue
		}
		s, ok := v.Value().(string)
		if !ok {
			continue
		}
		if strings.ToUpper(strings.TrimSpace(s)) != addr {
			continue
		}
		p := string(path)
		if strings.HasPrefix(p, "/org/bluez/") {
			return strings.TrimPrefix(p, "/org/bluez/")
		}
	}
	return ""
}

// bluezEnsureAdapterPowered sets Adapter1.Powered=true best-effort.
func bluezEnsureAdapterPowered(ctx context.Context, conn *dbus.Conn, adapterID string) error {
	adapterPath := dbus.ObjectPath("/org/bluez/" + strings.TrimSpace(adapterID))
	obj := conn.Object("org.bluez", adapterPath)
	return obj.CallWithContext(ctx, "org.freedesktop.DBus.Properties.Set", 0,
		"org.bluez.Adapter1", "Powered", dbus.MakeVariant(true)).Err
}

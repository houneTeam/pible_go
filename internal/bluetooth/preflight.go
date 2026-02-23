package bluetooth

import (
	"context"
	"strings"
	"time"

	"github.com/godbus/dbus/v5"

	"pible/internal/util"
)

type BlueZCacheMode string

const (
	BlueZCacheOff   BlueZCacheMode = "off"
	BlueZCacheAuto  BlueZCacheMode = "auto"
	BlueZCacheForce BlueZCacheMode = "force"
)

type PreflightOptions struct {
	RestartBluetoothService bool
	CacheMode               BlueZCacheMode
}

func PreflightBlueZ(ctx context.Context, adapters []string, opt PreflightOptions) {
	if len(adapters) == 0 {
		return
	}
	for i := range adapters {
		adapters[i] = strings.TrimSpace(adapters[i])
	}

	conn, err := dbus.SystemBus()
	if err != nil {
		util.Linef("[PREFLIGHT]", util.ColorYellow, "dbus SystemBus error: %v", err)
		return
	}

	// Ensure adapters exist; try restarting bluetooth service if requested.
	missing := []string{}
	for _, a := range adapters {
		if a == "" {
			continue
		}
		if !bluezAdapterExists(ctx, conn, a) {
			missing = append(missing, a)
		}
	}
	if len(missing) > 0 {
		util.Linef("[PREFLIGHT]", util.ColorYellow, "missing adapters: %s", strings.Join(missing, ","))
		if opt.RestartBluetoothService && util.IsRoot() {
			if !util.ServiceIsActive(ctx, "bluetooth") {
				util.Line("[PREFLIGHT]", util.ColorGray, "bluetooth service inactive -> restarting")
				_ = util.RestartService(ctx, "bluetooth")
			}
			// Re-check after restart.
			t := time.NewTimer(1500 * time.Millisecond)
			select {
			case <-ctx.Done():
				t.Stop()
				return
			case <-t.C:
			}
			missing2 := []string{}
			for _, a := range missing {
				if !bluezAdapterExists(ctx, conn, a) {
					missing2 = append(missing2, a)
				}
			}
			if len(missing2) > 0 {
				util.Linef("[PREFLIGHT]", util.ColorYellow, "still missing adapters: %s", strings.Join(missing2, ","))
			}
		}
	}

	// Best-effort cache cleanup.
	if opt.CacheMode == "" {
		opt.CacheMode = BlueZCacheAuto
	}
	if opt.CacheMode == BlueZCacheOff {
		return
	}
	for _, a := range adapters {
		if a == "" {
			continue
		}
		removed := clearBlueZCache(ctx, conn, a, opt.CacheMode)
		if removed > 0 {
			util.Linef("[PREFLIGHT]", util.ColorGray, "adapter=%s cache cleared: %d device objects", a, removed)
		}
	}
}

func clearBlueZCache(ctx context.Context, conn *dbus.Conn, adapterID string, mode BlueZCacheMode) int {
	root := conn.Object("org.bluez", dbus.ObjectPath("/"))
	call := root.CallWithContext(ctx, "org.freedesktop.DBus.ObjectManager.GetManagedObjects", 0)
	if call.Err != nil {
		return 0
	}
	var managed map[dbus.ObjectPath]map[string]map[string]dbus.Variant
	if err := call.Store(&managed); err != nil {
		return 0
	}

	adapterPath := dbus.ObjectPath("/org/bluez/" + adapterID)
	adapterObj := conn.Object("org.bluez", adapterPath)
	adapterPrefix := "/org/bluez/" + adapterID + "/dev_"

	removed := 0
	for path, ifaces := range managed {
		p := string(path)
		if !strings.HasPrefix(p, adapterPrefix) {
			continue
		}
		dev1, ok := ifaces["org.bluez.Device1"]
		if !ok {
			continue
		}
		// Never remove connected devices.
		if c := getBoolPtr(dev1, "Connected"); c != nil && *c {
			continue
		}
		paired := getBoolPtr(dev1, "Paired")
		trusted := getBoolPtr(dev1, "Trusted")

		if mode == BlueZCacheAuto {
			// Remove only unpaired/untrusted objects.
			if (paired != nil && *paired) || (trusted != nil && *trusted) {
				continue
			}
		}

		_ = adapterObj.CallWithContext(ctx, "org.bluez.Adapter1.RemoveDevice", 0, path).Err
		removed++
	}
	return removed
}

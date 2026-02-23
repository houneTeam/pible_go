package bluetooth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/godbus/dbus/v5"

	"pible/internal/db"
	"pible/internal/gps"
	"pible/internal/ids"
	"pible/internal/util"
)

// StartContinuousScanAndConnectMulti runs a continuous BlueZ discovery on one or more adapters.
//
// Design goals:
// - No short scan loops (no Start/Stop every few seconds). Discovery runs continuously.
// - Avoid org.bluez.Error.InProgress by not starting concurrent discoveries on the same adapter.
// - Scale to many devices: DB writes are throttled per MAC; connection attempts are rate-limited.
func StartContinuousScanAndConnectMulti(
	ctx context.Context,
	adapterIDs []string,
	store *db.Store,
	gpsState *gps.State,
	resolver *ids.Resolver,
	patterns *DeviceTypePatterns,
	sessionID int64,
	maxConnectTotal int,
	tag *string,
) error {
	if len(adapterIDs) == 0 {
		return errors.New("no adapters")
	}
	if maxConnectTotal < 1 {
		maxConnectTotal = 1
	}

	gpsState.SetScanningStarted(true)

	// Split connection concurrency across adapters.
	per := maxConnectTotal / len(adapterIDs)
	rest := maxConnectTotal % len(adapterIDs)
	if per < 1 {
		per = 1
		rest = 0
	}

	// Build per-adapter connection limits.
	limits := make(map[string]int, len(adapterIDs))
	for i, a := range adapterIDs {
		maxConn := per
		if i < rest {
			maxConn++
		}
		limits[a] = maxConn
	}

	// Run a managed worker per adapter with hot-plug support.
	var wg sync.WaitGroup
	for _, a := range adapterIDs {
		adapterID := a
		maxConn := limits[adapterID]
		wg.Add(1)
		go func() {
			defer wg.Done()
			runManagedAdapterLoop(ctx, adapterID, store, gpsState, resolver, patterns, sessionID, maxConn, tag)
		}()
	}

	// Block until cancelled.
	<-ctx.Done()
	// Let workers unwind.
	ch := make(chan struct{})
	go func() { wg.Wait(); close(ch) }()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
	}
	return ctx.Err()
}

type bluezConfig struct {
	SnapshotInterval      time.Duration
	DeviceUpdateMinPeriod time.Duration
	AdvInsertMinPeriod    time.Duration
	ClassicHistMinPeriod  time.Duration
	ConnectCooldown       time.Duration
	ConnectRSSIMin        int
	ConnectQueueSize      int
	DiscoverFilterRSSI    int16
	DuplicateData         bool
}

func defaultBlueZConfig() bluezConfig {
	return bluezConfig{
		SnapshotInterval:      3 * time.Second,
		DeviceUpdateMinPeriod: 10 * time.Second,
		AdvInsertMinPeriod:    30 * time.Second,
		ClassicHistMinPeriod:  30 * time.Second,
		ConnectCooldown:       30 * time.Minute,
		ConnectRSSIMin:        -75,
		ConnectQueueSize:      8192,
		DiscoverFilterRSSI:    int16(-90),
		DuplicateData:         false,
	}
}

func runBlueZDiscoveryLoop(
	ctx context.Context,
	adapterID string,
	store *db.Store,
	gpsState *gps.State,
	resolver *ids.Resolver,
	patterns *DeviceTypePatterns,
	sessionID int64,
	maxConnect int,
	tag *string,
) error {
	cfg := defaultBlueZConfig()

	conn, err := dbus.SystemBus()
	if err != nil {
		return err
	}

	adapterPath := dbus.ObjectPath("/org/bluez/" + adapterID)
	adapterObj := conn.Object("org.bluez", adapterPath)

	// Best-effort: set discovery filter. Ignore failures when another process is controlling discovery.
	_ = adapterObj.CallWithContext(ctx, "org.bluez.Adapter1.SetDiscoveryFilter", 0, map[string]dbus.Variant{
		"Transport":     dbus.MakeVariant("auto"),
		"RSSI":          dbus.MakeVariant(cfg.DiscoverFilterRSSI),
		"DuplicateData": dbus.MakeVariant(cfg.DuplicateData),
	}).Err

	// Start discovery once.
	startedByUs := false
	if err := adapterObj.CallWithContext(ctx, "org.bluez.Adapter1.StartDiscovery", 0).Err; err != nil {
		// If already discovering, do not treat as fatal.
		if !strings.Contains(err.Error(), "InProgress") {
			log.Printf("bluez StartDiscovery %s error: %v", adapterID, err)
		} else {
			util.Linef("[SCAN]", util.ColorGray, "adapter=%s discovery already in progress (reusing)", adapterID)
		}
	} else {
		startedByUs = true
		util.Linef("[SCAN]", util.ColorGray, "adapter=%s discovery started", adapterID)
	}

	// Stop discovery only if we started it.
	defer func() {
		if !startedByUs {
			return
		}
		_ = adapterObj.Call("org.bluez.Adapter1.StopDiscovery", 0).Err
	}()

	if maxConnect < 1 {
		maxConnect = 1
	}

	queue := make(chan string, cfg.ConnectQueueSize)
	doneCh := make(chan string, cfg.ConnectQueueSize)
	for i := 0; i < maxConnect; i++ {
		go bluezConnectWorker(ctx, conn, adapterID, store, resolver, patterns, sessionID, tag, queue, doneCh)
	}

	known := make(map[string]bool, 8192)
	inFlight := make(map[string]bool, 8192)
	lastConnAttempt := make(map[string]time.Time, 8192)
	seenCount := make(map[string]int, 8192)

	lastDeviceWrite := make(map[string]time.Time, 8192)
	lastAdvWrite := make(map[string]time.Time, 8192)
	lastClassicHist := make(map[string]time.Time, 8192)
	lastGPSWrite := make(map[string]time.Time, 8192)
	lastGPSVal := make(map[string]string, 8192)
	lastMarked := make(map[string]string, 8192)

	ticker := time.NewTicker(cfg.SnapshotInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}

		// Drain completed connect jobs.
		for {
			select {
			case mac := <-doneCh:
				delete(inFlight, mac)
			default:
				goto drained
			}
		}
	drained:

		// Snapshot all known devices under this adapter.
		snap, err := bluezSnapshotWithConn(ctx, conn, adapterID)
		if err != nil {
			util.Linef("[ERROR]", util.ColorYellow, "scan failed on %s: %v", adapterID, err)
			continue
		}

		if len(snap) == 0 {
			continue
		}

		now := time.Now()
		for mac, bd := range snap {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			mac = strings.ToUpper(strings.TrimSpace(mac))
			if mac == "" {
				continue
			}

			seenCount[mac]++

			name := util.SafeName(bd.Name)
			if !known[mac] {
				known[mac] = true
				util.Linef("[NEW]", util.ColorGreen, "%s (Interface: %s) RSSI: %s", name, adapterID, rssiStr(bd.RSSI))
			} else {
				// Update spam control: only print when we actually write an update.
			}

			// Build common fields.
			ts := util.NowTimestamp()
			gpsStr := gpsState.GPSStringForRecord()
			gLat, gLon, gOK, gCached := gpsState.FixSnapshot()
			var latPtr, lonPtr *float64
			if gOK {
				lat := gLat
				lon := gLon
				latPtr = &lat
				lonPtr = &lon
			}
			var gpsSource *string
			if src := strings.TrimSpace(gpsState.Source()); src != "" {
				gpsSource = &src
			}

			// Determine device type.
			devType := bluezTypeToDeviceType(bd)

			// MAC type/subtype.
			macType := "public_or_unknown"
			macSub := ""
			if bd.AddressType != nil {
				at := strings.ToLower(strings.TrimSpace(*bd.AddressType))
				macSub = at
				if at == "random" {
					macType = "random"
				}
			}

			// Vendor from OUI (MA-L). This may be empty for random/private addresses.
			var vendor *string
			if resolver != nil {
				if v := strings.TrimSpace(resolver.VendorForMAC(mac)); v != "" {
					vv := v
					vendor = &vv
				}
			}

			// Structured manufacturer/service data.
			mfgEntries := bd.ManufacturerEntries
			svcEntries := bd.ServiceDataEntries
			serviceUUIDs := annotateUUIDs(resolver, bd.UUIDs)

			mfgJSON := jsonOrEmptyArray(mfgEntries)
			svcUUIDJSON := jsonOrEmptyArray(serviceUUIDs)
			svcDataJSON := jsonOrEmptyArray(svcEntries)

			advJSON := buildAdvertisementJSONBlueZ(adapterID, bd, name, serviceUUIDs, mfgEntries, svcEntries)

			// Special marker detection (e.g., Coke-ON) from raw UUIDs + manufacturer data.
			markedTypeStr := DetectTypedDevice(patterns, bd.UUIDs, mfgEntries, bd.Name)

			// Throttle full device writes.
			if last, ok := lastDeviceWrite[mac]; ok && now.Sub(last) < cfg.DeviceUpdateMinPeriod {
				// Even when other fields are throttled, refresh GPS if we have a fix.
				if gpsStr != nil {
					gpsText := strings.TrimSpace(*gpsStr)
					if gpsText != "" {
						need := false
						if prev, ok := lastGPSVal[mac]; !ok || prev != gpsText {
							need = true
						} else if t0, ok := lastGPSWrite[mac]; !ok || now.Sub(t0) >= 10*time.Second {
							need = true
						}
						if need {
							_ = store.UpdateDeviceGPS(ctx, mac, gpsText)
							_ = store.RecordDeviceGPSHistoryIfChanged(ctx, &sessionID, mac, ts, latPtr, lonPtr, gpsText, gCached, gpsSource)
							lastGPSVal[mac] = gpsText
							lastGPSWrite[mac] = now
						}
					}
				}
				// Fast marker updates even when full device writes are throttled.
				if strings.TrimSpace(markedTypeStr) != "" {
					mt := strings.TrimSpace(markedTypeStr)
					if prev, ok := lastMarked[mac]; !ok || prev != mt {
						lastMarked[mac] = mt
						util.Linef("[MARK]", util.ColorCyan, "%s (%s) type=%s", name, mac, mt)
					}
					_ = store.UpdateDeviceMarkedType(ctx, mac, mt)
				}
			} else {
				// Full device write.
				lastDeviceWrite[mac] = now
				if seenCount[mac] > 1 {
					util.Linef("[UPDATE]", util.ColorGreen, "%s (Interface: %s) RSSI: %s", name, adapterID, rssiStr(bd.RSSI))
				}

				// Record/refresh GPS in DB + history.
				if gpsStr != nil {
					gpsText := strings.TrimSpace(*gpsStr)
					if gpsText != "" {
						_ = store.UpdateDeviceGPS(ctx, mac, gpsText)
						_ = store.RecordDeviceGPSHistoryIfChanged(ctx, &sessionID, mac, ts, latPtr, lonPtr, gpsText, gCached, gpsSource)
						lastGPSVal[mac] = gpsText
						lastGPSWrite[mac] = now
					}
				}

				// Upsert device.
				nameCopy := name
				adapterCopy := adapterID
				devTypeCopy := devType
				macTypeCopy := macType
				macSubCopy := macSub

				_ = store.SaveDevice(ctx, db.SaveParams{
					SessionID:         &sessionID,
					DeviceType:        &devTypeCopy,
					Name:              &nameCopy,
					MAC:               mac,
					MACType:           &macTypeCopy,
					MACSubType:        &macSubCopy,
					RSSI:              bd.RSSI,
					Timestamp:         &ts,
					Adapter:           &adapterCopy,
					ManufacturerData:  mfgJSON,
					ManufacturerName:  vendor,
					ServiceUUIDs:      svcUUIDJSON,
					ServiceData:       svcDataJSON,
					TxPower:           bd.TxPower,
					PlatformData:      bd.PropsJSON,
					AdvertisementJSON: advJSON,
					GPS:               gpsStr,
					UpdateExisting:    true,
					Tag:               tag,
				})

				// Marker type update.
				if strings.TrimSpace(markedTypeStr) != "" {
					mt := strings.TrimSpace(markedTypeStr)
					if prev, ok := lastMarked[mac]; !ok || prev != mt {
						lastMarked[mac] = mt
						util.Linef("[MARK]", util.ColorCyan, "%s (%s) type=%s", name, mac, mt)
					}
					_ = store.UpdateDeviceMarkedType(ctx, mac, mt)
				}
			}

			// Advertisement history (throttled per MAC).
			if last, ok := lastAdvWrite[mac]; !ok || now.Sub(last) >= cfg.AdvInsertMinPeriod {
				lastAdvWrite[mac] = now
				rssiVal := 0
				if bd.RSSI != nil {
					rssiVal = *bd.RSSI
				}
				id, ierr := store.InsertAdvertisement(ctx, db.AdvertisementParams{
					SessionID: &sessionID,
					MAC:       mac,
					Timestamp: ts,
					RSSI:      &rssiVal,
					Raw:       nil,
					JSON:      advJSON,
				})
				if ierr == nil && id > 0 {
					_ = store.UpdateDeviceLastAdvID(ctx, mac, id)
				}
			}

			// Classic supplemental tables (best-effort) when device is likely BR/EDR.
			if bd.isClassicLikely() {
				if last, ok := lastClassicHist[mac]; !ok || now.Sub(last) >= cfg.ClassicHistMinPeriod {
					lastClassicHist[mac] = now
					rssiVal := 0
					if bd.RSSI != nil {
						rssiVal = *bd.RSSI
					}
					_, _ = store.InsertClassicDiscovery(ctx, db.ClassicDiscoveryParams{
						SessionID: &sessionID,
						MAC:       mac,
						Timestamp: ts,
						RSSI:      &rssiVal,
						Class:     bd.Class,
						PropsJSON: bd.PropsJSON,
					})
				}

				_ = store.UpsertClassicInfo(ctx, db.ClassicInfoParams{
					MAC:           mac,
					Class:         bd.Class,
					Icon:          bd.Icon,
					Paired:        bd.Paired,
					Trusted:       bd.Trusted,
					Connected:     bd.Connected,
					Blocked:       bd.Blocked,
					LegacyPairing: bd.LegacyPairing,
					Modalias:      bd.Modalias,
					UUIDsJSON:     bd.UUIDsJSON,
					LastSeen:      &ts,
					PropsJSON:     bd.PropsJSON,
				})
			}

			// Connection scheduling (BLE / dual only).
			if devType == "classic" {
				continue
			}

			// Must have RSSI above threshold to reduce timeouts.
			if bd.RSSI == nil || *bd.RSSI < cfg.ConnectRSSIMin {
				continue
			}
			// Wait for at least 2 sightings before attempting connect.
			if seenCount[mac] < 2 {
				continue
			}

			hasGatt, _ := store.HasGattServices(ctx, mac)
			if hasGatt {
				continue
			}
			if inFlight[mac] {
				continue
			}
			if last, ok := lastConnAttempt[mac]; ok && now.Sub(last) < cfg.ConnectCooldown {
				continue
			}
			lastConnAttempt[mac] = now
			inFlight[mac] = true

			select {
			case queue <- mac:
				// queued
			default:
				delete(inFlight, mac)
			}
		}
	}
}

func bluezTypeToDeviceType(bd bluezDevice) string {
	if bd.Type != nil {
		t := strings.ToLower(strings.TrimSpace(*bd.Type))
		switch t {
		case "le":
			return "ble"
		case "bredr":
			return "classic"
		case "dual":
			return "dual"
		}
	}
	if bd.isClassicLikely() {
		return "classic"
	}
	return "ble"
}

func annotateUUIDs(resolver *ids.Resolver, uuids []string) []string {
	if len(uuids) == 0 {
		return []string{}
	}
	out := make([]string, 0, len(uuids))
	for _, u := range uuids {
		u = strings.TrimSpace(u)
		if u == "" {
			continue
		}
		if resolver != nil {
			u = resolver.AnnotateServiceUUID(u)
		}
		out = append(out, u)
	}
	return out
}

func buildAdvertisementJSONBlueZ(adapterID string, bd bluezDevice, name string, serviceUUIDs []string, mfg []manufacturerEntry, svc []serviceDataEntry) *string {
	payload := map[string]any{
		"source":        "bluez",
		"adapter":       adapterID,
		"local_name":    strings.TrimSpace(name),
		"service_uuids": serviceUUIDs,
		"manufacturer":  mfg,
		"service_data":  svc,
	}
	if bd.AddressType != nil {
		payload["address_type"] = strings.TrimSpace(*bd.AddressType)
	}
	if bd.Type != nil {
		payload["type"] = strings.TrimSpace(*bd.Type)
	}
	if bd.RSSI != nil {
		payload["rssi"] = *bd.RSSI
	}
	if bd.TxPower != nil {
		payload["tx_power"] = *bd.TxPower
	}
	if bd.Class != nil {
		payload["class"] = *bd.Class
	}
	if bd.Icon != nil {
		payload["icon"] = *bd.Icon
	}
	if b, err := json.Marshal(payload); err == nil {
		s := string(b)
		return &s
	}
	return nil
}

func rssiStr(rssi *int) string {
	if rssi == nil {
		return "n/a"
	}
	return fmt.Sprintf("%d", *rssi)
}

func bluezConnectWorker(
	ctx context.Context,
	conn *dbus.Conn,
	adapterID string,
	store *db.Store,
	resolver *ids.Resolver,
	patterns *DeviceTypePatterns,
	sessionID int64,
	tag *string,
	queue <-chan string,
	doneCh chan<- string,
) {
	for {
		select {
		case <-ctx.Done():
			return
		case mac := <-queue:
			if strings.TrimSpace(mac) == "" {
				continue
			}
			jobCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
			err := ConnectAndDumpGATTBlueZ(jobCtx, conn, adapterID, mac, store, resolver, patterns, sessionID, tag)
			cancel()
			if err != nil {
				// Best-effort: do not spam logs for common transient issues.
				es := err.Error()
				if !strings.Contains(es, "UnknownObject") && !strings.Contains(es, "NotAvailable") {
					log.Printf("bluez connect %s (%s) error: %v", adapterID, mac, err)
				}
			}
			select {
			case doneCh <- mac:
			default:
			}
		}
	}
}

// ConnectAndDumpGATTBlueZ connects using BlueZ and stores a service/characteristic listing.
func ConnectAndDumpGATTBlueZ(
	ctx context.Context,
	conn *dbus.Conn,
	adapterID string,
	mac string,
	store *db.Store,
	resolver *ids.Resolver,
	patterns *DeviceTypePatterns,
	sessionID int64,
	tag *string,
) error {
	mac = strings.ToUpper(strings.TrimSpace(mac))
	if mac == "" {
		return errors.New("empty mac")
	}

	devPath := deviceObjectPath(adapterID, mac)
	devObj := conn.Object("org.bluez", devPath)

	// Connect.
	if err := devObj.CallWithContext(ctx, "org.bluez.Device1.Connect", 0).Err; err != nil {
		return err
	}
	defer func() { _ = devObj.Call("org.bluez.Device1.Disconnect", 0).Err }()

	// Wait for services resolved.
	deadline := time.Now().Add(10 * time.Second)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		resolved, ok := bluezDeviceServicesResolved(ctx, conn, devPath)
		if ok && resolved {
			break
		}
		if time.Now().After(deadline) {
			return errors.New("services not resolved")
		}
		time.Sleep(300 * time.Millisecond)
	}

	// Dump and store characteristic-level data (flags + readable values).
	servicesText, devName, derr := DumpAndStoreGATT(ctx, conn, adapterID, devPath, mac, store, resolver)
	if derr != nil {
		return derr
	}

	// Save to DB.
	ts := util.NowTimestamp()
	_ = store.UpdateGattServices(ctx, mac, servicesText)
	_ = store.InsertGattServicesHistory(ctx, sessionID, mac, servicesText, ts)

	nameCopy := util.SafeName(devName)
	adapterCopy := adapterID
	serviceCopy := servicesText
	typeCopy := "ble"

	_ = store.SaveDevice(ctx, db.SaveParams{
		SessionID:      &sessionID,
		DeviceType:     &typeCopy,
		Name:           &nameCopy,
		MAC:            mac,
		Timestamp:      &ts,
		Adapter:        &adapterCopy,
		ServiceList:    &serviceCopy,
		UpdateExisting: true,
		Tag:            tag,
	})

	util.Linef("[CONNECTED]", util.ColorGreen, "%s (%s) via %s", nameCopy, mac, adapterID)
	return nil
}

func deviceObjectPath(adapterID, mac string) dbus.ObjectPath {
	m := strings.ToUpper(strings.TrimSpace(mac))
	m = strings.ReplaceAll(m, ":", "_")
	return dbus.ObjectPath("/org/bluez/" + adapterID + "/dev_" + m)
}

func bluezDeviceServicesResolved(ctx context.Context, conn *dbus.Conn, devPath dbus.ObjectPath) (bool, bool) {
	root := conn.Object("org.bluez", dbus.ObjectPath("/"))
	call := root.CallWithContext(ctx, "org.freedesktop.DBus.ObjectManager.GetManagedObjects", 0)
	if call.Err != nil {
		return false, false
	}
	var managed map[dbus.ObjectPath]map[string]map[string]dbus.Variant
	if err := call.Store(&managed); err != nil {
		return false, false
	}
	ifaces, ok := managed[devPath]
	if !ok {
		return false, false
	}
	dev1, ok := ifaces["org.bluez.Device1"]
	if !ok {
		return false, false
	}
	v, ok := dev1["ServicesResolved"]
	if !ok {
		return false, true
	}
	b, ok := v.Value().(bool)
	if !ok {
		return false, true
	}
	return b, true
}

func listGattServices(ctx context.Context, conn *dbus.Conn, adapterID string, devPath dbus.ObjectPath, resolver *ids.Resolver) (string, error) {
	root := conn.Object("org.bluez", dbus.ObjectPath("/"))
	call := root.CallWithContext(ctx, "org.freedesktop.DBus.ObjectManager.GetManagedObjects", 0)
	if call.Err != nil {
		return "", call.Err
	}
	var managed map[dbus.ObjectPath]map[string]map[string]dbus.Variant
	if err := call.Store(&managed); err != nil {
		return "", err
	}

	devPrefix := string(devPath) + "/"
	// Collect services in stable order.
	type svcItem struct {
		path string
		uuid string
	}
	services := make([]svcItem, 0, 64)

	for path, ifaces := range managed {
		p := string(path)
		if !strings.HasPrefix(p, devPrefix) {
			continue
		}
		svc, ok := ifaces["org.bluez.GattService1"]
		if !ok {
			continue
		}
		uuid, _ := getString(svc, "UUID")
		uuid = strings.TrimSpace(uuid)
		if uuid == "" {
			continue
		}
		services = append(services, svcItem{path: p, uuid: uuid})
	}

	sort.Slice(services, func(i, j int) bool { return services[i].path < services[j].path })

	lines := make([]string, 0, 256)
	for _, s := range services {
		svcUUID := s.uuid
		if resolver != nil {
			svcUUID = resolver.AnnotateServiceUUID(svcUUID)
		}
		lines = append(lines, fmt.Sprintf("Service: %s", svcUUID))

		// Characteristics under this service.
		chars := make([]string, 0, 32)
		for path, ifaces := range managed {
			p := string(path)
			if !strings.HasPrefix(p, s.path+"/") {
				continue
			}
			ch, ok := ifaces["org.bluez.GattCharacteristic1"]
			if !ok {
				continue
			}
			uuid, _ := getString(ch, "UUID")
			uuid = strings.TrimSpace(uuid)
			if uuid == "" {
				continue
			}
			if resolver != nil {
				uuid = resolver.AnnotateCharacteristicUUID(uuid)
			}
			chars = append(chars, uuid)
		}
		sort.Strings(chars)
		for _, cu := range chars {
			lines = append(lines, fmt.Sprintf("  ├─ Characteristic: %s", cu))
		}
		lines = append(lines, "  └─────────────────────────────────")
	}

	return strings.Join(lines, "\n"), nil
}

func strPtrIfNotEmpty(s string) *string {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	return &s
}

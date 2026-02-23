package bluetooth

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"strings"
	"sync"
	"time"

	tg "tinygo.org/x/bluetooth"

	"pible/internal/db"
	"pible/internal/gps"
	"pible/internal/ids"
	"pible/internal/util"
)

type ScanDevice struct {
	Addr tg.Address
	RSSI int
	Name string

	ManufacturerJSON  *string
	ManufacturerName  *string
	ServiceUUIDsJSON  *string
	ServiceDataJSON   *string
	TxPower           *string
	PlatformData      *string
	AdvertisementRaw  *string
	AdvertisementJSON *string
}

type manufacturerEntry struct {
	CompanyID uint16 `json:"company_id"`
	DataHex   string `json:"data_hex"`
}

type serviceDataEntry struct {
	UUID    string `json:"uuid"`
	Name    string `json:"name,omitempty"`
	DataHex string `json:"data_hex"`
}

type connectJob struct {
	mac       string
	addr      tg.Address
	name      string
	adapterID string
}

func StartContinuousScanAndConnect(
	ctx context.Context,
	adapterID string,
	store *db.Store,
	gpsState *gps.State,
	resolver *ids.Resolver,
	sessionID int64,
	maxConnect int,
	tag *string,
) error {
	adapter := tg.NewAdapter(adapterID)
	if err := adapter.Enable(); err != nil {
		return err
	}

	gpsState.SetScanningStarted(true)

	if maxConnect < 1 {
		maxConnect = 1
	}

	queue := make(chan connectJob, 4096)
	doneCh := make(chan string, 4096)

	for i := 0; i < maxConnect; i++ {
		go connectWorker(ctx, adapter, store, resolver, sessionID, tag, queue, doneCh)
	}

	inFlight := map[string]bool{}
	lastConnAttempt := map[string]time.Time{}
	connCooldown := 10 * time.Minute

	lastAdvInsert := map[string]time.Time{}
	advCooldown := 30 * time.Second

	lastClassicScan := time.Time{}
	classicScanInterval := 30 * time.Second
	classicAdvCooldown := 30 * time.Second
	lastClassicInsert := map[string]time.Time{}

	// BlueZ allows only one discovery session per adapter at a time.
	// Serialize LE scan (tinygo adapter.Scan) and Classic inquiry (DBus StartDiscovery).
	var discoveryMu sync.Mutex

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
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

		util.Linef("[SCAN]", util.ColorGray, "adapter=%s duration=3s", adapterID)
		discoveryMu.Lock()
		scanResults, err := scanFor(ctx, adapter, 3*time.Second, resolver)
		discoveryMu.Unlock()
		if err != nil {
			log.Printf("scan error: %v", err)
			util.Linef("[ERROR]", util.ColorYellow, "scan failed on %s: %v", adapterID, err)
			time.Sleep(3 * time.Second)
			continue
		}

		// Best-effort supplement from BlueZ (TxPower/RSSI/etc when available).
		_ = SupplementBLEFromBlueZ(ctx, adapterID, scanResults)

		if len(scanResults) == 0 {
			util.Line("[SCAN]", util.ColorGray, "no devices")
			time.Sleep(3 * time.Second)
			continue
		}

		for mac, d := range scanResults {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			ts := util.NowTimestamp()
			name := util.SafeName(d.Name)
			rssi := d.RSSI
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
			macType, macSub := ClassifyAddress(d.Addr)

			exists, err := store.DeviceExists(ctx, mac)
			if err != nil {
				log.Printf("db device exists error: %v", err)
			}

			if exists {
				util.Linef("[UPDATE]", util.ColorYellow, "%s (Interface: %s) RSSI: %d", name, adapterID, rssi)
			} else {
				util.Linef("[NEW]", util.ColorGreen, "%s (Interface: %s) RSSI: %d", name, adapterID, rssi)
			}

			// Tag (always persist when provided).
			var tagCopy *string
			if tag != nil {
				t := strings.TrimSpace(*tag)
				if t != "" {
					tagCopy = &t
				}
			}

			// Save scan metadata.
			rssiCopy := rssi
			tsCopy := ts
			adapterCopy := adapterID
			nameCopy := name
			macTypeCopy := macType
			macSubCopy := macSub

			devType := "ble"
			saveErr := store.SaveDevice(ctx, db.SaveParams{
				SessionID:         &sessionID,
				DeviceType:        &devType,
				Name:              &nameCopy,
				MAC:               mac,
				MACType:           &macTypeCopy,
				MACSubType:        &macSubCopy,
				RSSI:              &rssiCopy,
				Timestamp:         &tsCopy,
				Adapter:           &adapterCopy,
				ManufacturerData:  d.ManufacturerJSON,
				ManufacturerName:  d.ManufacturerName,
				ServiceUUIDs:      d.ServiceUUIDsJSON,
				ServiceData:       d.ServiceDataJSON,
				TxPower:           d.TxPower,
				PlatformData:      d.PlatformData,
				AdvertisementJSON: d.AdvertisementJSON,
				GPS:               gpsStr,
				UpdateExisting:    exists,
				Tag:               tagCopy,
			})
			if saveErr != nil {
				log.Printf("db save error: %v", saveErr)
			}
			if gpsStr != nil {
				gpsText := strings.TrimSpace(*gpsStr)
				if gpsText != "" {
					_ = store.RecordDeviceGPSHistoryIfChanged(ctx, &sessionID, mac, ts, latPtr, lonPtr, gpsText, gCached, gpsSource)
				}
			}

			// Insert advertisement history (throttled per MAC) and update devices.last_adv_id.
			if d.AdvertisementJSON != nil || d.AdvertisementRaw != nil {
				last := lastAdvInsert[mac]
				if last.IsZero() || time.Since(last) >= advCooldown {
					lastAdvInsert[mac] = time.Now()
					id, ierr := store.InsertAdvertisement(ctx, db.AdvertisementParams{
						SessionID: &sessionID,
						MAC:       mac,
						Timestamp: ts,
						RSSI:      &rssi,
						Raw:       d.AdvertisementRaw,
						JSON:      d.AdvertisementJSON,
					})
					if ierr == nil && id > 0 {
						_ = store.UpdateDeviceLastAdvID(ctx, mac, id)
					}
				}
			}

			// Connect logic with cooldown, non-blocking.
			hasGatt, _ := store.HasGattServices(ctx, mac)
			if hasGatt {
				continue
			}
			if inFlight[mac] {
				continue
			}
			if last, ok := lastConnAttempt[mac]; ok {
				if time.Since(last) < connCooldown {
					continue
				}
			}
			lastConnAttempt[mac] = time.Now()

			job := connectJob{mac: mac, addr: d.Addr, name: name, adapterID: adapterID}
			inFlight[mac] = true
			select {
			case queue <- job:
				// queued
			default:
				// queue full; drop
				delete(inFlight, mac)
			}
		}

		// Do not wait for connect jobs; keep scanning.
		// Periodically run Classic (BR/EDR) discovery and store results.
		if lastClassicScan.IsZero() || time.Since(lastClassicScan) >= classicScanInterval {
			lastClassicScan = time.Now()
			// Give BlueZ a short moment to settle after LE StopScan before starting BR/EDR inquiry.
			time.Sleep(250 * time.Millisecond)
			discoveryMu.Lock()
			classic, err := ScanClassicBlueZ(ctx, adapterID, 7*time.Second, resolver)
			discoveryMu.Unlock()
			if err != nil {
				log.Printf("classic scan error: %v", err)
			} else {
				for mac, cd := range classic {
					select {
					case <-ctx.Done():
						return ctx.Err()
					default:
					}

					// Throttle history inserts.
					last := lastClassicInsert[mac]
					if last.IsZero() || time.Since(last) >= classicAdvCooldown {
						lastClassicInsert[mac] = time.Now()
						_, _ = store.InsertClassicDiscovery(ctx, db.ClassicDiscoveryParams{
							SessionID: &sessionID,
							MAC:       mac,
							Timestamp: util.NowTimestamp(),
							RSSI:      cd.RSSI,
							Class:     cd.Class,
							PropsJSON: cd.PropsJSON,
						})
					}

					name := util.SafeName(cd.Name)
					macType := "public_or_unknown"
					macSub := ""
					adapterCopy := adapterID
					ts := util.NowTimestamp()
					nameCopy := name
					typeCopy := "classic"

					// Manufacturer from OUI for classic devices (public address).
					var vendor *string
					if resolver != nil {
						if v := strings.TrimSpace(resolver.VendorForMAC(mac)); v != "" {
							vendor = &v
						}
					}

					exists, _ := store.DeviceExists(ctx, mac)
					_ = store.SaveDevice(ctx, db.SaveParams{
						SessionID:        &sessionID,
						DeviceType:       &typeCopy,
						Name:             &nameCopy,
						MAC:              mac,
						MACType:          &macType,
						MACSubType:       &macSub,
						RSSI:             cd.RSSI,
						Timestamp:        &ts,
						Adapter:          &adapterCopy,
						ManufacturerName: vendor,
						ServiceUUIDs:     cd.UUIDsJSON,
						TxPower:          cd.TxPower,
						PlatformData:     cd.PropsJSON,
						GPS:              gpsState.GPSStringForRecord(),
						UpdateExisting:   exists,
						Tag:              tag,
					})

					_ = store.UpsertClassicInfo(ctx, db.ClassicInfoParams{
						MAC:           mac,
						Class:         cd.Class,
						Icon:          cd.Icon,
						Paired:        cd.Paired,
						Trusted:       cd.Trusted,
						Connected:     cd.Connected,
						Blocked:       cd.Blocked,
						LegacyPairing: cd.LegacyPairing,
						Modalias:      cd.Modalias,
						UUIDsJSON:     cd.UUIDsJSON,
						LastSeen:      &ts,
						PropsJSON:     cd.PropsJSON,
					})
				}
			}
		}

		time.Sleep(3 * time.Second)
	}
}

func connectWorker(
	ctx context.Context,
	adapter *tg.Adapter,
	store *db.Store,
	resolver *ids.Resolver,
	sessionID int64,
	tag *string,
	queue <-chan connectJob,
	doneCh chan<- string,
) {
	for {
		select {
		case <-ctx.Done():
			return
		case job := <-queue:
			// If channel is closed, job will be zero value; treat as exit.
			if job.mac == "" {
				continue
			}

			util.Linef("[CONNECT]", util.ColorGray, "%s starting", job.mac)

			// Overall watchdog for a single connect+GATT attempt.
			jobCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			err := ConnectAndDumpGATT(jobCtx, adapter, job.addr, job.name, job.adapterID, store, resolver, sessionID, tag)
			cancel()

			if err != nil {
				es := err.Error()
				if strings.Contains(es, "org.freedesktop.DBus.Error.UnknownObject") || strings.Contains(es, "Method \"Get\" with signature \"ss\"") {
					// device vanished between scan and connect
					goto done
				}
				if strings.Contains(es, "le-connection-abort-by-local") {
					goto done
				}
				if errors.Is(err, context.DeadlineExceeded) {
					goto done
				}
				log.Printf("connect error %s: %v", job.mac, err)
			}

		done:
			select {
			case doneCh <- job.mac:
			default:
			}
		}
	}
}

func scanFor(ctx context.Context, adapter *tg.Adapter, d time.Duration, resolver *ids.Resolver) (map[string]ScanDevice, error) {
	results := map[string]ScanDevice{}
	var mu sync.Mutex

	// Best-effort: ensure a previous scan is not still considered active by BlueZ.
	_ = adapter.StopScan()
	time.Sleep(150 * time.Millisecond)

	scanErrCh := make(chan error, 1)

	go func() {
		err := adapter.Scan(func(_ *tg.Adapter, res tg.ScanResult) {
			mac := strings.ToUpper(res.Address.String())

			localName := res.LocalName()
			serviceUUIDs := res.ServiceUUIDs()
			mfg := res.ManufacturerData()
			svcData := res.ServiceData()
			advBytes := res.Bytes()

			// Service UUIDs (annotated).
			serviceUUIDStrs := make([]string, 0, len(serviceUUIDs))
			for _, u := range serviceUUIDs {
				uuidStr := u.String()
				if resolver != nil {
					uuidStr = resolver.AnnotateServiceUUID(uuidStr)
				}
				serviceUUIDStrs = append(serviceUUIDStrs, uuidStr)
			}

			// Manufacturer data.
			mfgEntries := make([]manufacturerEntry, 0, len(mfg))
			for _, m := range mfg {
				mfgEntries = append(mfgEntries, manufacturerEntry{
					CompanyID: m.CompanyID,
					DataHex:   util.BytesToHex(append([]byte(nil), m.Data...)),
				})
			}

			// Service data.
			svcEntries := make([]serviceDataEntry, 0, len(svcData))
			for _, s := range svcData {
				uuidStr := s.UUID.String()
				name := ""
				if resolver != nil {
					name = resolver.ServiceName(uuidStr)
				}
				svcEntries = append(svcEntries, serviceDataEntry{
					UUID:    uuidStr,
					Name:    name,
					DataHex: util.BytesToHex(append([]byte(nil), s.Data...)),
				})
			}

			// Always write JSON fields (use [] when empty) so DB columns are not NULL.
			mfgJSON := jsonOrEmptyArray(mfgEntries)
			svcUUIDJSON := jsonOrEmptyArray(serviceUUIDStrs)
			svcDataJSON := jsonOrEmptyArray(svcEntries)

			rssi := int(res.RSSI)

			// Vendor from OUI (may be empty for private/random addresses).
			var vendor *string
			if resolver != nil {
				if v := strings.TrimSpace(resolver.VendorForMAC(mac)); v != "" {
					vendor = &v
				}
			}

			advRaw, advJSON, txPowerStr, platformDataStr := buildAdvertisementJSON(localName, serviceUUIDStrs, mfgEntries, svcEntries, advBytes)

			mu.Lock()
			results[mac] = ScanDevice{
				Addr:              res.Address,
				RSSI:              rssi,
				Name:              localName,
				ManufacturerJSON:  mfgJSON,
				ManufacturerName:  vendor,
				ServiceUUIDsJSON:  svcUUIDJSON,
				ServiceDataJSON:   svcDataJSON,
				TxPower:           txPowerStr,
				PlatformData:      platformDataStr,
				AdvertisementRaw:  advRaw,
				AdvertisementJSON: advJSON,
			}
			mu.Unlock()
		})
		scanErrCh <- err
	}()

	select {
	case <-ctx.Done():
		_ = adapter.StopScan()
		// Wait for scan goroutine to exit (best-effort).
		select {
		case <-scanErrCh:
		case <-time.After(8 * time.Second):
		}
		return results, ctx.Err()
	case <-time.After(d):
		_ = adapter.StopScan()
		select {
		case <-scanErrCh:
		case <-time.After(8 * time.Second):
			return results, errors.New("scan stop timeout (bluez still discovering)")
		}
		// Give BlueZ a short moment to settle.
		time.Sleep(150 * time.Millisecond)
		return results, nil
	case err := <-scanErrCh:
		// If scan exited early (including InProgress), ensure we are stopped.
		_ = adapter.StopScan()
		time.Sleep(150 * time.Millisecond)
		return results, err
	}
}

func jsonOrEmptyArray(v any) *string {
	b, err := json.Marshal(v)
	if err != nil {
		s := "[]"
		return &s
	}
	s := string(b)
	return &s
}

package bluetooth

import (
	"context"
	"fmt"
	"strings"
	"time"

	tg "tinygo.org/x/bluetooth"

	"pible/internal/db"
	"pible/internal/ids"
	"pible/internal/util"
)

func ConnectAndDumpGATT(ctx context.Context, adapter *tg.Adapter, addr tg.Address, displayName, adapterLabel string, store *db.Store, resolver *ids.Resolver, sessionID int64, tag *string) error {
	params := tg.ConnectionParams{ConnectionTimeout: tg.NewDuration(15 * time.Second)}
	dev, err := adapter.Connect(addr, params)
	if err != nil {
		return err
	}
	defer func() { _ = dev.Disconnect() }()

	// Give BlueZ a moment to populate services.
	time.Sleep(200 * time.Millisecond)

	// Discover services with our own timeout.
	services, err := discoverServicesWithTimeout(dev, 8*time.Second)
	if err != nil {
		return err
	}

	lines := make([]string, 0, 64)
	for _, svc := range services {
		svcUUID := svc.UUID().String()
		if resolver != nil {
			svcUUID = resolver.AnnotateServiceUUID(svcUUID)
		}
		lines = append(lines, fmt.Sprintf("Service: %s", svcUUID))

		chars, err := svc.DiscoverCharacteristics(nil)
		if err != nil {
			lines = append(lines, fmt.Sprintf("  ├─ DiscoverCharacteristics error: %v", err))
			lines = append(lines, "  └─────────────────────────────────")
			continue
		}

		for _, ch := range chars {
			chUUID := ch.UUID().String()
			if resolver != nil {
				chUUID = resolver.AnnotateCharacteristicUUID(chUUID)
			}
			lines = append(lines, fmt.Sprintf("  ├─ Characteristic: %s", chUUID))

			buf := make([]byte, 512)
			n, rerr := ch.Read(buf)
			if rerr != nil {
				lines = append(lines, fmt.Sprintf("  │  Read error: %v", rerr))
			} else {
				val := buf[:n]
				lines = append(lines, fmt.Sprintf("  │  Value(hex): %s", util.BytesToHex(val)))
				if s := asciiIfPrintable(val); s != "" {
					lines = append(lines, fmt.Sprintf("  │  Value(ascii): %s", s))
				}
			}

			lines = append(lines, "  └─────────────────────────────────")
		}
	}

	serviceList := strings.Join(lines, "\n")

	// Persist (latest + per-session history).
	addrStr := strings.ToUpper(addr.String())
	now := util.NowTimestamp()
	_ = store.UpdateGattServices(ctx, addrStr, serviceList)
	_ = store.InsertGattServicesHistory(ctx, sessionID, addrStr, serviceList, now)

	nameCopy := util.SafeName(displayName)
	adapterCopy := adapterLabel
	serviceCopy := serviceList

	var tagCopy *string
	if tag != nil {
		t := strings.TrimSpace(*tag)
		if t != "" {
			tagCopy = &t
		}
	}

	devType := "ble"
	if err := store.SaveDevice(ctx, db.SaveParams{
		SessionID:      &sessionID,
		DeviceType:     &devType,
		Name:           &nameCopy,
		MAC:            addrStr,
		Timestamp:      &now,
		Adapter:        &adapterCopy,
		ServiceList:    &serviceCopy,
		UpdateExisting: true,
		Tag:            tagCopy,
	}); err != nil {
		return err
	}

	util.Linef("[CONNECTED]", util.ColorGreen, "%s (%s)", nameCopy, addrStr)
	return nil
}

func discoverServicesWithTimeout(dev tg.Device, timeout time.Duration) ([]tg.DeviceService, error) {
	type res struct {
		s []tg.DeviceService
		e error
	}
	ch := make(chan res, 1)
	go func() {
		s, e := dev.DiscoverServices(nil)
		ch <- res{s: s, e: e}
	}()

	select {
	case r := <-ch:
		return r.s, r.e
	case <-time.After(timeout):
		return nil, fmt.Errorf("timeout on DiscoverServices")
	}
}

func asciiIfPrintable(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	for _, c := range b {
		if c == '\n' || c == '\r' || c == '\t' {
			continue
		}
		if c < 0x20 || c > 0x7e {
			return ""
		}
	}
	return string(b)
}

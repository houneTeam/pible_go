package bluetooth

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/godbus/dbus/v5"

	"pible/internal/ids"
	"pible/internal/util"
)

// ClassicScanDevice holds BR/EDR (Classic) discovery data.
// Most fields are best-effort and depend on BlueZ and the remote device.
type ClassicScanDevice struct {
	Name          string
	RSSI          *int
	TxPower       *string
	Class         *uint32
	Icon          *string
	Paired        *bool
	Trusted       *bool
	Connected     *bool
	Blocked       *bool
	LegacyPairing *bool
	Modalias      *string
	UUIDsJSON     *string
	PropsJSON     *string
}

// SupplementBLEFromBlueZ attempts to fill missing scan fields (notably TxPower)
// using BlueZ's org.bluez.Device1 properties.
//
// BlueZ may omit TxPower (optional) and RSSI may be stale/absent.
func SupplementBLEFromBlueZ(ctx context.Context, adapterID string, results map[string]ScanDevice) error {
	snap, err := bluezSnapshot(ctx, adapterID)
	if err != nil {
		return err
	}
	for mac, d := range results {
		bd, ok := snap[mac]
		if !ok {
			continue
		}
		changed := false
		if d.TxPower == nil && bd.TxPower != nil {
			d.TxPower = bd.TxPower
			changed = true
		}
		// As a fallback, if the scan RSSI was 0 (rare), use BlueZ RSSI.
		if d.RSSI == 0 && bd.RSSI != nil {
			d.RSSI = *bd.RSSI
			changed = true
		}
		if changed {
			results[mac] = d
		}
	}
	return nil
}

// ScanClassicBlueZ performs a BR/EDR discovery (best-effort) using BlueZ D-Bus.
// It returns devices that are likely Classic (Type == "bredr"/"dual" or non-zero Class).
func ScanClassicBlueZ(ctx context.Context, adapterID string, duration time.Duration, resolver *ids.Resolver) (map[string]ClassicScanDevice, error) {
	if duration <= 0 {
		duration = 7 * time.Second
	}

	conn, err := dbus.SystemBus()
	if err != nil {
		return nil, err
	}

	adapterPath := dbus.ObjectPath("/org/bluez/" + adapterID)
	adapterObj := conn.Object("org.bluez", adapterPath)

	// Try to restrict discovery to Classic to avoid interfering with LE scans.
	_ = adapterObj.CallWithContext(ctx, "org.bluez.Adapter1.SetDiscoveryFilter", 0, map[string]dbus.Variant{
		"Transport": dbus.MakeVariant("bredr"),
	}).Err

	// Start discovery (ignore InProgress).
	if err := adapterObj.CallWithContext(ctx, "org.bluez.Adapter1.StartDiscovery", 0).Err; err != nil {
		// If already discovering, keep going; we'll snapshot whatever is available.
		if !strings.Contains(err.Error(), "InProgress") {
			// Keep going anyway.
		}
	}

	// Wait.
	select {
	case <-ctx.Done():
		_ = adapterObj.Call("org.bluez.Adapter1.StopDiscovery", 0).Err
		return nil, ctx.Err()
	case <-time.After(duration):
	}

	_ = adapterObj.CallWithContext(ctx, "org.bluez.Adapter1.StopDiscovery", 0).Err
	// Clear discovery filter (best-effort).
	_ = adapterObj.CallWithContext(ctx, "org.bluez.Adapter1.SetDiscoveryFilter", 0, map[string]dbus.Variant{}).Err

	snap, err := bluezSnapshotWithConn(ctx, conn, adapterID)
	if err != nil {
		return nil, err
	}

	out := make(map[string]ClassicScanDevice, 256)
	for mac, bd := range snap {
		if !bd.isClassicLikely() {
			continue
		}

		name := strings.TrimSpace(bd.Name)
		if name == "" {
			name = "Unknown"
		}

		// Annotate UUIDs best-effort (service names are BLE-centric but still useful when UUIDs overlap).
		uuidJSON := bd.UUIDsJSON
		if resolver != nil && uuidJSON != nil {
			// Leave as-is; we already store UUID strings.
		}

		out[mac] = ClassicScanDevice{
			Name:          name,
			RSSI:          bd.RSSI,
			TxPower:       bd.TxPower,
			Class:         bd.Class,
			Icon:          bd.Icon,
			Paired:        bd.Paired,
			Trusted:       bd.Trusted,
			Connected:     bd.Connected,
			Blocked:       bd.Blocked,
			LegacyPairing: bd.LegacyPairing,
			Modalias:      bd.Modalias,
			UUIDsJSON:     uuidJSON,
			PropsJSON:     bd.PropsJSON,
		}
	}

	return out, nil
}

type bluezDevice struct {
	Name          string
	Type          *string
	AddressType   *string
	RSSI          *int
	TxPower       *string
	UUIDs         []string
	ManufacturerEntries []manufacturerEntry
	ServiceDataEntries  []serviceDataEntry
	Class         *uint32
	Icon          *string
	Paired        *bool
	Trusted       *bool
	Connected     *bool
	Blocked       *bool
	LegacyPairing *bool
	Modalias      *string
	UUIDsJSON     *string
	PropsJSON     *string
}

func (d bluezDevice) isClassicLikely() bool {
	if d.Type != nil {
		t := strings.ToLower(strings.TrimSpace(*d.Type))
		if t == "bredr" || t == "dual" {
			return true
		}
		if t == "le" {
			return false
		}
	}
	if d.Class != nil && *d.Class != 0 {
		return true
	}
	// As a fallback, presence of legacy pairing often indicates Classic.
	if d.LegacyPairing != nil && *d.LegacyPairing {
		return true
	}
	return false
}

func bluezSnapshot(ctx context.Context, adapterID string) (map[string]bluezDevice, error) {
	conn, err := dbus.SystemBus()
	if err != nil {
		return nil, err
	}
	return bluezSnapshotWithConn(ctx, conn, adapterID)
}

func bluezSnapshotWithConn(ctx context.Context, conn *dbus.Conn, adapterID string) (map[string]bluezDevice, error) {
	root := conn.Object("org.bluez", dbus.ObjectPath("/"))
	call := root.CallWithContext(ctx, "org.freedesktop.DBus.ObjectManager.GetManagedObjects", 0)
	if call.Err != nil {
		return nil, call.Err
	}

	var managed map[dbus.ObjectPath]map[string]map[string]dbus.Variant
	if err := call.Store(&managed); err != nil {
		return nil, err
	}

	adapterPrefix := "/org/bluez/" + adapterID + "/dev_"
	out := make(map[string]bluezDevice, 1024)

	for path, ifaces := range managed {
		p := string(path)
		if !strings.HasPrefix(p, adapterPrefix) {
			continue
		}
		dev1, ok := ifaces["org.bluez.Device1"]
		if !ok {
			continue
		}

		addr, _ := getString(dev1, "Address")
		addr = strings.TrimSpace(addr)
		if addr == "" {
			continue
		}

		name, _ := getString(dev1, "Name")
		if name == "" {
			name, _ = getString(dev1, "Alias")
		}

		var typ *string
		if v, ok := getString(dev1, "Type"); ok {
			vv := strings.TrimSpace(v)
			typ = &vv
		}

		var addrType *string
		if v, ok := getString(dev1, "AddressType"); ok {
			vv := strings.TrimSpace(v)
			addrType = &vv
		}

		var classPtr *uint32
		if v, ok := dev1["Class"]; ok {
			if c, ok2 := v.Value().(uint32); ok2 {
				cc := c
				classPtr = &cc
			}
		}

		icon := getStringPtr(dev1, "Icon")
		modalias := getStringPtr(dev1, "Modalias")

		paired := getBoolPtr(dev1, "Paired")
		trusted := getBoolPtr(dev1, "Trusted")
		connected := getBoolPtr(dev1, "Connected")
		blocked := getBoolPtr(dev1, "Blocked")
		legacy := getBoolPtr(dev1, "LegacyPairing")

		rssi := getInt16AsIntPtr(dev1, "RSSI")
		txp := getInt16AsIntPtr(dev1, "TxPower")
		var txPowerStr *string
		if txp != nil {
			s := formatSignedInt8Like(*txp)
			txPowerStr = &s
		}

		uuidList := getUUIDsList(dev1)
		uuidJSON := uuidListToJSON(uuidList)
		mfgEntries := parseManufacturerEntries(dev1)
		svcEntries := parseServiceDataEntries(dev1)
		propsJSON := propsToJSON(dev1)

		out[strings.ToUpper(addr)] = bluezDevice{
			Name:          name,
			Type:          typ,
			AddressType:   addrType,
			RSSI:          rssi,
			TxPower:       txPowerStr,
			UUIDs:         uuidList,
			ManufacturerEntries: mfgEntries,
			ServiceDataEntries:  svcEntries,
			Class:         classPtr,
			Icon:          icon,
			Paired:        paired,
			Trusted:       trusted,
			Connected:     connected,
			Blocked:       blocked,
			LegacyPairing: legacy,
			Modalias:      modalias,
			UUIDsJSON:     uuidJSON,
			PropsJSON:     propsJSON,
		}
	}

	return out, nil
}

func getString(props map[string]dbus.Variant, key string) (string, bool) {
	v, ok := props[key]
	if !ok {
		return "", false
	}
	s, ok := v.Value().(string)
	if !ok {
		return "", false
	}
	return s, true
}

func getStringPtr(props map[string]dbus.Variant, key string) *string {
	if s, ok := getString(props, key); ok {
		s = strings.TrimSpace(s)
		if s == "" {
			return nil
		}
		return &s
	}
	return nil
}

func getBoolPtr(props map[string]dbus.Variant, key string) *bool {
	v, ok := props[key]
	if !ok {
		return nil
	}
	b, ok := v.Value().(bool)
	if !ok {
		return nil
	}
	bb := b
	return &bb
}

func getInt16AsIntPtr(props map[string]dbus.Variant, key string) *int {
	v, ok := props[key]
	if !ok {
		return nil
	}
	sw := v.Value()
	switch x := sw.(type) {
	case int16:
		vv := int(x)
		return &vv
	case int32:
		vv := int(x)
		return &vv
	case int:
		vv := x
		return &vv
	default:
		return nil
	}
}


func getUUIDsList(props map[string]dbus.Variant) []string {
	v, ok := props["UUIDs"]
	if !ok {
		return nil
	}
	u, ok := v.Value().([]string)
	if !ok {
		return nil
	}
	return u
}

func uuidListToJSON(u []string) *string {
	if len(u) == 0 {
		s := "[]"
		return &s
	}
	b, err := json.Marshal(u)
	if err != nil {
		s := "[]"
		return &s
	}
	s := string(b)
	return &s
}

func parseManufacturerEntries(props map[string]dbus.Variant) []manufacturerEntry {
	v, ok := props["ManufacturerData"]
	if !ok {
		return nil
	}
	sw := v.Value()
	out := make([]manufacturerEntry, 0, 8)
	switch mm := sw.(type) {
	case map[uint16][]byte:
		for k, b := range mm {
			out = append(out, manufacturerEntry{CompanyID: k, DataHex: util.BytesToHex(append([]byte(nil), b...))})
		}
	case map[uint16]dbus.Variant:
		for k, vv := range mm {
			if b, ok := vv.Value().([]byte); ok {
				out = append(out, manufacturerEntry{CompanyID: k, DataHex: util.BytesToHex(append([]byte(nil), b...))})
			}
		}
	default:
		return nil
	}
	if len(out) == 0 {
		return nil
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CompanyID < out[j].CompanyID })
	return out
}

func parseServiceDataEntries(props map[string]dbus.Variant) []serviceDataEntry {
	v, ok := props["ServiceData"]
	if !ok {
		return nil
	}
	sw := v.Value()
	out := make([]serviceDataEntry, 0, 8)
	switch mm := sw.(type) {
	case map[string][]byte:
		for k, b := range mm {
			k = strings.TrimSpace(k)
			if k == "" {
				continue
			}
			out = append(out, serviceDataEntry{UUID: k, DataHex: util.BytesToHex(append([]byte(nil), b...))})
		}
	case map[string]dbus.Variant:
		for k, vv := range mm {
			k = strings.TrimSpace(k)
			if k == "" {
				continue
			}
			if b, ok := vv.Value().([]byte); ok {
				out = append(out, serviceDataEntry{UUID: k, DataHex: util.BytesToHex(append([]byte(nil), b...))})
			}
		}
	default:
		return nil
	}
	if len(out) == 0 {
		return nil
	}
	sort.Slice(out, func(i, j int) bool { return out[i].UUID < out[j].UUID })
	return out
}

func propsToJSON(props map[string]dbus.Variant) *string {
	// Convert all properties to JSON-friendly values.
	m := make(map[string]any, len(props))
	for k, v := range props {
		m[k] = sanitizeDBusValue(v.Value())
	}
	b, err := json.Marshal(m)
	if err != nil {
		return nil
	}
	s := string(b)
	return &s
}

func sanitizeDBusValue(v any) any {
	if v == nil {
		return nil
	}

	// Common special cases.
	switch x := v.(type) {
	case dbus.ObjectPath:
		return string(x)
	case []byte:
		return util.BytesToHex(append([]byte(nil), x...))
	case []string:
		return x
	case map[string]dbus.Variant:
		m := make(map[string]any, len(x))
		for k, vv := range x {
			m[k] = sanitizeDBusValue(vv.Value())
		}
		return m
	case map[uint16]dbus.Variant:
		m := make(map[string]any, len(x))
		for k, vv := range x {
			m[fmt.Sprintf("0x%04X", k)] = sanitizeDBusValue(vv.Value())
		}
		return m
	case map[string][]byte:
		m := make(map[string]any, len(x))
		for k, vv := range x {
			m[k] = util.BytesToHex(append([]byte(nil), vv...))
		}
		return m
	case map[uint16][]byte:
		m := make(map[string]any, len(x))
		for k, vv := range x {
			m[fmt.Sprintf("0x%04X", k)] = util.BytesToHex(append([]byte(nil), vv...))
		}
		return m
	}

	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Slice, reflect.Array:
		l := rv.Len()
		out := make([]any, 0, l)
		for i := 0; i < l; i++ {
			out = append(out, sanitizeDBusValue(rv.Index(i).Interface()))
		}
		return out
	case reflect.Map:
		out := make(map[string]any, rv.Len())
		iter := rv.MapRange()
		for iter.Next() {
			k := iter.Key().Interface()
			vk := fmt.Sprint(k)
			out[vk] = sanitizeDBusValue(iter.Value().Interface())
		}
		return out
	default:
		return v
	}
}

func formatSignedInt8Like(v int) string {
	if v >= 0 {
		return "+" + strconvItoa(v)
	}
	return strconvItoa(v)
}

func strconvItoa(x int) string {
	if x == 0 {
		return "0"
	}
	neg := x < 0
	if neg {
		x = -x
	}
	buf := [12]byte{}
	i := len(buf)
	for x > 0 {
		i--
		buf[i] = byte('0' + x%10)
		x /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

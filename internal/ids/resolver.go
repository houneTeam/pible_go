package ids

import (
	"strings"
)

// Resolver provides name lookups for various identifiers used in BLE scanning.
//
// - Vendor names are resolved by MAC OUI (from oui.csv).
// - Service UUID names are resolved from service_uuids.yaml.
// - Characteristic UUID names are resolved from characteristic_uuids.yaml.
//
// All UUID keys are stored in canonical 128-bit, lower-case form.
type Resolver struct {
	vendors map[string]string

	serviceUUIDNames map[string]string
	charUUIDNames    map[string]string
}

func (r *Resolver) VendorForMAC(mac string) string {
	if r == nil || len(r.vendors) == 0 {
		return ""
	}
	oui := macToOUI(mac)
	if oui == "" {
		return ""
	}
	if v, ok := r.vendors[oui]; ok {
		return v
	}
	return ""
}

func (r *Resolver) ServiceName(uuid128Lower string) string {
	if r == nil || len(r.serviceUUIDNames) == 0 {
		return ""
	}
	u := strings.ToLower(strings.TrimSpace(uuid128Lower))
	if u == "" {
		return ""
	}
	if v, ok := r.serviceUUIDNames[u]; ok {
		return v
	}
	return ""
}

func (r *Resolver) CharacteristicName(uuid128Lower string) string {
	if r == nil || len(r.charUUIDNames) == 0 {
		return ""
	}
	u := strings.ToLower(strings.TrimSpace(uuid128Lower))
	if u == "" {
		return ""
	}
	if v, ok := r.charUUIDNames[u]; ok {
		return v
	}
	return ""
}

func (r *Resolver) AnnotateServiceUUID(uuid128Lower string) string {
	name := r.ServiceName(uuid128Lower)
	if name == "" {
		return uuid128Lower
	}
	return uuid128Lower + " (" + name + ")"
}

func (r *Resolver) AnnotateCharacteristicUUID(uuid128Lower string) string {
	name := r.CharacteristicName(uuid128Lower)
	if name == "" {
		return uuid128Lower
	}
	return uuid128Lower + " (" + name + ")"
}

func macToOUI(mac string) string {
	mac = strings.TrimSpace(mac)
	if mac == "" {
		return ""
	}
	// Expected formats: AA:BB:CC:DD:EE:FF or AA-BB-CC-DD-EE-FF
	parts := strings.FieldsFunc(mac, func(r rune) bool {
		return r == ':' || r == '-'
	})
	if len(parts) < 3 {
		return ""
	}
	oui := strings.ToUpper(parts[0] + parts[1] + parts[2])
	if len(oui) != 6 {
		return ""
	}
	return oui
}

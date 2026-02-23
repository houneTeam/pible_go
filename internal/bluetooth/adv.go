package bluetooth

import (
	"encoding/json"
	"strings"

	"pible/internal/util"
)

// buildAdvertisementJSON builds:
// - advertisement_raw (hex) when raw bytes are available
// - advertisement_json (decoded + structured fields)
// - tx_power (string) when present
// - platform_data (JSON, always best-effort)
//
// Note: AdvertisementPayload.Bytes() may be nil depending on platform/stack. See tinygo.org/x/bluetooth docs.
func buildAdvertisementJSON(
	localName string, serviceUUIDs []string, mfg []manufacturerEntry, svcData []serviceDataEntry, advBytes []byte,
) (advRaw *string, advJSON *string, txPower *string, platformData *string) {
	payload := map[string]any{
		"source":        "tinygo.org/x/bluetooth",
		"local_name":    strings.TrimSpace(localName),
		"service_uuids": serviceUUIDs,
		"manufacturer":  mfg,
		"service_data":  svcData,
	}

	if advBytes != nil && len(advBytes) > 0 {
		advCopy := append([]byte(nil), advBytes...)
		rawHex := util.BytesToHex(advCopy)
		advRaw = &rawHex

		items, txp := decodeADStructures(advCopy)
		payload["ad_structures"] = items
		payload["adv_hex"] = rawHex
		payload["adv_size"] = len(advCopy)

		if txp != nil {
			s := *txp
			txPower = &s
		}
	}

	// platform_data: store the payload, even if raw bytes are not available.
	if b, err := json.Marshal(payload); err == nil {
		s := string(b)
		platformData = &s
		advJSON = &s
	}

	return
}

type adItem struct {
	TypeHex string `json:"type"`
	Name    string `json:"name,omitempty"`
	DataHex string `json:"data_hex"`
	Text    string `json:"text,omitempty"`
}

func decodeADStructures(adv []byte) (items []adItem, txPower *string) {
	for i := 0; i < len(adv); {
		l := int(adv[i])
		if l == 0 {
			break
		}
		if i+1+l > len(adv) {
			break
		}
		adType := adv[i+1]
		data := adv[i+2 : i+1+l]

		item := adItem{
			TypeHex: "0x" + strings.ToUpper(hex2(adType)),
			Name:    adTypeName(adType),
			DataHex: util.BytesToHex(data),
		}

		// Common text payloads.
		if adType == 0x08 || adType == 0x09 { // Shortened/Complete Local Name
			item.Text = safeASCII(data)
		}
		if adType == 0x0A && len(data) >= 1 { // Tx Power Level
			// signed int8
			v := int8(data[0])
			tx := fmtInt(v)
			txPower = &tx
			item.Text = tx
		}

		items = append(items, item)
		i += 1 + l
	}
	return
}

func adTypeName(t byte) string {
	switch t {
	case 0x01:
		return "Flags"
	case 0x02:
		return "Incomplete List of 16-bit Service Class UUIDs"
	case 0x03:
		return "Complete List of 16-bit Service Class UUIDs"
	case 0x06:
		return "Incomplete List of 128-bit Service Class UUIDs"
	case 0x07:
		return "Complete List of 128-bit Service Class UUIDs"
	case 0x08:
		return "Shortened Local Name"
	case 0x09:
		return "Complete Local Name"
	case 0x0A:
		return "Tx Power Level"
	case 0x16:
		return "Service Data - 16-bit UUID"
	case 0x20:
		return "Service Data - 32-bit UUID"
	case 0x21:
		return "Service Data - 128-bit UUID"
	case 0xFF:
		return "Manufacturer Specific Data"
	default:
		return ""
	}
}

func hex2(b byte) string {
	const h = "0123456789abcdef"
	return string([]byte{h[b>>4], h[b&0x0f]})
}

func safeASCII(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	for _, c := range b {
		if c < 0x20 || c > 0x7e {
			return ""
		}
	}
	return string(b)
}

func fmtInt(v int8) string {
	// minimal allocation
	if v >= 0 {
		return "+" + itoa(int(v))
	}
	return itoa(int(v))
}

func itoa(x int) string {
	// small int to string
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

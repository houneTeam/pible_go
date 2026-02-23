package bluetooth

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

// DeviceTypePatterns holds a list of tagging patterns loaded from YAML.
type DeviceTypePatterns struct {
	Types []DeviceTypePattern `yaml:"types"`
}

type DeviceTypePattern struct {
	Name               string `yaml:"name"`
	RequireServiceUUID string `yaml:"require_service_uuid"`

	IBeacon struct {
		AppleCompanyID int    `yaml:"apple_company_id"`
		UUID           string `yaml:"uuid"`
		Major          int    `yaml:"major"`
		Minor          int    `yaml:"minor"`
	} `yaml:"ibeacon"`

	Manufacturer5B struct {
		CompanyID int `yaml:"company_id"`
		Length    int `yaml:"length"`
	} `yaml:"manufacturer_5b"`

	NameBase645B bool `yaml:"name_base64_5b"`
}

// LoadDeviceTypePatterns loads patterns from:
//   <dataDir>/default/device_types.yaml
//   <customDir>/device_types.yaml  (optional override)
// If customDir is empty, it defaults to <dataDir>/custom.
func LoadDeviceTypePatterns(dataDir, customDir string) (*DeviceTypePatterns, error) {
	if strings.TrimSpace(dataDir) == "" {
		dataDir = "./data"
	}
	defPath := filepath.Join(dataDir, "default", "device_types.yaml")
	if strings.TrimSpace(customDir) == "" {
		customDir = filepath.Join(dataDir, "custom")
	}
	customPath := filepath.Join(customDir, "device_types.yaml")

	out := &DeviceTypePatterns{}
	// Load defaults if present.
	_ = loadPatternsFile(defPath, out)
	// Overlay custom if present.
	_ = loadPatternsFile(customPath, out)

	// Normalize.
	for i := range out.Types {
		out.Types[i].Name = strings.ToLower(strings.TrimSpace(out.Types[i].Name))
		out.Types[i].RequireServiceUUID = strings.ToUpper(strings.TrimSpace(out.Types[i].RequireServiceUUID))
		out.Types[i].IBeacon.UUID = strings.ToUpper(strings.TrimSpace(out.Types[i].IBeacon.UUID))
	}
	return out, nil
}

func loadPatternsFile(path string, out *DeviceTypePatterns) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var tmp DeviceTypePatterns
	if err := yaml.Unmarshal(b, &tmp); err != nil {
		return err
	}
	if len(tmp.Types) == 0 {
		return nil
	}
	// Merge by name: custom overrides default.
	idx := map[string]int{}
	for i := range out.Types {
		idx[out.Types[i].Name] = i
	}
	for _, t := range tmp.Types {
		name := strings.ToLower(strings.TrimSpace(t.Name))
		if name == "" {
			continue
		}
		t.Name = name
		if j, ok := idx[name]; ok {
			out.Types[j] = t
		} else {
			idx[name] = len(out.Types)
			out.Types = append(out.Types, t)
		}
	}
	return nil
}

var base64Re = regexp.MustCompile(`^[A-Za-z0-9+/]+={0,2}$`)

// DetectTypedDevice returns a type string when any configured pattern matches.
// Returned string is the pattern name (e.g., "cokeon").
func DetectTypedDevice(patterns *DeviceTypePatterns, serviceUUIDsRaw []string, mfg []manufacturerEntry, name string) string {
	if patterns == nil || len(patterns.Types) == 0 {
		return ""
	}

	// Build service UUID set (upper-case).
	svcSet := map[string]struct{}{}
	for _, u := range serviceUUIDsRaw {
		u = strings.ToUpper(strings.TrimSpace(u))
		if u != "" {
			svcSet[u] = struct{}{}
		}
	}

	for _, p := range patterns.Types {
		if p.Name == "" {
			continue
		}
		// Optional requirement: advertised VMINFO service UUID.
		if p.RequireServiceUUID != "" {
			if _, ok := svcSet[strings.ToUpper(p.RequireServiceUUID)]; !ok {
				continue
			}
		}

		// 1) iBeacon inside Apple manufacturer data.
		if p.IBeacon.UUID != "" && p.IBeacon.AppleCompanyID > 0 {
			payload := findManufacturerBytes(mfg, uint16(p.IBeacon.AppleCompanyID))
			if len(payload) >= 23 && payload[0] == 0x02 && payload[1] == 0x15 {
				uuid := strings.ToUpper(formatUUID(payload[2:18]))
				major := int(binary.BigEndian.Uint16(payload[18:20]))
				minor := int(binary.BigEndian.Uint16(payload[20:22]))
				if uuid == strings.ToUpper(p.IBeacon.UUID) && major == p.IBeacon.Major && minor == p.IBeacon.Minor {
					return p.Name
				}
			}
		}

		// 2) Manufacturer company ID with fixed length.
		if p.Manufacturer5B.CompanyID > 0 && p.Manufacturer5B.Length > 0 {
			payload := findManufacturerBytes(mfg, uint16(p.Manufacturer5B.CompanyID))
			if len(payload) == p.Manufacturer5B.Length {
				return p.Name
			}
		}

		// 3) Name base64 -> 5 bytes.
		if p.NameBase645B {
			n := strings.TrimSpace(name)
			if n != "" && len(n) <= 64 && base64Re.MatchString(n) {
				if raw, err := base64.StdEncoding.DecodeString(n); err == nil {
					if len(raw) == 5 {
						return p.Name
					}
				}
			}
		}
	}
	return ""
}

func findManufacturerBytes(mfg []manufacturerEntry, companyID uint16) []byte {
	for _, e := range mfg {
		if e.CompanyID != companyID {
			continue
		}
		b := parseHexBytes(e.DataHex)
		if len(b) > 0 {
			return b
		}
	}
	return nil
}

func parseHexBytes(s string) []byte {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	fields := strings.Fields(s)
	if len(fields) == 0 {
		return nil
	}
	if len(fields) == 1 {
		b, err := hex.DecodeString(strings.ReplaceAll(fields[0], " ", ""))
		if err == nil {
			return b
		}
	}
	out := make([]byte, 0, len(fields))
	for _, f := range fields {
		f = strings.TrimSpace(f)
		if f == "" {
			continue
		}
		if len(f) == 1 {
			f = "0" + f
		}
		b, err := hex.DecodeString(f)
		if err != nil || len(b) != 1 {
			return nil
		}
		out = append(out, b[0])
	}
	return out
}

func formatUUID(b []byte) string {
	if len(b) != 16 {
		return ""
	}
	// 8-4-4-4-12
	hexs := hex.EncodeToString(b)
	return strings.ToUpper(hexs[0:8] + "-" + hexs[8:12] + "-" + hexs[12:16] + "-" + hexs[16:20] + "-" + hexs[20:32])
}

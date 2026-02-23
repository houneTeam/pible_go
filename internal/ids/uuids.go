package ids

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

type uuidFile struct {
	UUIDs []uuidEntry `yaml:"uuids"`
}

type uuidEntry struct {
	UUID any    `yaml:"uuid"`
	Name string `yaml:"name"`
}

// LoadUUIDYaml loads UUID -> Name mapping from Bluetooth SIG YAML files
// (service_uuids.yaml / characteristic_uuids.yaml).
//
// Keys are returned as canonical 128-bit lower-case UUID strings.
func LoadUUIDYaml(path string) (map[string]string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var f uuidFile
	if err := yaml.Unmarshal(b, &f); err != nil {
		return nil, err
	}

	out := make(map[string]string, len(f.UUIDs))
	for _, e := range f.UUIDs {
		uuidStr := normalizeUUIDValue(e.UUID)
		name := strings.TrimSpace(e.Name)
		if uuidStr == "" || name == "" {
			continue
		}
		uuid128, err := normalizeUUID(uuidStr)
		if err != nil {
			continue
		}
		out[uuid128] = name
	}

	return out, nil
}

func normalizeUUIDValue(v any) string {
	switch t := v.(type) {
	case string:
		return strings.TrimSpace(t)
	case int:
		return fmt.Sprintf("0x%X", t)
	case int64:
		return fmt.Sprintf("0x%X", t)
	case uint64:
		return fmt.Sprintf("0x%X", t)
	case uint32:
		return fmt.Sprintf("0x%X", t)
	case uint16:
		return fmt.Sprintf("0x%X", t)
	default:
		return ""
	}
}

func normalizeUUID(s string) (string, error) {
	s = strings.ToLower(strings.TrimSpace(s))
	if s == "" {
		return "", ErrBadUUID
	}

	// YAML often uses 0x1800 or 0x2A00.
	if strings.HasPrefix(s, "0x") {
		hexStr := strings.TrimPrefix(s, "0x")
		hexStr = strings.TrimSpace(hexStr)
		if hexStr == "" {
			return "", ErrBadUUID
		}
		v, err := strconv.ParseUint(hexStr, 16, 32)
		if err != nil {
			return "", err
		}
		if len(hexStr) <= 4 {
			return fmt.Sprintf("0000%04x-0000-1000-8000-00805f9b34fb", v), nil
		}
		if len(hexStr) <= 8 {
			return fmt.Sprintf("%08x-0000-1000-8000-00805f9b34fb", v), nil
		}
		// Unexpected length.
		return "", ErrBadUUID
	}

	// Raw 16/32-bit without hyphens.
	if len(s) == 4 {
		v, err := strconv.ParseUint(s, 16, 16)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("0000%04x-0000-1000-8000-00805f9b34fb", v), nil
	}
	if len(s) == 8 {
		v, err := strconv.ParseUint(s, 16, 32)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%08x-0000-1000-8000-00805f9b34fb", v), nil
	}

	// Already 128-bit.
	if strings.Count(s, "-") == 4 {
		return s, nil
	}

	return "", ErrBadUUID
}

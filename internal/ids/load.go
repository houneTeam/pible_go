package ids

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

type LoadConfig struct {
	// DataDir is the root directory that contains default/ and custom/ subfolders.
	// Example:
	//   data/default/oui.csv
	//   data/custom/oui.csv
	DataDir string

	// CustomDir optionally overrides the custom directory path. When empty, it is
	// assumed to be <DataDir>/custom.
	CustomDir string
}

func Load(cfg LoadConfig) (*Resolver, error) {
	if cfg.DataDir == "" {
		cfg.DataDir = "data"
	}
	defaultDir := filepath.Join(cfg.DataDir, "default")
	customDir := cfg.CustomDir
	if customDir == "" {
		customDir = filepath.Join(cfg.DataDir, "custom")
	}

	res := &Resolver{
		vendors:          map[string]string{},
		serviceUUIDNames: map[string]string{},
		charUUIDNames:    map[string]string{},
	}

	// Load defaults (best-effort).
	_ = loadOUIInto(res.vendors, filepath.Join(defaultDir, "oui.csv"))
	_ = loadUUIDYamlInto(res.serviceUUIDNames, filepath.Join(defaultDir, "service_uuids.yaml"))
	_ = loadUUIDYamlInto(res.charUUIDNames, filepath.Join(defaultDir, "characteristic_uuids.yaml"))

	// Overlay custom (best-effort).
	_ = loadOUIInto(res.vendors, filepath.Join(customDir, "oui.csv"))
	_ = loadUUIDYamlInto(res.serviceUUIDNames, filepath.Join(customDir, "service_uuids.yaml"))
	_ = loadUUIDYamlInto(res.charUUIDNames, filepath.Join(customDir, "characteristic_uuids.yaml"))

	// If nothing loaded at all, return nil resolver without error.
	if len(res.vendors) == 0 && len(res.serviceUUIDNames) == 0 && len(res.charUUIDNames) == 0 {
		return nil, nil
	}

	// Validate directories existence only when user explicitly provided them.
	if cfg.CustomDir != "" {
		if _, err := os.Stat(cfg.CustomDir); err != nil {
			return res, fmt.Errorf("custom-data-dir not accessible: %w", err)
		}
	}

	return res, nil
}

func loadOUIInto(dst map[string]string, path string) error {
	if _, err := os.Stat(path); err != nil {
		return err
	}
	items, err := LoadOUI(path)
	if err != nil {
		return err
	}
	for k, v := range items {
		dst[k] = v
	}
	return nil
}

func loadUUIDYamlInto(dst map[string]string, path string) error {
	if _, err := os.Stat(path); err != nil {
		return err
	}
	items, err := LoadUUIDYaml(path)
	if err != nil {
		return err
	}
	for k, v := range items {
		// Ignore empty entries.
		if k == "" || v == "" {
			continue
		}
		dst[k] = v
	}
	return nil
}

var ErrBadUUID = errors.New("bad uuid")

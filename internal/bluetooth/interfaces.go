package bluetooth

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

type InterfaceInfo struct {
	ID          string
	DisplayName string // e.g. "hci0: Realtek Bluetooth 5.4 Radio"
	BusInfo     string // best-effort, optional
}

var (
	ifaceLineRe = regexp.MustCompile(`^(hci\d+):.*`)
	busRe       = regexp.MustCompile(`Bus:\s*(USB|UART|PCI|SDIO|Virtual)`) // best-effort
	strictHCIRe = regexp.MustCompile(`^hci\d+$`)
)

// AdapterDisplayName returns a human-friendly adapter label.
// It is primarily based on sysfs USB "manufacturer" + "product" strings.
func AdapterDisplayName(adapterID string) string {
	id := strings.TrimSpace(adapterID)
	if id == "" {
		return ""
	}
	// Safety + consistency: only accept plain controller IDs.
	// Sub-devices like "hci0:1" must be ignored in the UI and DB.
	if !strictHCIRe.MatchString(id) {
		return id
	}

	man, prod := adapterManufacturerProduct(id)
	label := strings.TrimSpace(strings.Join([]string{strings.TrimSpace(man), strings.TrimSpace(prod)}, " "))
	label = strings.Join(strings.Fields(label), " ")
	if label == "" {
		return fmt.Sprintf("%s: Unknown", id)
	}
	return fmt.Sprintf("%s: %s", id, label)
}

func adapterManufacturerProduct(adapterID string) (string, string) {
	id := strings.TrimSpace(adapterID)
	if id == "" {
		return "", ""
	}
	if !strictHCIRe.MatchString(id) {
		return "", ""
	}

	// IMPORTANT:
	// Do NOT path-clean "device/../...". In sysfs, "device" is a symlink.
	// The user's working bash command relies on *symlink-aware* path resolution:
	//   /sys/class/bluetooth/hciX/device/../(manufacturer|product)
	// If we clean it to /sys/class/bluetooth/hciX/(manufacturer|product),
	// we may lose the ability to reach the underlying USB device attributes.
	base := "/sys/class/bluetooth/" + id + "/device/../"
	man := readSysfsText(base + "manufacturer")
	prod := readSysfsText(base + "product")
	return man, prod
}

func readSysfsText(path string) string {
	// Do NOT filepath.Clean() here (see adapterManufacturerProduct).
	b, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

func GetBluetoothInterfaces() ([]InterfaceInfo, error) {
	m := map[string]InterfaceInfo{}

	// Prefer sysfs enumeration to avoid parsing hciconfig output.
	if matches, _ := filepath.Glob("/sys/class/bluetooth/hci*"); len(matches) > 0 {
		for _, p := range matches {
			id := strings.TrimSpace(filepath.Base(p))
			if id == "" {
				continue
			}
			// Filter out non-controller entries like "hci0:1" that can appear on some systems.
			if !strictHCIRe.MatchString(id) {
				continue
			}
			m[id] = InterfaceInfo{ID: id, DisplayName: AdapterDisplayName(id)}
		}
	}

	// Enrich with hciconfig bus info (best-effort) and fallback enumeration.
	cmd := exec.Command("hciconfig")
	out, err := cmd.CombinedOutput()
	if err == nil {
		lines := bytes.Split(out, []byte{'\n'})
		cur := ""
		bus := ""
		flush := func() {
			if cur == "" {
				return
			}
			if !strictHCIRe.MatchString(cur) {
				cur, bus = "", ""
				return
			}
			inf := m[cur]
			inf.ID = cur
			inf.BusInfo = bus
			if strings.TrimSpace(inf.DisplayName) == "" {
				inf.DisplayName = AdapterDisplayName(cur)
			}
			// If sysfs did not provide manufacturer/product, fall back to bus info.
			if strings.HasSuffix(inf.DisplayName, ": Unknown") && strings.TrimSpace(bus) != "" && strings.TrimSpace(bus) != "Unknown" {
				inf.DisplayName = fmt.Sprintf("%s: %s", cur, strings.TrimSpace(bus))
			}
			m[cur] = inf
			cur, bus = "", ""
		}

		for _, raw := range lines {
			line := string(bytes.TrimRight(raw, "\r"))
			lineTrim := strings.TrimSpace(line)
			if lineTrim == "" {
				flush()
				continue
			}

			if mm := ifaceLineRe.FindStringSubmatch(lineTrim); mm != nil {
				flush()
				cur = mm[1]
				if bm := busRe.FindStringSubmatch(lineTrim); bm != nil {
					bus = bm[1]
				}
				continue
			}

			if cur != "" && bus == "" {
				if bm := busRe.FindStringSubmatch(lineTrim); bm != nil {
					bus = bm[1]
				}
			}
		}
		flush()
	}

	outList := make([]InterfaceInfo, 0, len(m))
	for _, inf := range m {
		if strings.TrimSpace(inf.DisplayName) == "" {
			inf.DisplayName = AdapterDisplayName(inf.ID)
		}
		// Ensure only real controllers are returned.
		if !strictHCIRe.MatchString(strings.TrimSpace(inf.ID)) {
			continue
		}
		outList = append(outList, inf)
	}

	sort.Slice(outList, func(i, j int) bool {
		ai := hciIndex(outList[i].ID)
		aj := hciIndex(outList[j].ID)
		if ai != aj {
			return ai < aj
		}
		return outList[i].ID < outList[j].ID
	})

	return outList, nil
}

func hciIndex(id string) int {
	id = strings.TrimSpace(id)
	if !strings.HasPrefix(id, "hci") {
		return 1 << 30
	}
	n := strings.TrimPrefix(id, "hci")
	i, err := strconv.Atoi(n)
	if err != nil {
		return 1 << 30
	}
	return i
}

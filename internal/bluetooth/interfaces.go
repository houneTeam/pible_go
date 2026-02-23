package bluetooth

import (
	"bytes"
	"os/exec"
	"regexp"
	"strings"
)

type InterfaceInfo struct {
	ID      string
	BusInfo string
}

var (
	ifaceLineRe = regexp.MustCompile(`^(hci\d+):.*`)
	busRe       = regexp.MustCompile(`Bus:\s*(USB|UART|PCI|SDIO|Virtual)`) // best-effort
)

func GetBluetoothInterfaces() ([]InterfaceInfo, error) {
	cmd := exec.Command("hciconfig")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, err
	}

	lines := bytes.Split(out, []byte{'\n'})
	var outList []InterfaceInfo

	var cur string
	var bus string

	flush := func() {
		if cur == "" {
			return
		}
		if bus == "" {
			bus = "Unknown"
		}
		outList = append(outList, InterfaceInfo{ID: cur, BusInfo: bus})
		cur, bus = "", ""
	}

	for _, raw := range lines {
		line := string(bytes.TrimRight(raw, "\r"))
		lineTrim := strings.TrimSpace(line)
		if lineTrim == "" {
			flush()
			continue
		}

		if m := ifaceLineRe.FindStringSubmatch(lineTrim); m != nil {
			flush()
			cur = m[1]
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

	return outList, nil
}

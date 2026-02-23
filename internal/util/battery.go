package util

import (
	"os/exec"
	"regexp"
	"strings"
)

var batteryPctRe = regexp.MustCompile(`(\d{1,3})%`)

// BatteryPercent returns battery percent using `acpi -b` (Linux).
// Returns empty string when not available.
func BatteryPercent() string {
	out, err := exec.Command("acpi", "-b").CombinedOutput()
	if err != nil {
		return ""
	}
	s := string(out)
	m := batteryPctRe.FindStringSubmatch(s)
	if m == nil {
		return ""
	}
	return strings.TrimSpace(m[1]) + "%"
}

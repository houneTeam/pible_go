package gps

import (
	"os"
	"path/filepath"

	"go.bug.st/serial"
	"go.bug.st/serial/enumerator"
)

// ListSerialPorts returns the list of serial device paths.
// On Linux, USB GPS adapters typically appear as /dev/ttyUSB* or /dev/ttyACM*.
func ListSerialPorts() ([]string, error) {
	// Prefer detailed enumeration when available.
	detailed, err := enumerator.GetDetailedPortsList()
	if err == nil && len(detailed) > 0 {
		out := make([]string, 0, len(detailed))
		for _, p := range detailed {
			out = append(out, p.Name)
		}
		return out, nil
	}

	ports, err2 := serial.GetPortsList()
	if err2 != nil {
		if err != nil {
			return nil, err
		}
		return nil, err2
	}
	return ports, nil
}

// GuessSerialDevice attempts to find a likely GPS serial device.
// Returns empty string if nothing is detected.
func GuessSerialDevice() string {
	// Prefer stable symlinks.
	if matches, _ := filepath.Glob("/dev/serial/by-id/*"); len(matches) > 0 {
		return matches[0]
	}

	if ports, _ := ListSerialPorts(); len(ports) > 0 {
		return ports[0]
	}

	candidates := []string{"/dev/ttyACM0", "/dev/ttyUSB0", "/dev/ttyAMA0"}
	for _, c := range candidates {
		if _, err := os.Stat(c); err == nil {
			return c
		}
	}
	return ""
}

package gps

import (
	"bufio"
	"context"
	"errors"
	"log"
	"strings"
	"time"

	nmea "github.com/adrianmo/go-nmea"
	"go.bug.st/serial"

	"pible/internal/util"
)

func (s *State) runSerialLoop(ctx context.Context, dev string, baud int) {
	connected := false
	devPath := strings.TrimSpace(dev)
	if devPath == "" {
		devPath = GuessSerialDevice()
	}
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if !connected {
			util.Linef("[GPS]", util.ColorGray, "opening serial %s (%d baud)", devPath, baud)
			log.Printf("gps: opening serial %s (%d baud)", devPath, baud)
		}
		connected = true
		if err := s.readSerial(ctx, devPath, baud); err != nil {
			connected = false
			util.Linef("[GPS]", util.ColorYellow, "serial disconnected: %v", err)
			log.Printf("gps: serial disconnected: %v", err)

			// Hot-plug support: if the device path disappears or changes, try to re-detect.
			if guessed := GuessSerialDevice(); guessed != "" && guessed != devPath {
				util.Linef("[GPS]", util.ColorGray, "serial device changed -> %s", guessed)
				log.Printf("gps: serial device changed -> %s", guessed)
				devPath = guessed
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
			}
		}
	}
}

func (s *State) readSerial(ctx context.Context, dev string, baud int) error {
	mode := &serial.Mode{BaudRate: baud}
	port, err := serial.Open(dev, mode)
	if err != nil {
		return err
	}
	defer port.Close()

	s.setActiveCloser("serial", func() {
		_ = port.Close()
	})
	defer s.clearActiveCloser()

	// Ensure we can break out of blocking reads when ctx is canceled.
	go func() {
		<-ctx.Done()
		_ = port.Close()
	}()

	scanner := bufio.NewScanner(port)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 256*1024)

	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		line := strings.TrimSpace(scanner.Text())
		line = strings.TrimRight(line, "\r")
		if line == "" {
			continue
		}
		if !strings.HasPrefix(line, "$") && !strings.HasPrefix(line, "!") {
			// Not NMEA/AIS.
			continue
		}
		s.updatePacket()

		sent, err := nmea.Parse(line)
		if err != nil {
			continue
		}

		switch v := sent.(type) {
		case nmea.RMC:
			if strings.EqualFold(v.Validity, "A") {
				s.updateFix(v.Latitude, v.Longitude)
			}
		case nmea.GGA:
			// FixQuality: "0" means invalid.
			if v.FixQuality != "0" && (v.Latitude != 0 || v.Longitude != 0) {
				s.updateFix(v.Latitude, v.Longitude)
			}
		case nmea.GLL:
			if strings.EqualFold(v.Validity, "A") {
				s.updateFix(v.Latitude, v.Longitude)
			}
		case nmea.GNS:
			if v.Latitude != 0 || v.Longitude != 0 {
				s.updateFix(v.Latitude, v.Longitude)
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return err
	}
	return errors.New("serial reader stopped")
}

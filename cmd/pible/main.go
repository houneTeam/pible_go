package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"pible/internal/bluetooth"
	"pible/internal/db"
	"pible/internal/gps"
	"pible/internal/ids"
	"pible/internal/status"
	"pible/internal/util"
)

func main() {
	var (
		useGPSFlag      = flag.String("use-gps", "", "Use GPS? 'y' to enable, 'n' to skip.")
		gpsModeFlag     = flag.String("gps-mode", "auto", "GPS mode: auto|gpsd|serial|off")
		gpsdAddrFlag    = flag.String("gpsd-addr", "127.0.0.1:2947", "gpsd TCP address")
		gpsDeviceFlag   = flag.String("gps-device", "", "GPS serial device path (e.g., /dev/ttyUSB0)")
		gpsBaudFlag     = flag.Int("gps-baud", 9600, "GPS serial baud rate")
		dataDirFlag     = flag.String("data-dir", "./data", "Data directory root (expects default/ and custom/ subfolders)")
		customDataFlag  = flag.String("custom-data-dir", "", "Optional custom data directory path (overrides <data-dir>/custom)")
		adaptersFlag    = flag.String("adapters", "", "Comma-separated list of Bluetooth adapters to use (e.g., hci0,hci1). If empty, interactive selection is used.")
		adapterIndexFlg = flag.Int("adapter-index", -1, "Index of the Bluetooth adapter to use.")
		restartBlueZSvc = flag.Bool("restart-bluetooth", true, "Preflight: restart bluetooth service if adapters are missing (requires root + systemctl)")
		bluezCacheMode  = flag.String("bluez-cache", "auto", "Preflight: BlueZ device cache cleanup mode: auto|off|force")
		statsInterval   = flag.Int("stats-interval", 5, "Console status interval in seconds")
	)
	flag.Parse()

	logFile, err := os.OpenFile("app.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err == nil {
		log.SetOutput(logFile)
		defer logFile.Close()
	}
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	printLogo()

	ctx, cancel := signalContext(context.Background())
	defer cancel()

	store, err := db.Open("bluetooth_devices.db")
	if err != nil {
		util.Linef("[ERROR]", util.ColorYellow, "failed to open database: %v", err)
		os.Exit(1)
	}
	defer store.Close()

	resolver, err := ids.Load(ids.LoadConfig{DataDir: strings.TrimSpace(*dataDirFlag), CustomDir: strings.TrimSpace(*customDataFlag)})
	if err != nil {
		util.Linef("[ERROR]", util.ColorYellow, "failed to load data files: %v", err)
		os.Exit(1)
	}

	// Load device type detection patterns (optional).
	patterns, perr := bluetooth.LoadDeviceTypePatterns(strings.TrimSpace(*dataDirFlag), strings.TrimSpace(*customDataFlag))
	if perr != nil {
		// Non-fatal: scanning still works without type detection.
		patterns = nil
		util.Linef("[WARN]", util.ColorYellow, "failed to load device type patterns: %v", perr)
	}

	// GPS selection.
	useGPS := false
	mode := strings.ToLower(strings.TrimSpace(*gpsModeFlag))
	if mode == "off" {
		useGPS = false
	} else if *useGPSFlag == "" {
		s, err := util.PromptString("Use GPS? (y/n): ")
		if err == nil {
			useGPS = (s == "y" || s == "Y")
		}
	} else {
		useGPS = (*useGPSFlag == "y" || *useGPSFlag == "Y")
	}

	tagInput, _ := util.PromptString("Enter a tag to use for new devices (leave blank if none): ")

	gpsState := gps.NewState(useGPS, 300*time.Second)
	defer gpsState.Stop()
	if useGPS {
		cfg := gps.Config{
			Mode:       mode,
			GPSDAddr:   strings.TrimSpace(*gpsdAddrFlag),
			SerialDev:  strings.TrimSpace(*gpsDeviceFlag),
			SerialBaud: *gpsBaudFlag,
		}

		// If user didn't specify gps-mode explicitly (default "auto"), keep the interactive flow.
		if strings.TrimSpace(*gpsModeFlag) == "auto" {
			choice, _ := util.PromptString("GPS source (auto/gpsd/serial) [auto]: ")
			choice = strings.ToLower(strings.TrimSpace(choice))
			if choice != "" {
				cfg.Mode = choice
			}
		}

		if cfg.Mode == "serial" {
			if cfg.SerialDev == "" {
				ports, _ := gps.ListSerialPorts()
				if len(ports) > 0 {
					fmt.Println("Available serial ports:")
					for i, p := range ports {
						fmt.Printf("%d: %s\n", i, p)
					}
					idx, _ := util.PromptInt("Select the serial port to use (enter the number): ", 0)
					if idx >= 0 && idx < len(ports) {
						cfg.SerialDev = ports[idx]
					}
				}
				if cfg.SerialDev == "" {
					p, _ := util.PromptString("Enter GPS serial device path (e.g., /dev/ttyUSB0): ")
					cfg.SerialDev = strings.TrimSpace(p)
				}
			}
			if cfg.SerialBaud <= 0 {
				cfg.SerialBaud = 9600
			}
			b, _ := util.PromptInt(fmt.Sprintf("Enter baud rate [%d]: ", cfg.SerialBaud), cfg.SerialBaud)
			if b > 0 {
				cfg.SerialBaud = b
			}
		}

		if err := gpsState.Start(ctx, cfg); err != nil {
			util.Linef("[ERROR]", util.ColorYellow, "failed to start GPS reader: %v", err)
			os.Exit(1)
		}
		// Do not block scanning waiting for GPS: it will appear in periodic status output.
		util.Line("[GPS]", util.ColorGray, "GPS reader started")
		// Preflight: verify we receive packets; if not, try to kick gpsd (best-effort).
		if !gpsState.WaitForFirstPacket(ctx, 3*time.Second) {
			util.Line("[GPS]", util.ColorYellow, "no packets yet (will keep retrying; using last known if available)")
			if util.IsRoot() && util.HasSystemctl() && cfg.Mode != "serial" {
				util.Line("[PREFLIGHT]", util.ColorGray, "restarting gpsd")
				_ = util.RestartService(ctx, "gpsd")
			}
		}
	}

	interfaces, err := bluetooth.GetBluetoothInterfaces()
	if err != nil {
		util.Linef("[ERROR]", util.ColorYellow, "failed to get Bluetooth interfaces: %v", err)
		os.Exit(1)
	}
	if len(interfaces) == 0 {
		fmt.Println("No Bluetooth interfaces found.")
		os.Exit(1)
	}

	chosenAdapters, err := selectAdapters(interfaces, strings.TrimSpace(*adaptersFlag), *adapterIndexFlg)
	if err != nil {
		util.Linef("[ERROR]", util.ColorYellow, "%v", err)
		os.Exit(1)
	}
	if len(chosenAdapters) == 0 {
		util.Line("[ERROR]", util.ColorYellow, "no adapters selected")
		os.Exit(1)
	}
	adaptersJoined := strings.Join(chosenAdapters, ",")

	// Preflight: ensure adapters are visible to BlueZ and optionally clear runtime cache.
	cache := strings.ToLower(strings.TrimSpace(*bluezCacheMode))
	cacheMode := bluetooth.BlueZCacheAuto
	switch cache {
	case "off":
		cacheMode = bluetooth.BlueZCacheOff
	case "force":
		cacheMode = bluetooth.BlueZCacheForce
	case "auto", "":
		cacheMode = bluetooth.BlueZCacheAuto
	default:
		cacheMode = bluetooth.BlueZCacheAuto
	}
	bluetooth.PreflightBlueZ(ctx, chosenAdapters, bluetooth.PreflightOptions{
		RestartBluetoothService: *restartBlueZSvc,
		CacheMode:               cacheMode,
	})

	maxConn, _ := util.PromptInt("Set the limit on the number of simultaneous connections: ", 5)
	if maxConn < 1 {
		maxConn = 1
	}

	var tagPtr *string
	if strings.TrimSpace(tagInput) != "" {
		t := strings.TrimSpace(tagInput)
		tagPtr = &t
	}

	// Create scanning session id.
	var gpsStart *string
	if s := gpsState.GPSStringForRecord(); s != nil {
		gpsStart = s
	}
	sessionID, err := store.CreateSession(ctx, adaptersJoined, tagPtr, gpsStart)
	if err != nil {
		util.Linef("[ERROR]", util.ColorYellow, "failed to create scan session: %v", err)
		os.Exit(1)
	}
	util.Linef("[SESSION]", util.ColorGray, "id=%d adapters=%s", sessionID, adaptersJoined)

	// Periodic status (GPS/DB/Battery).
	go status.Run(ctx, time.Duration(*statsInterval)*time.Second, status.Provider{GPS: gpsState, Store: store})

	if err := bluetooth.StartContinuousScanAndConnectMulti(ctx, chosenAdapters, store, gpsState, resolver, patterns, sessionID, maxConn, tagPtr); err != nil {
		if ctx.Err() != nil {
			util.Line("[EXIT]", util.ColorGray, "stopping")
			return
		}
		fmt.Printf("[ERROR] Fatal: %v\n", err)
		os.Exit(1)
	}
}

func selectAdapters(interfaces []bluetooth.InterfaceInfo, adaptersFlag string, adapterIndex int) ([]string, error) {
	// If explicit adapter list provided (e.g. hci0,hci1), validate it.
	if adaptersFlag != "" {
		parts := splitCSV(adaptersFlag)
		if len(parts) == 0 {
			return nil, fmt.Errorf("-adapters is empty")
		}
		valid := make(map[string]bool, len(interfaces))
		for _, inf := range interfaces {
			valid[inf.ID] = true
		}
		out := make([]string, 0, len(parts))
		for _, p := range parts {
			if !valid[p] {
				return nil, fmt.Errorf("unknown adapter in -adapters: %s", p)
			}
			out = append(out, p)
		}
		return uniqueStrings(out), nil
	}

	// Legacy single index flag.
	if adapterIndex >= 0 {
		if adapterIndex >= len(interfaces) {
			return nil, fmt.Errorf("adapter-index out of range")
		}
		return []string{interfaces[adapterIndex].ID}, nil
	}

	// Interactive: allow a single index or multiple indices separated by commas.
	fmt.Println("Available Bluetooth interfaces:")
	for i, inf := range interfaces {
		fmt.Printf("%d: %s (%s)\n", i, inf.ID, inf.BusInfo)
	}
	s, err := util.PromptString("Select the interface(s) to use (e.g. 0 or 0,1): ")
	if err != nil {
		return nil, fmt.Errorf("invalid selection: %w", err)
	}
	s = strings.TrimSpace(s)
	if s == "" {
		return []string{interfaces[0].ID}, nil
	}
	idxStrs := splitCSV(s)
	if len(idxStrs) == 0 {
		return nil, fmt.Errorf("invalid selection")
	}
	out := make([]string, 0, len(idxStrs))
	for _, v := range idxStrs {
		var idx int
		if _, err := fmt.Sscanf(v, "%d", &idx); err != nil {
			return nil, fmt.Errorf("invalid adapter index: %s", v)
		}
		if idx < 0 || idx >= len(interfaces) {
			return nil, fmt.Errorf("adapter index out of range: %d", idx)
		}
		out = append(out, interfaces[idx].ID)
	}
	return uniqueStrings(out), nil
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		out = append(out, p)
	}
	return out
}

func uniqueStrings(in []string) []string {
	seen := make(map[string]bool, len(in))
	out := make([]string, 0, len(in))
	for _, v := range in {
		if seen[v] {
			continue
		}
		seen[v] = true
		out = append(out, v)
	}
	return out
}

func signalContext(parent context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	ch := make(chan os.Signal, 2)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	go func() {
		select {
		case <-ch:
			cancel()
		case <-ctx.Done():
		}
		// Drain second signal to avoid goroutine leaks in some shells.
		select {
		case <-ch:
		default:
		}
	}()
	return ctx, cancel
}

func printLogo() {
	logo := `
    _/_/_/    _/  _/_/_/    _/        _/_/_/_/
   _/    _/      _/    _/  _/        _/
  _/_/_/    _/  _/_/_/    _/        _/_/_/
 _/        _/  _/    _/  _/        _/
_/        _/  _/_/_/    _/_/_/_/  _/_/_/_/
`
	fmt.Println(logo)
	fmt.Println("HouneTeam - PiBLE (Go port)")
}

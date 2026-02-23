package bluetooth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/godbus/dbus/v5"

	"pible/internal/db"
	"pible/internal/ids"
	"pible/internal/util"
)

// DumpAndStoreGATT reads as much as possible from BlueZ GATT objects:
// - services
// - characteristics (UUID, handle, flags, readable values)
// - descriptors (UUID, handle, flags, readable values)
// It returns a human-readable text dump and the best-effort device name.
func DumpAndStoreGATT(
	ctx context.Context,
	conn *dbus.Conn,
	adapterID string,
	devPath dbus.ObjectPath,
	mac string,
	store *db.Store,
	resolver *ids.Resolver,
) (string, string, error) {
	root := conn.Object("org.bluez", dbus.ObjectPath("/"))
	call := root.CallWithContext(ctx, "org.freedesktop.DBus.ObjectManager.GetManagedObjects", 0)
	if call.Err != nil {
		return "", "", call.Err
	}
	var managed map[dbus.ObjectPath]map[string]map[string]dbus.Variant
	if err := call.Store(&managed); err != nil {
		return "", "", err
	}

	// Best-effort name.
	name := "Unknown"
	if ifaces, ok := managed[devPath]; ok {
		if dev1, ok := ifaces["org.bluez.Device1"]; ok {
			if s, ok := getString(dev1, "Alias"); ok {
				s = strings.TrimSpace(s)
				if s != "" {
					name = s
				}
			} else if s, ok := getString(dev1, "Name"); ok {
				s = strings.TrimSpace(s)
				if s != "" {
					name = s
				}
			}
		}
	}

	devPrefix := string(devPath) + "/"

	type svcItem struct {
		path   dbus.ObjectPath
		uuid   string
		handle *uint16
	}
	services := make([]svcItem, 0, 64)
	for path, ifaces := range managed {
		p := string(path)
		if !strings.HasPrefix(p, devPrefix) {
			continue
		}
		svc, ok := ifaces["org.bluez.GattService1"]
		if !ok {
			continue
		}
		uuid, _ := getString(svc, "UUID")
		uuid = strings.TrimSpace(uuid)
		if uuid == "" {
			continue
		}
		h := getUint16Ptr(svc, "Handle")
		services = append(services, svcItem{path: path, uuid: uuid, handle: h})
	}
	if len(services) == 0 {
		return "", name, errors.New("no GATT services")
	}
	sort.Slice(services, func(i, j int) bool { return string(services[i].path) < string(services[j].path) })

	lines := make([]string, 0, 512)
	now := util.NowTimestamp()

	// Limits to prevent pathological devices from stalling the worker.
	// Values are tuned for "lots of devices" environments.
	const maxCharsToRead = 40
	const perReadTimeout = 900 * time.Millisecond
	readCount := 0

	for _, s := range services {
		svcUUIDAnn := s.uuid
		if resolver != nil {
			svcUUIDAnn = resolver.AnnotateServiceUUID(svcUUIDAnn)
		}
		lines = append(lines, fmt.Sprintf("Service: %s", svcUUIDAnn))

		// Collect characteristics under this service.
		type chItem struct {
			path   dbus.ObjectPath
			uuid   string
			handle *uint16
			flags  []string
		}
		chars := make([]chItem, 0, 32)
		for path, ifaces := range managed {
			p := string(path)
			if !strings.HasPrefix(p, string(s.path)+"/") {
				continue
			}
			ch, ok := ifaces["org.bluez.GattCharacteristic1"]
			if !ok {
				continue
			}
			uuid, _ := getString(ch, "UUID")
			uuid = strings.TrimSpace(uuid)
			if uuid == "" {
				continue
			}
			h := getUint16Ptr(ch, "Handle")
			flags := getStringSlice(ch, "Flags")
			chars = append(chars, chItem{path: path, uuid: uuid, handle: h, flags: flags})
		}
		sort.Slice(chars, func(i, j int) bool { return string(chars[i].path) < string(chars[j].path) })

		for _, c := range chars {
			cuAnn := c.uuid
			if resolver != nil {
				cuAnn = resolver.AnnotateCharacteristicUUID(cuAnn)
			}
			lines = append(lines, fmt.Sprintf("  ├─ Characteristic: %s", cuAnn))

			flagsStr := ""
			if len(c.flags) > 0 {
				flagsStr = strings.Join(c.flags, ", ")
			}
			if flagsStr == "" {
				flagsStr = "(unknown)"
			}
			lines = append(lines, fmt.Sprintf("  │  Properties: %s", flagsStr))

			// Read value when allowed and within limits.
			var valHex, valASCII, readErrStr *string
			if readCount < maxCharsToRead && hasFlag(c.flags, "read") {
				readCount++
				v, rerr := readCharacteristic(ctx, conn, c.path, perReadTimeout)
				if rerr != nil {
					e := rerr.Error()
					readErrStr = &e
					lines = append(lines, fmt.Sprintf("  │  Read error: %v", rerr))
				} else {
					h := util.BytesToHex(v)
					valHex = &h
					lines = append(lines, fmt.Sprintf("  │  Value(hex): %s", h))
					if s := asciiIfPrintable(v); s != "" {
						valASCII = &s
						lines = append(lines, fmt.Sprintf("  │  Value(ascii): %s", s))
					}
				}
			} else if readCount >= maxCharsToRead && hasFlag(c.flags, "read") {
				lines = append(lines, "  │  Value: (skipped; read limit reached)")
			}

			// Persist characteristic.
			if store != nil {
				flagsJSON := sliceToJSON(c.flags)
				_ = store.UpsertGattCharacteristic(ctx, db.GattCharacteristicParams{
					MAC:           mac,
					ServiceUUID:   s.uuid,
					ServiceHandle: s.handle,
					CharUUID:      c.uuid,
					CharHandle:    c.handle,
					FlagsJSON:     flagsJSON,
					ValueHex:      valHex,
					ValueASCII:    valASCII,
					ReadError:     readErrStr,
					LastReadAt:    now,
				})
			}

			// Descriptors for this characteristic.
			type dItem struct {
				path   dbus.ObjectPath
				uuid   string
				handle *uint16
				flags  []string
			}
			desc := make([]dItem, 0, 8)
			for path, ifaces := range managed {
				p := string(path)
				if !strings.HasPrefix(p, string(c.path)+"/") {
					continue
				}
				di, ok := ifaces["org.bluez.GattDescriptor1"]
				if !ok {
					continue
				}
				uuid, _ := getString(di, "UUID")
				uuid = strings.TrimSpace(uuid)
				if uuid == "" {
					continue
				}
				h := getUint16Ptr(di, "Handle")
				flags := getStringSlice(di, "Flags")
				desc = append(desc, dItem{path: path, uuid: uuid, handle: h, flags: flags})
			}
			sort.Slice(desc, func(i, j int) bool { return string(desc[i].path) < string(desc[j].path) })

			for _, d := range desc {
				lines = append(lines, fmt.Sprintf("  │  Descriptor: %s", d.uuid))
				flagsStr := ""
				if len(d.flags) > 0 {
					flagsStr = strings.Join(d.flags, ", ")
				}
				if flagsStr == "" {
					flagsStr = "(unknown)"
				}
				lines = append(lines, fmt.Sprintf("  │    Properties: %s", flagsStr))

				var vHex, vASCII, rErr *string
				if hasFlag(d.flags, "read") {
					v, rerr := readDescriptor(ctx, conn, d.path, perReadTimeout)
					if rerr != nil {
						e := rerr.Error()
						rErr = &e
						lines = append(lines, fmt.Sprintf("  │    Read error: %v", rerr))
					} else {
						h := util.BytesToHex(v)
						vHex = &h
						lines = append(lines, fmt.Sprintf("  │    Value(hex): %s", h))
						if s := asciiIfPrintable(v); s != "" {
							vASCII = &s
							lines = append(lines, fmt.Sprintf("  │    Value(ascii): %s", s))
						}
					}
				}

				if store != nil {
					flagsJSON := sliceToJSON(d.flags)
					_ = store.UpsertGattDescriptor(ctx, db.GattDescriptorParams{
						MAC:         mac,
						ServiceUUID: s.uuid,
						CharUUID:    c.uuid,
						DescUUID:    d.uuid,
						DescHandle:  d.handle,
						FlagsJSON:   flagsJSON,
						ValueHex:    vHex,
						ValueASCII:  vASCII,
						ReadError:   rErr,
						LastReadAt:  now,
					})
				}
			}

			lines = append(lines, "  └─────────────────────────────────")
		}
	}

	return strings.Join(lines, "\n"), name, nil
}

func getUint16Ptr(props map[string]dbus.Variant, key string) *uint16 {
	v, ok := props[key]
	if !ok {
		return nil
	}
	sw := v.Value()
	switch x := sw.(type) {
	case uint16:
		vv := x
		return &vv
	case uint32:
		vv := uint16(x)
		return &vv
	case int32:
		if x < 0 {
			return nil
		}
		vv := uint16(x)
		return &vv
	default:
		return nil
	}
}

func getStringSlice(props map[string]dbus.Variant, key string) []string {
	v, ok := props[key]
	if !ok {
		return nil
	}
	sw := v.Value()
	switch x := sw.(type) {
	case []string:
		out := make([]string, 0, len(x))
		for _, s := range x {
			s = strings.TrimSpace(s)
			if s != "" {
				out = append(out, s)
			}
		}
		return out
	default:
		return nil
	}
}

func sliceToJSON(in []string) *string {
	if len(in) == 0 {
		s := "[]"
		return &s
	}
	b, err := json.Marshal(in)
	if err != nil {
		s := "[]"
		return &s
	}
	s := string(b)
	return &s
}

func hasFlag(flags []string, want string) bool {
	want = strings.ToLower(strings.TrimSpace(want))
	if want == "" {
		return false
	}
	for _, f := range flags {
		if strings.ToLower(strings.TrimSpace(f)) == want {
			return true
		}
	}
	return false
}

func readCharacteristic(ctx context.Context, conn *dbus.Conn, chPath dbus.ObjectPath, timeout time.Duration) ([]byte, error) {
	obj := conn.Object("org.bluez", chPath)
	readCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	call := obj.CallWithContext(readCtx, "org.bluez.GattCharacteristic1.ReadValue", 0, map[string]dbus.Variant{})
	if call.Err != nil {
		return nil, call.Err
	}
	var out []byte
	if err := call.Store(&out); err != nil {
		return nil, err
	}
	return out, nil
}

func readDescriptor(ctx context.Context, conn *dbus.Conn, dPath dbus.ObjectPath, timeout time.Duration) ([]byte, error) {
	obj := conn.Object("org.bluez", dPath)
	readCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	call := obj.CallWithContext(readCtx, "org.bluez.GattDescriptor1.ReadValue", 0, map[string]dbus.Variant{})
	if call.Err != nil {
		return nil, call.Err
	}
	var out []byte
	if err := call.Store(&out); err != nil {
		return nil, err
	}
	return out, nil
}

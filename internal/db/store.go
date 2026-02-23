package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

type Store struct {
	mu sync.Mutex
	db *sql.DB

	// gpsHistLast caches the last gps_text written to device_gps_history per MAC.
	// This avoids a SELECT on every device observation.
	gpsHistLast   map[string]string
	gpsHistLastAt map[string]time.Time
}

func Open(dbPath string) (*Store, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, err
	}
	// Foreign keys are disabled by default in SQLite; enable per-connection.
	// Best-effort (won't fail open if unsupported by build).
	_, _ = db.Exec(`PRAGMA foreign_keys = ON;`)
	// SQLite is effectively single-writer; keep one connection to avoid SQLITE_BUSY
	// when concurrent goroutines do writes.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)

	s := &Store{db: db, gpsHistLast: map[string]string{}, gpsHistLastAt: map[string]time.Time{}}
	if err := s.Initialize(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) Initialize(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Create tables (new DB).
	_, err := s.db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS devices (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	session_id INTEGER,
	device_type TEXT,
	name TEXT,
	mac TEXT UNIQUE COLLATE NOCASE,
	mac_type TEXT,
	mac_subtype TEXT,
	rssi INTEGER,
	service TEXT,
	timestamp TEXT,
	adapter TEXT,
	manufacturer_data TEXT,
	manufacturer_name TEXT,
	service_uuids TEXT,
	service_data TEXT,
	tx_power TEXT,
	platform_data TEXT,
	advertisement_json TEXT,
	last_adv_id INTEGER,
	gps TEXT,
	detection_count INTEGER DEFAULT 1,
	last_count_update TEXT,
	tag TEXT,
	type TEXT
);
`)
	if err != nil {
		return err
	}

	// Backward-compatible schema updates for old DBs.
	_ = execIgnore(s.db, ctx, `ALTER TABLE devices ADD COLUMN service TEXT`)
	_ = execIgnore(s.db, ctx, `ALTER TABLE devices ADD COLUMN session_id INTEGER`)
	_ = execIgnore(s.db, ctx, `ALTER TABLE devices ADD COLUMN device_type TEXT`)
	_ = execIgnore(s.db, ctx, `ALTER TABLE devices ADD COLUMN manufacturer_name TEXT`)
	_ = execIgnore(s.db, ctx, `ALTER TABLE devices ADD COLUMN advertisement_json TEXT`)
	_ = execIgnore(s.db, ctx, `ALTER TABLE devices ADD COLUMN last_adv_id INTEGER`)
	_ = execIgnore(s.db, ctx, `ALTER TABLE devices ADD COLUMN mac_type TEXT`)
	_ = execIgnore(s.db, ctx, `ALTER TABLE devices ADD COLUMN mac_subtype TEXT`)
	_ = execIgnore(s.db, ctx, `ALTER TABLE devices ADD COLUMN last_count_update TEXT`)
	_ = execIgnore(s.db, ctx, `ALTER TABLE devices ADD COLUMN tag TEXT`)
	_ = execIgnore(s.db, ctx, `ALTER TABLE devices ADD COLUMN type TEXT`)

	// Migration for older schemas (DROP COLUMN is not guaranteed to be supported).
	if err := s.migrateDevicesTableIfNeeded(ctx); err != nil {
		return err
	}

	// Classic Bluetooth supplemental info (BR/EDR).
	_, err = s.db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS classic_devices (
	mac TEXT PRIMARY KEY,
	class INTEGER,
	icon TEXT,
	paired INTEGER,
	trusted INTEGER,
	connected INTEGER,
	blocked INTEGER,
	legacy_pairing INTEGER,
	modalias TEXT,
	uuids TEXT,
	last_seen TEXT,
	props_json TEXT
);
`)
	if err != nil {
		return err
	}

	_, err = s.db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS classic_discoveries (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	session_id INTEGER,
	mac TEXT,
	timestamp TEXT,
	rssi INTEGER,
	class INTEGER,
	props_json TEXT
);
`)
	if err != nil {
		return err
	}

	_, err = s.db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS gatt_services (
	mac TEXT PRIMARY KEY,
	service TEXT
);
`)
	if err != nil {
		return err
	}

	_, err = s.db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS gatt_characteristics (
	mac TEXT,
	service_uuid TEXT,
	service_handle INTEGER,
	char_uuid TEXT,
	char_handle INTEGER,
	flags_json TEXT,
	value_hex TEXT,
	value_ascii TEXT,
	read_error TEXT,
	last_read_at TEXT,
	PRIMARY KEY (mac, service_uuid, char_uuid)
);
`)
	if err != nil {
		return err
	}
	_ = execIgnore(s.db, ctx, `CREATE INDEX IF NOT EXISTS idx_gatt_chars_mac ON gatt_characteristics(mac)`)

	_, err = s.db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS gatt_descriptors (
	mac TEXT,
	service_uuid TEXT,
	char_uuid TEXT,
	desc_uuid TEXT,
	desc_handle INTEGER,
	flags_json TEXT,
	value_hex TEXT,
	value_ascii TEXT,
	read_error TEXT,
	last_read_at TEXT,
	PRIMARY KEY (mac, service_uuid, char_uuid, desc_uuid)
);
`)
	if err != nil {
		return err
	}
	_ = execIgnore(s.db, ctx, `CREATE INDEX IF NOT EXISTS idx_gatt_desc_mac ON gatt_descriptors(mac)`)

	_, err = s.db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS scan_sessions (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	started_at TEXT,
	adapter TEXT,
	tag TEXT,
	gps_start TEXT
);
`)
	if err != nil {
		return err
	}

	_, err = s.db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS advertisements (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	session_id INTEGER,
	device_id INTEGER,
	mac TEXT,
	timestamp TEXT,
	rssi INTEGER,
	adv_raw TEXT,
	adv_json TEXT,
	FOREIGN KEY(device_id) REFERENCES devices(id) ON DELETE CASCADE
);
`)
	if err != nil {
		return err
	}
	_ = execIgnore(s.db, ctx, `CREATE INDEX IF NOT EXISTS idx_advertisements_device_id ON advertisements(device_id)`)
	_ = execIgnore(s.db, ctx, `CREATE INDEX IF NOT EXISTS idx_advertisements_mac ON advertisements(mac)`)

	if err := s.migrateAdvertisementsTableIfNeeded(ctx); err != nil {
		return err
	}

	_, err = s.db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS gatt_services_history (
	session_id INTEGER,
	mac TEXT,
	timestamp TEXT,
	service TEXT,
	PRIMARY KEY (session_id, mac)
);
`)
	if err != nil {
		return err
	}
	// GPS history
	return s.initGPSHistory(ctx)
}

// GPS history for devices.
// Linked to devices via the UNIQUE devices.mac field.
func (s *Store) initGPSHistory(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS device_gps_history (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	session_id INTEGER,
	mac TEXT NOT NULL,
	timestamp TEXT,
	lat REAL,
	lon REAL,
	gps_text TEXT,
	is_cached INTEGER,
	source TEXT,
	FOREIGN KEY(mac) REFERENCES devices(mac) ON DELETE CASCADE
);
`)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_device_gps_history_mac_time ON device_gps_history(mac, timestamp);`)
	return err
}

func normalizeMAC(mac string) string {
	return strings.ToUpper(strings.TrimSpace(mac))
}

func (s *Store) migrateDevicesTableIfNeeded(ctx context.Context) error {
	cols, err := tableColumns(ctx, s.db, "devices")
	if err != nil {
		return err
	}
	// Migrate only if legacy columns are present.
	if !cols["advertisement_raw"] && !cols["device_info"] {
		return nil
	}

	// Rebuild table to drop legacy columns and enforce MAC uniqueness case-insensitively.
	// Disable FK checks during rebuild.
	_, _ = s.db.ExecContext(ctx, `PRAGMA foreign_keys = OFF;`)
	_, err = s.db.ExecContext(ctx, `BEGIN`)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_, _ = s.db.ExecContext(ctx, `ROLLBACK`)
		}
	}()

	_, err = s.db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS devices_new (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	session_id INTEGER,
	device_type TEXT,
	name TEXT,
	mac TEXT UNIQUE COLLATE NOCASE,
	mac_type TEXT,
	mac_subtype TEXT,
	rssi INTEGER,
	service TEXT,
	timestamp TEXT,
	adapter TEXT,
	manufacturer_data TEXT,
	manufacturer_name TEXT,
	service_uuids TEXT,
	service_data TEXT,
	tx_power TEXT,
	platform_data TEXT,
	advertisement_json TEXT,
	last_adv_id INTEGER,
	gps TEXT,
	detection_count INTEGER DEFAULT 1,
	last_count_update TEXT,
	tag TEXT,
	type TEXT
);
`)
	if err != nil {
		return err
	}

	// Copy latest row per MAC (case-insensitive), normalizing MAC to upper-case.
	_, err = s.db.ExecContext(ctx, `

INSERT INTO devices_new (
	id,
	session_id, device_type, name, mac, mac_type, mac_subtype, rssi, service, timestamp, adapter,
	manufacturer_data, manufacturer_name, service_uuids, service_data, tx_power, platform_data,
	advertisement_json, last_adv_id, gps, detection_count, last_count_update, tag, type
)
SELECT
	d.id,
	d.session_id,
	d.device_type,
	d.name,
	UPPER(d.mac) as mac,
	d.mac_type,
	d.mac_subtype,
	d.rssi,
	d.service,
	d.timestamp,
	d.adapter,
	d.manufacturer_data,
	d.manufacturer_name,
	d.service_uuids,
	d.service_data,
	d.tx_power,
	d.platform_data,
	d.advertisement_json,
	d.last_adv_id,
	d.gps,
	COALESCE(d.detection_count, 1) as detection_count,
	d.last_count_update,
	d.tag,
	NULL as type
FROM devices d
JOIN (
	SELECT UPPER(mac) AS umac, MAX(id) AS maxid
	FROM devices
	WHERE mac IS NOT NULL AND TRIM(mac) != ''
	GROUP BY UPPER(mac)
) m
ON UPPER(d.mac) = m.umac AND d.id = m.maxid;
`)
	if err != nil {
		return err
	}

	_, err = s.db.ExecContext(ctx, `DROP TABLE devices;`)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `ALTER TABLE devices_new RENAME TO devices;`)
	if err != nil {
		return err
	}

	_, err = s.db.ExecContext(ctx, `COMMIT`)
	_, _ = s.db.ExecContext(ctx, `PRAGMA foreign_keys = ON;`)
	return err
}

func (s *Store) migrateAdvertisementsTableIfNeeded(ctx context.Context) error {
	cols, err := tableColumns(ctx, s.db, "advertisements")
	if err != nil {
		return err
	}
	if cols["device_id"] {
		return nil
	}

	_, _ = s.db.ExecContext(ctx, `PRAGMA foreign_keys = OFF;`)
	_, err = s.db.ExecContext(ctx, `BEGIN`)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_, _ = s.db.ExecContext(ctx, `ROLLBACK`)
		}
	}()

	_, err = s.db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS advertisements_new (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	session_id INTEGER,
	device_id INTEGER,
	mac TEXT,
	timestamp TEXT,
	rssi INTEGER,
	adv_raw TEXT,
	adv_json TEXT,
	FOREIGN KEY(device_id) REFERENCES devices(id) ON DELETE CASCADE
);
`)
	if err != nil {
		return err
	}

	// Preserve IDs so devices.last_adv_id stays valid.
	_, err = s.db.ExecContext(ctx, `
INSERT INTO advertisements_new (id, session_id, device_id, mac, timestamp, rssi, adv_raw, adv_json)
SELECT
	a.id,
	a.session_id,
	d.id as device_id,
	UPPER(a.mac) as mac,
	a.timestamp,
	a.rssi,
	a.adv_raw,
	a.adv_json
FROM advertisements a
LEFT JOIN devices d ON UPPER(d.mac) = UPPER(a.mac);
`)
	if err != nil {
		return err
	}

	_, err = s.db.ExecContext(ctx, `DROP TABLE advertisements;`)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `ALTER TABLE advertisements_new RENAME TO advertisements;`)
	if err != nil {
		return err
	}

	_, err = s.db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_advertisements_device_id ON advertisements(device_id);`)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS idx_advertisements_mac ON advertisements(mac);`)
	if err != nil {
		return err
	}

	_, err = s.db.ExecContext(ctx, `COMMIT`)
	_, _ = s.db.ExecContext(ctx, `PRAGMA foreign_keys = ON;`)
	return err
}

func tableColumns(ctx context.Context, db *sql.DB, table string) (map[string]bool, error) {
	rows, err := db.QueryContext(ctx, `PRAGMA table_info(`+table+`);`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	cols := map[string]bool{}
	for rows.Next() {
		var cid int
		var name, ctype string
		var notnull int
		var dflt sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			return nil, err
		}
		cols[name] = true
	}
	return cols, rows.Err()
}

func execIgnore(db *sql.DB, ctx context.Context, q string) error {
	_, err := db.ExecContext(ctx, q)
	return err
}

func (s *Store) DeviceExists(ctx context.Context, mac string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	mac = normalizeMAC(mac)
	if mac == "" {
		return false, nil
	}

	var n int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM devices WHERE mac = ?`, mac).Scan(&n)
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

type SaveParams struct {
	SessionID         *int64
	DeviceType        *string
	Name              *string
	MAC               string
	MACType           *string
	MACSubType        *string
	RSSI              *int
	Timestamp         *string
	Adapter           *string
	ManufacturerData  *string
	ManufacturerName  *string
	ServiceUUIDs      *string
	ServiceData       *string
	TxPower           *string
	PlatformData      *string
	AdvertisementJSON *string
	LastAdvID         *int64
	GPS               *string
	ServiceList       *string
	UpdateExisting    bool
	Tag               *string
	MarkedType        *string
}

func (s *Store) SaveDevice(ctx context.Context, p SaveParams) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	p.MAC = normalizeMAC(p.MAC)
	if p.MAC == "" {
		return errors.New("empty MAC")
	}
	if p.Timestamp == nil {
		ts := time.Now().Format("2006-01-02 15:04:05")
		p.Timestamp = &ts
	}

	if p.UpdateExisting {
		// Fetch existing counters.
		var existingCount int
		var lastCountUpdate sql.NullString
		var existingTag sql.NullString
		var existingType sql.NullString
		err := s.db.QueryRowContext(ctx, `SELECT detection_count, last_count_update, tag, device_type FROM devices WHERE mac = ?`, p.MAC).
			Scan(&existingCount, &lastCountUpdate, &existingTag, &existingType)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				// Record does not exist; fall back to insert.
				p.UpdateExisting = false
			} else {
				return err
			}
		}
		if p.UpdateExisting {
			count := existingCount
			lastUpdateStr := lastCountUpdate.String
			typeStr := strings.TrimSpace(existingType.String)

			// Merge/upgrade device_type (ble/classic/dual).
			if p.DeviceType != nil {
				incoming := strings.TrimSpace(*p.DeviceType)
				if incoming != "" {
					if typeStr == "" {
						typeStr = incoming
					} else if strings.EqualFold(typeStr, "dual") {
						// keep
					} else if strings.EqualFold(incoming, "dual") {
						typeStr = "dual"
					} else if !strings.EqualFold(typeStr, incoming) {
						typeStr = "dual"
					}
				}
			}

			// detection_count increments if >= 30 minutes since last_count_update.
			if lastUpdateStr == "" {
				count++
				lastUpdateStr = *p.Timestamp
			} else {
				prev, err := time.Parse("2006-01-02 15:04:05", lastUpdateStr)
				cur, err2 := time.Parse("2006-01-02 15:04:05", *p.Timestamp)
				if err != nil || err2 != nil {
					count++
					lastUpdateStr = *p.Timestamp
				} else if cur.Sub(prev) >= 30*time.Minute {
					count++
					lastUpdateStr = *p.Timestamp
				}
			}

			fields := make([]string, 0, 16)
			args := make([]any, 0, 16)

			if p.Name != nil {
				fields = append(fields, "name = ?")
				args = append(args, *p.Name)
			}
			if p.MACType != nil {
				fields = append(fields, "mac_type = ?")
				args = append(args, *p.MACType)
			}
			if p.MACSubType != nil {
				fields = append(fields, "mac_subtype = ?")
				args = append(args, *p.MACSubType)
			}
			if p.SessionID != nil {
				fields = append(fields, "session_id = ?")
				args = append(args, *p.SessionID)
			}
			if typeStr != "" {
				fields = append(fields, "device_type = ?")
				args = append(args, typeStr)
			}
			if p.RSSI != nil {
				fields = append(fields, "rssi = ?")
				args = append(args, *p.RSSI)
			}
			if p.Timestamp != nil {
				fields = append(fields, "timestamp = ?")
				args = append(args, *p.Timestamp)
			}
			if p.Adapter != nil {
				fields = append(fields, "adapter = ?")
				args = append(args, *p.Adapter)
			}
			if p.ManufacturerData != nil {
				fields = append(fields, "manufacturer_data = ?")
				args = append(args, *p.ManufacturerData)
			}
			if p.ManufacturerName != nil {
				fields = append(fields, "manufacturer_name = ?")
				args = append(args, *p.ManufacturerName)
			}
			if p.ServiceUUIDs != nil {
				fields = append(fields, "service_uuids = ?")
				args = append(args, *p.ServiceUUIDs)
			}
			if p.ServiceData != nil {
				fields = append(fields, "service_data = ?")
				args = append(args, *p.ServiceData)
			}
			if p.TxPower != nil {
				fields = append(fields, "tx_power = ?")
				args = append(args, *p.TxPower)
			}
			if p.PlatformData != nil {
				fields = append(fields, "platform_data = ?")
				args = append(args, *p.PlatformData)
			}
			if p.AdvertisementJSON != nil {
				fields = append(fields, "advertisement_json = ?")
				args = append(args, *p.AdvertisementJSON)
			}
			if p.LastAdvID != nil {
				fields = append(fields, "last_adv_id = ?")
				args = append(args, *p.LastAdvID)
			}
			if p.GPS != nil {
				fields = append(fields, "gps = ?")
				args = append(args, *p.GPS)
			}
			if p.ServiceList != nil {
				fields = append(fields, "service = ?")
				args = append(args, *p.ServiceList)
			}

			fields = append(fields, "detection_count = ?")
			args = append(args, count)
			fields = append(fields, "last_count_update = ?")
			args = append(args, lastUpdateStr)

			if p.Tag != nil {
				fields = append(fields, "tag = ?")
				args = append(args, *p.Tag)
			}

			if p.MarkedType != nil {
				mt := strings.TrimSpace(*p.MarkedType)
				if mt != "" {
					fields = append(fields, "type = ?")
					args = append(args, mt)
				}
			}
			args = append(args, p.MAC)

			q := fmt.Sprintf("UPDATE devices SET %s WHERE mac = ?", strings.Join(fields, ", "))
			_, err := s.db.ExecContext(ctx, q, args...)
			return err
		}
	}

	// Insert path.
	_, err := s.db.ExecContext(ctx, `
INSERT OR IGNORE INTO devices (
	session_id, device_type, name, mac, mac_type, mac_subtype, rssi, timestamp, adapter, manufacturer_data,
	manufacturer_name, service_uuids, service_data, tx_power, platform_data, gps,
	advertisement_json,
	last_adv_id,
	service, detection_count, last_count_update, tag, type
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`,
		optInt64(p.SessionID),
		optString(p.DeviceType),
		optString(p.Name),
		p.MAC,
		optString(p.MACType),
		optString(p.MACSubType),
		optInt(p.RSSI),
		optString(p.Timestamp),
		optString(p.Adapter),
		optString(p.ManufacturerData),
		optString(p.ManufacturerName),
		optString(p.ServiceUUIDs),
		optString(p.ServiceData),
		optString(p.TxPower),
		optString(p.PlatformData),
		optString(p.GPS),
		optString(p.AdvertisementJSON),
		optInt64(p.LastAdvID),
		optString(p.ServiceList),
		1,
		optString(p.Timestamp),
		optString(p.Tag),
		optString(p.MarkedType),
	)
	return err
}

func (s *Store) HasGattServices(ctx context.Context, mac string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	mac = normalizeMAC(mac)
	if mac == "" {
		return false, nil
	}
	var n int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM gatt_services WHERE mac = ? AND service IS NOT NULL AND service != ''`, mac).Scan(&n)
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

func optString(p *string) any {
	if p == nil {
		return nil
	}
	return *p
}

func optInt(p *int) any {
	if p == nil {
		return nil
	}
	return *p
}

func optInt64(p *int64) any {
	if p == nil {
		return nil
	}
	return *p
}

func optBool(p *bool) any {
	if p == nil {
		return nil
	}
	if *p {
		return 1
	}
	return 0
}

func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

// UpdateDeviceGPS updates the gps field for an existing device.
// It is intended for fast GPS refreshes even when other device fields are write-throttled.
func (s *Store) UpdateDeviceGPS(ctx context.Context, mac string, gpsText string) error {
	mac = normalizeMAC(mac)
	if mac == "" {
		return nil
	}
	gpsText = strings.TrimSpace(gpsText)
	if gpsText == "" {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.ExecContext(ctx, `UPDATE devices SET gps = ? WHERE mac = ?`, gpsText, mac)
	return err
}

// UpdateDeviceMarkedType updates the special marker type/meta for an existing device.
// It is intended for fast updates even when full device writes are throttled.
// UpdateDeviceMarkedType updates the special marker type for an existing device.
// It is intended for fast updates even when full device writes are throttled.
func (s *Store) UpdateDeviceMarkedType(ctx context.Context, mac string, markedType string) error {
	mac = normalizeMAC(mac)
	if mac == "" {
		return nil
	}
	markedType = strings.TrimSpace(markedType)
	if markedType == "" {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.ExecContext(ctx, `UPDATE devices SET type = ? WHERE mac = ?`, markedType, mac)
	return err
}

// RecordDeviceGPSHistoryIfChanged inserts a GPS history row when the GPS text changed
// (or when enough time has passed) for the given device MAC.
//
// It links by devices.mac (UNIQUE) to allow stable joins without relying on autoincrement ids.
func (s *Store) RecordDeviceGPSHistoryIfChanged(
	ctx context.Context,
	sessionID *int64,
	mac string,
	timestamp string,
	lat *float64,
	lon *float64,
	gpsText string,
	isCached bool,
	source *string,
) error {
	mac = normalizeMAC(mac)
	if mac == "" {
		return nil
	}
	gpsText = strings.TrimSpace(gpsText)
	if gpsText == "" {
		return nil
	}

	// Throttle: record if changed, or if last record is older than this interval.
	const minInterval = 30 * time.Second

	s.mu.Lock()
	defer s.mu.Unlock()

	lastTxt := s.gpsHistLast[mac]
	lastAt := s.gpsHistLastAt[mac]
	if lastTxt == gpsText && !lastAt.IsZero() && time.Since(lastAt) < minInterval {
		return nil
	}

	_, err := s.db.ExecContext(ctx, `
INSERT INTO device_gps_history (session_id, mac, timestamp, lat, lon, gps_text, is_cached, source)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
`, optInt64(sessionID), mac, timestamp, lat, lon, gpsText, boolToInt(isCached), optString(source))
	if err != nil {
		return err
	}
	s.gpsHistLast[mac] = gpsText
	s.gpsHistLastAt[mac] = time.Now()
	return nil
}

func optUint32(p *uint32) any {
	if p == nil {
		return nil
	}
	return int64(*p)
}

func optUint16(p *uint16) any {
	if p == nil {
		return nil
	}
	return int64(*p)
}

func (s *Store) UpdateGattServices(ctx context.Context, mac string, services string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	mac = normalizeMAC(mac)
	if mac == "" {
		return nil
	}

	_, err := s.db.ExecContext(ctx, `
INSERT INTO gatt_services (mac, service)
VALUES (?, ?)
ON CONFLICT(mac) DO UPDATE SET service = excluded.service
`, mac, services)
	return err
}

type GattCharacteristicParams struct {
	MAC           string
	ServiceUUID   string
	ServiceHandle *uint16
	CharUUID      string
	CharHandle    *uint16
	FlagsJSON     *string
	ValueHex      *string
	ValueASCII    *string
	ReadError     *string
	LastReadAt    string
}

func (s *Store) UpsertGattCharacteristic(ctx context.Context, p GattCharacteristicParams) error {
	p.MAC = normalizeMAC(p.MAC)
	if p.MAC == "" || strings.TrimSpace(p.ServiceUUID) == "" || strings.TrimSpace(p.CharUUID) == "" {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.ExecContext(ctx, `
INSERT INTO gatt_characteristics (
	mac, service_uuid, service_handle, char_uuid, char_handle, flags_json, value_hex, value_ascii, read_error, last_read_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(mac, service_uuid, char_uuid) DO UPDATE SET
	service_handle = COALESCE(excluded.service_handle, gatt_characteristics.service_handle),
	char_handle = COALESCE(excluded.char_handle, gatt_characteristics.char_handle),
	flags_json = COALESCE(excluded.flags_json, gatt_characteristics.flags_json),
	value_hex = COALESCE(excluded.value_hex, gatt_characteristics.value_hex),
	value_ascii = COALESCE(excluded.value_ascii, gatt_characteristics.value_ascii),
	read_error = excluded.read_error,
	last_read_at = excluded.last_read_at
`,
		p.MAC,
		strings.TrimSpace(p.ServiceUUID),
		optUint16(p.ServiceHandle),
		strings.TrimSpace(p.CharUUID),
		optUint16(p.CharHandle),
		optString(p.FlagsJSON),
		optString(p.ValueHex),
		optString(p.ValueASCII),
		optString(p.ReadError),
		p.LastReadAt,
	)
	return err
}

type GattDescriptorParams struct {
	MAC         string
	ServiceUUID string
	CharUUID    string
	DescUUID    string
	DescHandle  *uint16
	FlagsJSON   *string
	ValueHex    *string
	ValueASCII  *string
	ReadError   *string
	LastReadAt  string
}

func (s *Store) UpsertGattDescriptor(ctx context.Context, p GattDescriptorParams) error {
	p.MAC = normalizeMAC(p.MAC)
	if p.MAC == "" || strings.TrimSpace(p.ServiceUUID) == "" || strings.TrimSpace(p.CharUUID) == "" || strings.TrimSpace(p.DescUUID) == "" {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.ExecContext(ctx, `
INSERT INTO gatt_descriptors (
	mac, service_uuid, char_uuid, desc_uuid, desc_handle, flags_json, value_hex, value_ascii, read_error, last_read_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(mac, service_uuid, char_uuid, desc_uuid) DO UPDATE SET
	desc_handle = COALESCE(excluded.desc_handle, gatt_descriptors.desc_handle),
	flags_json = COALESCE(excluded.flags_json, gatt_descriptors.flags_json),
	value_hex = COALESCE(excluded.value_hex, gatt_descriptors.value_hex),
	value_ascii = COALESCE(excluded.value_ascii, gatt_descriptors.value_ascii),
	read_error = excluded.read_error,
	last_read_at = excluded.last_read_at
`,
		p.MAC,
		strings.TrimSpace(p.ServiceUUID),
		strings.TrimSpace(p.CharUUID),
		strings.TrimSpace(p.DescUUID),
		optUint16(p.DescHandle),
		optString(p.FlagsJSON),
		optString(p.ValueHex),
		optString(p.ValueASCII),
		optString(p.ReadError),
		p.LastReadAt,
	)
	return err
}

func (s *Store) GetStatistics(ctx context.Context) (totalDevices, namedDevices, devicesWithService, typedDevices int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	err = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM devices`).Scan(&totalDevices)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	err = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM devices WHERE name != 'Unknown'`).Scan(&namedDevices)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	err = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM devices WHERE service IS NOT NULL AND service != ''`).Scan(&devicesWithService)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	err = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM devices WHERE type IS NOT NULL AND TRIM(type) != ''`).Scan(&typedDevices)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	return totalDevices, namedDevices, devicesWithService, typedDevices, nil
}

func (s *Store) CreateSession(ctx context.Context, adapter string, tag *string, gpsStart *string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	startedAt := time.Now().Format("2006-01-02 15:04:05")
	res, err := s.db.ExecContext(ctx, `INSERT INTO scan_sessions (started_at, adapter, tag, gps_start) VALUES (?, ?, ?, ?)`,
		startedAt,
		adapter,
		optString(tag),
		optString(gpsStart),
	)
	if err != nil {
		return 0, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	return id, nil
}

type AdvertisementParams struct {
	SessionID *int64
	MAC       string
	Timestamp string
	RSSI      *int
	Raw       *string
	JSON      *string
}

func (s *Store) InsertAdvertisement(ctx context.Context, p AdvertisementParams) (int64, error) {
	mac := normalizeMAC(p.MAC)
	if mac == "" {
		return 0, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	// Resolve device_id; create minimal device row if missing.
	var devID int64
	err := s.db.QueryRowContext(ctx, `SELECT id FROM devices WHERE mac = ?`, mac).Scan(&devID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// Minimal upsert.
			_, _ = s.db.ExecContext(ctx, `
INSERT OR IGNORE INTO devices (session_id, device_type, name, mac, rssi, timestamp)
VALUES (?, ?, ?, ?, ?, ?)
`, optInt64(p.SessionID), "ble", "Unknown", mac, optInt(p.RSSI), p.Timestamp)
			err2 := s.db.QueryRowContext(ctx, `SELECT id FROM devices WHERE mac = ?`, mac).Scan(&devID)
			if err2 != nil {
				return 0, err2
			}
		} else {
			return 0, err
		}
	}

	res, err := s.db.ExecContext(ctx, `
INSERT INTO advertisements (session_id, device_id, mac, timestamp, rssi, adv_raw, adv_json)
VALUES (?, ?, ?, ?, ?, ?, ?)
`, optInt64(p.SessionID), devID, mac, p.Timestamp, optInt(p.RSSI), optString(p.Raw), optString(p.JSON))
	if err != nil {
		return 0, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	return id, nil
}

type ClassicInfoParams struct {
	MAC           string
	Class         *uint32
	Icon          *string
	Paired        *bool
	Trusted       *bool
	Connected     *bool
	Blocked       *bool
	LegacyPairing *bool
	Modalias      *string
	UUIDsJSON     *string
	LastSeen      *string
	PropsJSON     *string
}

func (s *Store) UpsertClassicInfo(ctx context.Context, p ClassicInfoParams) error {
	p.MAC = normalizeMAC(p.MAC)
	if p.MAC == "" {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.ExecContext(ctx, `
INSERT INTO classic_devices (
	mac, class, icon, paired, trusted, connected, blocked, legacy_pairing, modalias, uuids, last_seen, props_json
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(mac) DO UPDATE SET
	class = COALESCE(excluded.class, classic_devices.class),
	icon = COALESCE(excluded.icon, classic_devices.icon),
	paired = COALESCE(excluded.paired, classic_devices.paired),
	trusted = COALESCE(excluded.trusted, classic_devices.trusted),
	connected = COALESCE(excluded.connected, classic_devices.connected),
	blocked = COALESCE(excluded.blocked, classic_devices.blocked),
	legacy_pairing = COALESCE(excluded.legacy_pairing, classic_devices.legacy_pairing),
	modalias = COALESCE(excluded.modalias, classic_devices.modalias),
	uuids = COALESCE(excluded.uuids, classic_devices.uuids),
	last_seen = COALESCE(excluded.last_seen, classic_devices.last_seen),
	props_json = COALESCE(excluded.props_json, classic_devices.props_json)
`,
		p.MAC,
		optUint32(p.Class),
		optString(p.Icon),
		optBool(p.Paired),
		optBool(p.Trusted),
		optBool(p.Connected),
		optBool(p.Blocked),
		optBool(p.LegacyPairing),
		optString(p.Modalias),
		optString(p.UUIDsJSON),
		optString(p.LastSeen),
		optString(p.PropsJSON),
	)
	return err
}

type ClassicDiscoveryParams struct {
	SessionID *int64
	MAC       string
	Timestamp string
	RSSI      *int
	Class     *uint32
	PropsJSON *string
}

func (s *Store) InsertClassicDiscovery(ctx context.Context, p ClassicDiscoveryParams) (int64, error) {
	p.MAC = normalizeMAC(p.MAC)
	if p.MAC == "" {
		return 0, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	res, err := s.db.ExecContext(ctx, `
INSERT INTO classic_discoveries (session_id, mac, timestamp, rssi, class, props_json)
VALUES (?, ?, ?, ?, ?, ?)
`, optInt64(p.SessionID), p.MAC, p.Timestamp, optInt(p.RSSI), optUint32(p.Class), optString(p.PropsJSON))
	if err != nil {
		return 0, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	return id, nil
}

func (s *Store) UpdateDeviceLastAdvID(ctx context.Context, mac string, advID int64) error {
	mac = normalizeMAC(mac)
	if mac == "" || advID <= 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.ExecContext(ctx, `UPDATE devices SET last_adv_id = ? WHERE mac = ?`, advID, mac)
	return err
}

func (s *Store) InsertGattServicesHistory(ctx context.Context, sessionID int64, mac string, services string, ts string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	mac = normalizeMAC(mac)
	if mac == "" {
		return nil
	}
	_, err := s.db.ExecContext(ctx, `
INSERT INTO gatt_services_history (session_id, mac, timestamp, service)
VALUES (?, ?, ?, ?)
ON CONFLICT(session_id, mac) DO UPDATE SET timestamp = excluded.timestamp, service = excluded.service
`, sessionID, mac, ts, services)
	return err
}

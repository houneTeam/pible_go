package ids

import (
	"encoding/csv"
	"io"
	"os"
	"strings"
)

// LoadOUI loads vendor names keyed by OUI (6 hex digits, uppercase) from a CSV
// file with IEEE format (Registry, Assignment, Organization Name, ...).
func LoadOUI(path string) (map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := csv.NewReader(f)
	r.FieldsPerRecord = -1

	// Read header.
	if _, err := r.Read(); err != nil {
		return nil, err
	}

	out := make(map[string]string, 1024)
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if len(rec) < 3 {
			continue
		}
		assignment := strings.ToUpper(strings.TrimSpace(rec[1]))
		assignment = strings.ReplaceAll(assignment, "-", "")
		assignment = strings.ReplaceAll(assignment, ":", "")
		if len(assignment) != 6 {
			continue
		}
		org := strings.TrimSpace(rec[2])
		if org == "" {
			continue
		}
		out[assignment] = org
	}

	return out, nil
}

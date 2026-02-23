package status

import (
	"context"
	"time"

	"pible/internal/db"
	"pible/internal/gps"
	"pible/internal/util"
)

type Provider struct {
	GPS   *gps.State
	Store *db.Store
}

// Run prints periodic structured status lines to the console.
func Run(ctx context.Context, interval time.Duration, p Provider) {
	if interval <= 0 {
		interval = 5 * time.Second
	}

	t := time.NewTicker(interval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			printOnce(ctx, p)
		}
	}
}

func printOnce(ctx context.Context, p Provider) {
	// GPS
	gpsLine := "offline"
	if p.GPS != nil {
		if s := p.GPS.GPSStringForRecord(); s != nil {
			gpsLine = *s
		}
	}
	util.Linef("[GPS DATA]", util.ColorCyan, "%s", gpsLine)

	// DB stats
	if p.Store != nil {
		total, named, withServices, typed, err := p.Store.GetStatistics(ctx)
		if err == nil {
			util.Linef("[DB STATS]", util.ColorGray, "Total Devices: %d, Named: %d, With Services: %d, Typed %d", total, named, withServices, typed)
		}
	}

	// Battery
	if pct := util.BatteryPercent(); pct != "" {
		util.Linef("[BATTERY]", util.ColorGray, "%s", pct)
	}
}

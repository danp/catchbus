package main

import (
	"context"
	"database/sql"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/danp/catchbus/gtfs/gtfsrt"
	"github.com/graxinc/errutil"
	"google.golang.org/protobuf/proto"
	_ "modernc.org/sqlite"
)

func main() {
	ctx := context.Background()

	db, err := sql.Open("sqlite", "file:delay.db")
	if err != nil {
		log.Fatal(err)
	}

	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS delay_observations (ts INTEGER, route_id TEXT, delay_bucket INTEGER, count INTEGER, PRIMARY KEY (ts, route_id, delay_bucket)) WITHOUT ROWID`); err != nil {
		log.Fatal(err)
	}

	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	tracker := &tracker{make(map[seenKey]struct{})}

	first := true
	for {
		if !first {
			<-ticker.C
		}

		now, buckets, err := tracker.run(ctx)
		if err != nil {
			log.Println(err)
			continue
		}

		if first {
			// recording info from the first run might duplicate data
			// from previous execution
			first = false
			continue
		}

		var observed int
		err = func() error {
			tx, err := db.Begin()
			if err != nil {
				return errutil.With(err)
			}
			defer tx.Rollback()

			for route, routeBuckets := range buckets {
				for delay, count := range routeBuckets {
					observed += count
					if _, err := tx.Exec(`INSERT INTO delay_observations (ts, route_id, delay_bucket, count) VALUES (?, ?, ?, ?)`, now.Unix(), route, delay.Minutes(), count); err != nil {
						return errutil.With(err)
					}
				}
			}

			if err := tx.Commit(); err != nil {
				return errutil.With(err)
			}
			return nil
		}()
		if err != nil {
			log.Println(err)
			continue
		}

		log.Printf("%v: observed %v", now, observed)
	}
}

type sample struct {
	tu   *gtfsrt.TripUpdate
	stop *gtfsrt.TripUpdate_StopTimeUpdate
}

type seenKey struct {
	tripID string
	stopID string
}

type tracker struct {
	seen map[seenKey]struct{}
}

func (t *tracker) run(ctx context.Context) (time.Time, map[string]map[time.Duration]int, error) {
	data, err := t.fetch(ctx)
	if err != nil {
		return time.Time{}, nil, errutil.With(err)
	}

	buckets := make(map[string]map[time.Duration]int) // route -> delay -> count
	observed := make(map[seenKey]struct{})

	now := time.Now()
	if data.GetHeader().GetTimestamp() > 0 {
		now = time.Unix(int64(data.GetHeader().GetTimestamp()), 0)
	}

	for _, entity := range data.GetEntity() {
		tu := entity.GetTripUpdate()
		for _, stu := range tu.StopTimeUpdate {
			arrival := stu.GetArrival()
			var arrivalTime time.Time
			if arrival.Time != nil {
				arrivalTime = time.Unix(arrival.GetTime(), 0)
			}
			if arrivalTime.IsZero() || arrivalTime.After(now) {
				continue
			}
			key := seenKey{tu.GetTrip().GetTripId(), stu.GetStopId()}
			observed[key] = struct{}{}
			if _, ok := t.seen[key]; ok {
				continue
			}
			t.seen[key] = struct{}{}
			delay := (time.Duration(arrival.GetDelay()) * time.Second).Round(time.Minute)
			if buckets[tu.GetTrip().GetRouteId()] == nil {
				buckets[tu.GetTrip().GetRouteId()] = make(map[time.Duration]int)
			}
			buckets[tu.GetTrip().GetRouteId()][delay]++
		}
	}

	for k := range t.seen {
		if _, ok := observed[k]; !ok {
			delete(t.seen, k)
		}
	}

	return now, buckets, nil
}

func (t *tracker) fetch(ctx context.Context) (*gtfsrt.FeedMessage, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", "https://gtfs.halifax.ca/realtime/TripUpdate/TripUpdates.pb", nil)
	if err != nil {
		return nil, errutil.With(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, errutil.With(err)
	}
	defer resp.Body.Close()

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errutil.With(err)
	}

	m := new(gtfsrt.FeedMessage)
	if err := proto.Unmarshal(b, m); err != nil {
		return nil, errutil.With(err)
	}
	return m, nil
}

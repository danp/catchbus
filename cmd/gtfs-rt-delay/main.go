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
	db, err := sql.Open("sqlite", "file:delay.db")
	if err != nil {
		log.Fatal(err)
	}

	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS delay_observations (ts INTEGER, delay_bucket INTEGER, count INTEGER)`); err != nil {
		log.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS delay_samples (ts INTEGER, delay_bucket INTEGER, route_id TEXT, trip_id TEXT, stop_id TEXT)`); err != nil {
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
		first = false

		now, buckets, samples, err := tracker.run(context.Background())
		if err != nil {
			log.Println(err)
			continue
		}

		func() error {
			tx, err := db.Begin()
			if err != nil {
				return errutil.With(err)
			}
			defer tx.Rollback()

			for delay, count := range buckets {
				if _, err := tx.Exec(`INSERT INTO delay_observations (ts, delay_bucket, count) VALUES (?, ?, ?)`, now.Unix(), delay.Minutes(), count); err != nil {
					return errutil.With(err)
				}
			}

			for delay, samples := range samples {
				for _, sample := range samples {
					if _, err := tx.Exec(`INSERT INTO delay_samples (ts, delay_bucket, route_id, trip_id, stop_id) VALUES (?, ?, ?, ?, ?)`, now.Unix(), delay.Minutes(), sample.tu.GetTrip().GetRouteId(), sample.tu.GetTrip().GetTripId(), sample.stop.GetStopId()); err != nil {
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

		log.Printf("%v: %v", now.UTC().Format(time.RFC3339), buckets)
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

func (t *tracker) run(ctx context.Context) (time.Time, map[time.Duration]int, map[time.Duration][]sample, error) {
	data, err := t.fetch(ctx)
	if err != nil {
		return time.Time{}, nil, nil, errutil.With(err)
	}

	buckets := make(map[time.Duration]int)
	samples := make(map[time.Duration][]sample)
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
			buckets[delay]++

			if len(samples[delay]) >= 5 {
				continue
			}
			samples[delay] = append(samples[delay], sample{tu, stu})
		}
	}

	for k := range t.seen {
		if _, ok := observed[k]; !ok {
			delete(t.seen, k)
		}
	}

	return now, buckets, samples, nil
}

func (t *tracker) fetch(ctx context.Context) (*gtfsrt.FeedMessage, error) {
	resp, err := http.Get("https://gtfs.halifax.ca/realtime/TripUpdate/TripUpdates.pb")
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

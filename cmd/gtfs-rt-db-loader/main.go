package main

import (
	"io"
	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/danp/catchbus/gtfs/gtfsrt"
	"github.com/golang/protobuf/proto"
	"github.com/jmoiron/sqlx"

	_ "github.com/lib/pq"
)

type TripUpdateMessage struct {
	TS time.Time
}

type TripUpdateEntity struct {
	ID                   string
	TripUpdateMessageTS  time.Time `db:"trip_update_message_ts"`
	TS                   time.Time
	Delay                int
	TripID               string    `db:"trip_id"`
	RouteID              string    `db:"route_id"`
	DirectionID          int       `db:"direction_id"`
	StartTime            string    `db:"start_time"`
	StartDate            time.Time `db:"start_date"`
	ScheduleRelationship int       `db:"schedule_relationship"`
	VehicleID            string    `db:"vehicle_id"`
	VehicleLabel         string    `db:"vehicle_label"`
}

type StopTimeUpdate struct {
	TripUpdateEntityID   string    `db:"trip_update_entitiy_id"`
	TripUpdateMessageTS  time.Time `db:"trip_update_message_ts"`
	StopSequence         int       `db:"stop_sequence"`
	StopID               string    `db:"stop_id"`
	DepartureTime        time.Time `db:"departure_time"`
	ArrivalTime          time.Time `db:"arrival_time"`
	ScheduleRelationship int       `db:"schedule_relationship"`
}

func main() {
	db, err := sqlx.Connect("postgres", "postgres://localhost/hfx-gtfs-rt-archive?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}

	var rc io.ReadCloser

	if len(os.Args) > 1 {
		f, err := os.Open(os.Args[1])
		if err != nil {
			log.Fatal(err)
		}
		rc = f
	} else {
		rc = os.Stdin
	}

	b, err := ioutil.ReadAll(rc)
	if err != nil {
		log.Fatal(err)
	}
	defer rc.Close()

	m := new(gtfsrt.FeedMessage)
	if err := proto.Unmarshal(b, m); err != nil {
		log.Fatal(err)
	}

	tx, err := db.Beginx()
	if err != nil {
		log.Fatal(err)
	}

	dm := new(TripUpdateMessage)
	dm.TS = time.Unix(int64(m.GetHeader().GetTimestamp()), 0).UTC()

	_, err = tx.Exec("insert into trip_update_messages (ts) values ($1)", dm.TS)
	if err != nil {
		log.Fatal(err)
	}

	for _, e := range m.GetEntity() {
		tu := e.GetTripUpdate()
		if tu == nil {
			continue
		}

		de := new(TripUpdateEntity)
		de.ID = e.GetId()
		de.TripUpdateMessageTS = dm.TS
		de.TS = time.Unix(int64(tu.GetTimestamp()), 0)
		de.Delay = int(tu.GetDelay())
		de.TripID = tu.GetTrip().GetTripId()
		de.RouteID = tu.GetTrip().GetRouteId()
		de.DirectionID = int(tu.GetTrip().GetDirectionId())
		de.StartTime = tu.GetTrip().GetStartTime()
		sd, err := time.Parse("20060102", tu.GetTrip().GetStartDate())
		if err != nil {
			log.Fatal(err)
		}
		de.StartDate = sd
		de.ScheduleRelationship = int(tu.GetTrip().GetScheduleRelationship())
		de.VehicleID = tu.GetVehicle().GetId()
		de.VehicleLabel = tu.GetVehicle().GetLabel()

		_, err = tx.Exec("insert into trip_update_entities (id, trip_update_message_ts, ts, delay, trip_id, route_id, direction_id, start_time, start_date, schedule_relationship, vehicle_id, vehicle_label) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)",
			de.ID,
			de.TripUpdateMessageTS,
			de.TS,
			de.Delay,
			de.TripID,
			de.RouteID,
			de.DirectionID,
			de.StartTime,
			de.StartDate,
			de.ScheduleRelationship,
			de.VehicleID,
			de.VehicleLabel,
		)
		if err != nil {
			log.Fatal(err)
		}

		for _, s := range tu.GetStopTimeUpdate() {
			ds := new(StopTimeUpdate)
			ds.TripUpdateEntityID = de.ID
			ds.TripUpdateMessageTS = dm.TS
			ds.StopSequence = int(s.GetStopSequence())
			ds.StopID = s.GetStopId()
			ds.DepartureTime = time.Unix(int64(s.GetDeparture().GetTime()), 0)
			ds.ArrivalTime = time.Unix(int64(s.GetArrival().GetTime()), 0)
			if en := s.GetScheduleRelationship().Enum(); en != nil {
				ds.ScheduleRelationship = int(*en)
			}

			_, err = tx.Exec("insert into stop_time_updates (trip_update_entity_id, trip_update_message_ts, stop_sequence, stop_id, departure_time, arrival_time, schedule_relationship) values ($1, $2, $3, $4, $5, $6, $7)",
				ds.TripUpdateEntityID,
				ds.TripUpdateMessageTS,
				ds.StopSequence,
				ds.StopID,
				ds.DepartureTime,
				ds.ArrivalTime,
				ds.ScheduleRelationship,
			)
			if err != nil {
				log.Fatal(err)
			}
		}
	}

	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}
}

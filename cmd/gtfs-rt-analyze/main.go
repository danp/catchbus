package main

import (
	"bufio"
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"time"

	"github.com/danp/catchbus/gtfs/gtfsrt"
	"github.com/gogo/protobuf/proto"
	"github.com/jmoiron/sqlx"
	"github.com/jmoiron/sqlx/types"
	"github.com/pkg/errors"

	_ "github.com/lib/pq"
)

// given a top directory of trip updates
// scan them in order
// build a log of what happened to each trip
// to start, just log every change in expected arrival/departure time

type trip struct {
	tr  *gtfsrt.TripDescriptor
	tus []tripUpdate
}

type tripUpdate struct {
	tu *gtfsrt.TripUpdate
	ts time.Time
}

type processor struct {
	db  *sqlx.DB
	loc *time.Location

	// map of full trip id -> last seen trip update
	tripCache map[string]*tripUpdate
}

func newProcessor(db *sqlx.DB, loc *time.Location) *processor {
	return &processor{db: db, loc: loc, tripCache: make(map[string]*tripUpdate)}
}

func (p *processor) processFile(fn string) error {
	b, err := ioutil.ReadFile(fn)
	if err != nil {
		return errors.Wrapf(err, "reading %s", fn)
	}

	m := new(gtfsrt.FeedMessage)
	if err := proto.Unmarshal(b, m); err != nil {
		return errors.Wrapf(err, "unmarshaling %s", fn)
	}

	for _, e := range m.GetEntity() {
		tu := e.GetTripUpdate()
		if tu == nil {
			continue
		}

		fullID := tu.GetTrip().GetStartDate() + "/" + tu.GetTrip().GetStartTime() + "/" + tu.GetTrip().GetTripId()
		ltu, ok := p.tripCache[fullID]
		if !ok {
			tr := &trip{tr: tu.GetTrip()}
			if err := p.insertTrip(fullID, tr); err != nil {
				return err
			}
		}

		ts := time.Unix(int64(tu.GetTimestamp()), 0)
		if ltu != nil {
			if ts.Equal(ltu.ts) {
				continue
			}

			if reflect.DeepEqual(ltu.tu.GetStopTimeUpdate(), tu.GetStopTimeUpdate()) {
				continue
			}
		}

		otu := &tripUpdate{tu, ts}
		if err := p.insertTripUpdate(fullID, otu); err != nil {
			return err
		}
		p.tripCache[fullID] = otu
	}

	return nil
}

func (p *processor) insertTrip(id string, tr *trip) error {
	tl := &DBTripLog{
		ID:     id,
		TripID: tr.tr.GetTripId(),
	}

	_, err := p.db.NamedExec("insert into trip_logs values (:id, :trip_id)", tl)
	return errors.Wrap(err, "executing insert of trip log")
}

func (p *processor) insertTripUpdate(fullTripID string, tu *tripUpdate) error {
	tuj, err := json.Marshal(tu.tu)
	if err != nil {
		return errors.Wrap(err, "marshaling trip update")
	}

	dtu := &DBTripUpdate{
		TripLogID:  fullTripID,
		TS:         tu.ts,
		TripUpdate: types.JSONText(tuj),
	}

	_, err = p.db.NamedExec("insert into trip_log_updates values (:trip_log_id, :ts, :trip_update)", dtu)
	return errors.Wrap(err, "executing insert of trip log update")
}

type DBTripLog struct {
	ID     string
	TripID string `db:"trip_id"`
}

type DBTripUpdate struct {
	TripLogID  string `db:"trip_log_id"`
	TS         time.Time
	TripUpdate types.JSONText `db:"trip_update"`
}

func main() {
	log.SetFlags(0)

	db, err := sqlx.Connect("postgres", "dbname=hfxtransit sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	log.Println("connected")

	loc, err := time.LoadLocation("America/Halifax")
	if err != nil {
		log.Fatal(err)
	}

	pr := newProcessor(db, loc)

	scnr := bufio.NewScanner(os.Stdin)
	for scnr.Scan() {
		if err := pr.processFile(scnr.Text()); err != nil {
			log.Fatal(err)
		}
	}

	if err := scnr.Err(); err != nil {
		log.Fatal(err)
	}
}

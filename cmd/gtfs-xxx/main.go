package main

import (
	"bufio"
	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/danp/catchbus/gtfs/gtfsrt"
	"github.com/gogo/protobuf/proto"
	"github.com/jmoiron/sqlx"

	_ "github.com/lib/pq"
)

type entry struct {
	stu     *gtfsrt.TripUpdate_StopTimeUpdate
	initial bool
	ts      uint64
}

var (
	lastFile       string
	stusProcessed  uint64
	updatesEmitted uint64
)

func main() {
	db, err := sqlx.Connect("postgres", "postgres://localhost/transitstuff?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}

	var (
		firstFile = true
		initials  = make(map[string]bool)
		store     = make(map[string]*entry)
	)

	go report()

	sc := bufio.NewScanner(os.Stdin)
	for sc.Scan() {
		lastFile = sc.Text()

		b, err := ioutil.ReadFile(sc.Text())
		if err != nil {
			log.Fatal(err)
		}

		m := new(gtfsrt.FeedMessage)
		if err := proto.Unmarshal(b, m); err != nil {
			log.Fatal(err)
		}

		tuts := m.GetHeader().GetTimestamp()

		for _, e := range m.GetEntity() {
			tu := e.GetTripUpdate()
			if tu == nil {
				continue
			}

			tkey := tu.GetTrip().GetStartDate() + "-" + tu.GetTrip().GetTripId()

			if firstFile {
				initials[tkey] = true
				continue
			}

			if initials[tkey] {
				continue
			}

			for _, stu := range tu.GetStopTimeUpdate() {
				stusProcessed++

				skey := tkey + "-" + stu.GetStopId()

				cat := stu.GetArrival().GetTime()
				cdt := stu.GetDeparture().GetTime()

				pent, ok := store[skey]
				if !ok {
					store[skey] = &entry{
						stu:     stu,
						ts:      tuts,
						initial: true,
					}

					updatesEmitted++

					_, err := db.Exec(
						"insert into stop_time_updates (service_date, trip_id, stop_id, start_at, arrival_time, departure_time) values ($1, $2, $3, $4, $5, $6)",
						tu.GetTrip().GetStartDate(),
						tu.GetTrip().GetTripId(),
						stu.GetStopId(),
						tuts,
						cat,
						cdt,
					)
					if err != nil {
						log.Fatal(err)
					}

					continue
				}

				pat := pent.stu.GetArrival().GetTime()
				pdt := pent.stu.GetDeparture().GetTime()

				sd := pdt == cdt
				sa := pat == cat

				if sd && sa {
					continue
				}

				store[skey] = &entry{
					stu: stu,
					ts:  tuts,
				}

				updatesEmitted++

				if pent.initial {
					_, err := db.Exec(
						"update stop_time_updates set end_at=$1 where service_date=$2 and trip_id=$3 and stop_id=$4 and start_at=$5",
						tuts,
						tu.GetTrip().GetStartDate(),
						tu.GetTrip().GetTripId(),
						stu.GetStopId(),
						pent.ts,
					)
					if err != nil {
						log.Fatal(err)
					}
				} else {
					_, err := db.Exec(
						"insert into stop_time_updates (service_date, trip_id, stop_id, start_at, end_at, arrival_time, departure_time) values ($1, $2, $3, $4, $5, $6, $7)",
						tu.GetTrip().GetStartDate(),
						tu.GetTrip().GetTripId(),
						stu.GetStopId(),
						pent.ts,
						tuts,
						cat,
						cdt,
					)
					if err != nil {
						log.Fatal(err)
					}
				}
			}
		}

		if firstFile {
			log.Printf("noted %d initial trips", len(initials))
			firstFile = false
		}
	}

	if err := sc.Err(); err != nil {
		log.Fatal(err)
	}
}

func report() {
	for range time.Tick(time.Second) {
		upPct := float64(updatesEmitted) / float64(stusProcessed) * 100.0
		log.Printf("last file: %s / stop time updates processed: %d / updates emitted: %d (%.2f%%)", lastFile, stusProcessed, updatesEmitted, upPct)
	}
}

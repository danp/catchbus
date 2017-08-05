package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/danp/catchbus/gtfs/gtfsrt"
	"github.com/gogo/protobuf/proto"
)

type entry struct {
	stu *gtfsrt.TripUpdate_StopTimeUpdate
	ts  uint64
}

var (
	lastFile       string
	stusProcessed  uint64
	updatesEmitted uint64
)

func main() {
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
						stu: stu,
						ts:  tuts,
					}

					updatesEmitted++

					fmt.Printf(
						"skey %s initial at %d, arrival %d, departure %d\n",
						skey, tuts, cat, cdt,
					)

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

				tutsdiff := tuts - pent.ts
				adiff := cat - pat
				ddiff := cdt - pdt

				updatesEmitted++

				fmt.Printf(
					"skey %s update at %d (%d) arrival %d -> %d (%d), departure %d -> %d (%d)\n",
					skey, tuts, tutsdiff, pat, cat, adiff, pdt, cdt, ddiff,
				)
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

package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/danp/catchbus/gtfs/gtfsrt"
	"github.com/gogo/protobuf/proto"
)

type entry struct {
	stu *gtfsrt.TripUpdate_StopTimeUpdate
	ts  uint64
}

func main() {
	cache := make(map[string]*entry)

	sc := bufio.NewScanner(os.Stdin)
	for sc.Scan() {
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

			for _, stu := range tu.GetStopTimeUpdate() {
				skey := tkey + "-" + stu.GetStopId()

				cat := stu.GetArrival().GetTime()
				cdt := stu.GetDeparture().GetTime()

				pent, ok := cache[skey]
				if !ok {
					cache[skey] = &entry{
						stu: stu,
						ts:  tuts,
					}

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

				cache[skey] = &entry{
					stu: stu,
					ts:  tuts,
				}

				tutsdiff := tuts - pent.ts
				adiff := cat - pat
				ddiff := cdt - pdt

				fmt.Printf(
					"skey %s update at %d (%d, %d) arrival %d -> %d (%d), departure %d -> %d (%d)\n",
					skey, tuts, pent.ts, tutsdiff, pat, cat, adiff, pdt, cdt, ddiff,
				)
			}
		}
	}

	if err := sc.Err(); err != nil {
		log.Fatal(err)
	}
}

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

func main() {
	cache := make(map[string]*gtfsrt.TripUpdate_StopTimeUpdate)

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

		for _, e := range m.GetEntity() {
			tu := e.GetTripUpdate()
			if tu == nil {
				continue
			}

			tuts := tu.GetTimestamp()
			if tuts == 0 {
				tuts = m.GetHeader().GetTimestamp()
			}

			tkey := tu.GetTrip().GetStartDate() + "-" + tu.GetTrip().GetTripId()

			for _, stu := range tu.GetStopTimeUpdate() {
				skey := tkey + "-" + stu.GetStopId()

				cat := stu.GetArrival().GetTime()
				cdt := stu.GetDeparture().GetTime()

				pstu, ok := cache[skey]
				if ok {
					pat := pstu.GetArrival().GetTime()
					adiff := cat - pat

					pdt := pstu.GetDeparture().GetTime()
					ddiff := cdt - pdt

					sd := pdt == cdt
					sa := pat == cat

					if !sa {
						fmt.Printf("skey %s update at %d arrival was %d now %d (%d)\n", skey, tuts, pat, cat, adiff)
					}
					if !sd {
						fmt.Printf("skey %s update at %d departure was %d now %d (%d)\n", skey, tuts, pdt, cdt, ddiff)
					}

					if sd && sa {
						continue
					}
				} else {
					fmt.Printf("skey %s first update at %d arrival %d, departure %d\n", skey, tuts, cat, cdt)
				}

				cache[skey] = stu
			}
		}
	}

	if err := sc.Err(); err != nil {
		log.Fatal(err)
	}
}

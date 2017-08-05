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
			var tutss = "(unknown)"
			if tuts > 0 {
				tutss = time.Unix(int64(tuts), 0).Format("03:04:05 PM")
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
						x := time.Unix(pat, 0).Format("03:04:05 PM")
						y := time.Unix(cat, 0).Format("03:04:05 PM")
						fmt.Printf("skey %s update at %s arrival was %s now %s (%d)\n", skey, tutss, x, y, adiff)
					}
					if !sd {
						x := time.Unix(pdt, 0).Format("03:04:05 PM")
						y := time.Unix(cdt, 0).Format("03:04:05 PM")
						fmt.Printf("skey %s update at %s departure was %s now %s (%d)\n", skey, tutss, x, y, ddiff)
					}

					if sd && sa {
						continue
					}
				} else {
					var (
						x = "(unknown)"
						y = "(unknown)"
					)

					if cat > 0 {
						x = time.Unix(cat, 0).Format("03:04:05 PM")
					}
					if cdt > 0 {
						y = time.Unix(cdt, 0).Format("03:04:05 PM")
					}

					fmt.Printf("skey %s first update at %s arrival %s, departure %s\n", skey, tutss, x, y)
				}

				cache[skey] = stu
			}
		}
	}

	if err := sc.Err(); err != nil {
		log.Fatal(err)
	}
}

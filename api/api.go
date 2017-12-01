package api

import (
	"encoding/json"
	"log"
	"net/http"
	"sort"
	"time"

	"github.com/NYTimes/gziphandler"
	"github.com/danp/catchbus/gtfs"
	"github.com/danp/catchbus/gtfs/gtfsrt"
	"github.com/danp/catchbus/gtfs/gtfsrt/feed"
	"github.com/danp/catchbus/planner"
	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
)

type HistoryEntry struct {
	Time  time.Time
	Entry *gtfsrt.FeedMessage
}

type history interface {
	GetAsOf(kind string, ts time.Time) (HistoryEntry, error)
}

func Start(st *gtfs.Static, pl *planner.Planner, fd *feed.Feed, hist history) {
	mx := chi.NewMux()

	mx.Use(middleware.RequestID)
	mx.Use(middleware.RealIP)
	mx.Use(middleware.Logger)

	mx.Get("/calendar", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wj(w, st.Calendar)
	}))

	mx.Get("/calendar/{service_id}", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serviceID := chi.URLParam(r, "service_id")

		c, err := st.CalendarForServiceID(serviceID)
		if err != nil {
			// TODO: could be something else?
			http.Error(w, "service id not found", http.StatusNotFound)
			return
		}

		wj(w, c)
	}))

	mx.Get("/routes/{route_id}/positions", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		routeID := chi.URLParam(r, "route_id")

		trips := st.TripIDsForRouteID(routeID)
		vp := fd.CurrentVehiclePositions()

		type vehiclePosition struct {
			Trip            *gtfs.Trip
			VehiclePosition *gtfsrt.VehiclePosition
		}

		var vps []vehiclePosition
		for _, p := range vp.GetEntity() {
			v := p.GetVehicle()
			tid := v.GetTrip().GetTripId()
			if trips[tid] {
				tr := st.TripIDsToTrips[tid]
				vps = append(vps, vehiclePosition{tr, v})
			}
		}

		wj(w, vps)
	}))

	mx.Get("/routes/{route_id}/updates", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		routeID := chi.URLParam(r, "route_id")

		trips := st.TripIDsForRouteID(routeID)
		tu := fd.CurrentTripUpdates()

		type tripUpdate struct {
			Trip       *gtfs.Trip
			TripUpdate *gtfsrt.TripUpdate
		}

		var ups []tripUpdate
		for _, u := range tu.GetEntity() {
			t := u.GetTripUpdate()
			tid := t.GetTrip().GetTripId()
			if trips[tid] {
				tr := st.TripIDsToTrips[tid]
				ups = append(ups, tripUpdate{tr, t})
			}
		}

		wj(w, ups)
	}))

	mx.Get("/stops/{stop_id}/departures", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		stopID := chi.URLParam(r, "stop_id")
		stop := st.StopIDsToStops[stopID]
		if stop == nil {
			http.Error(w, "stop not found", http.StatusNotFound)
			return
		}

		targetTime := time.Now()
		if tts := r.URL.Query().Get("targetTime"); tts != "" {
			t, err := time.Parse(time.RFC3339, tts)
			if err != nil {
				http.Error(w, "time parse error: "+err.Error(), http.StatusBadRequest)
				return
			}
			targetTime = t
		}

		deps := pl.DeparturesForStop(stop, targetTime)

		var resp struct {
			Stop       gtfs.Stop
			Departures []planner.Departure
		}
		resp.Stop = *stop
		resp.Departures = deps

		wj(w, resp)
	}))

	mx.Get("/history/{kind}", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		kind := chi.URLParam(r, "kind")

		tss := r.URL.Query().Get("ts")
		if tss == "" {
			http.Error(w, "need ts", http.StatusBadRequest)
			return
		}

		ts, err := time.Parse(time.RFC3339, tss)
		if err != nil {
			http.Error(w, "ts parse error: "+err.Error(), http.StatusBadRequest)
			return
		}

		entry, err := hist.GetAsOf(kind, ts)
		if err != nil {
			log.Println(err)
			http.Error(w, "error fetching entry", http.StatusInternalServerError)
			return
		}

		wj(w, entry)
	}))

	mx.Get("/final-updates", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sts, ets := r.URL.Query().Get("startTime"), r.URL.Query().Get("endTime")
		if sts == "" || ets == "" {
			http.Error(w, "need startTime and endTime", http.StatusBadRequest)
			return
		}

		st, err := time.Parse(time.RFC3339, sts)
		if err != nil {
			http.Error(w, "startTime parse error: "+err.Error(), http.StatusBadRequest)
			return
		}
		st = st.Truncate(time.Minute)

		et, err := time.Parse(time.RFC3339, ets)
		if err != nil {
			http.Error(w, "endTime parse error: "+err.Error(), http.StatusBadRequest)
			return
		}
		et = et.Truncate(time.Minute)

		if !et.After(st) {
			http.Error(w, "endTime must be after startTime", http.StatusBadRequest)
			return
		}

		if et.Sub(st) > 4*time.Hour {
			http.Error(w, "endTime must be no more than 4 hours after startTime", http.StatusBadRequest)
			return
		}

		var minutes []time.Time
		for m := st; m.Before(et) || m.Equal(et); m = m.Add(time.Minute) {
			minutes = append(minutes, m)
		}

		var hes []HistoryEntry
		for _, m := range minutes {
			he, err := hist.GetAsOf("TripUpdates", m)
			if err != nil {
				log.Println(err)
				http.Error(w, "error fetching entry", http.StatusInternalServerError)
				return
			}
			log.Println(he.Time)
			hes = append(hes, he)
		}

		type te struct {
			asOf time.Time
			stu  *gtfsrt.TripUpdate_StopTimeUpdate
		}

		type tustage struct {
			tu   *gtfsrt.TripUpdate
			stes map[string]te
		}

		stage := make(map[string]tustage)

		for _, he := range hes {
			for _, e := range he.Entry.GetEntity() {
				tu := e.GetTripUpdate()
				if tu == nil {
					continue
				}

				debug := tu.GetTrip().GetTripId() == "16572623"

				tkey := tu.GetTrip().GetStartDate() + "-" + tu.GetTrip().GetTripId()
				tus, ok := stage[tkey]
				if !ok {
					if debug {
						log.Printf("init new tus for %s", tkey)
					}
					tus = tustage{
						tu:   tu,
						stes: make(map[string]te),
					}
				} else if debug {
					log.Printf("existing tus for %s", tkey)
				}

				for _, stu := range tu.GetStopTimeUpdate() {
					skey := tkey + "-" + stu.GetStopId()

					if debug {
						log.Printf(
							"set skey=%s asOf=%s stopSeq=%d arrival=%d departure=%d",
							skey, he.Time, stu.GetStopSequence(),
							stu.GetArrival().GetTime(), stu.GetDeparture().GetTime(),
						)
					}

					tus.stes[skey] = te{
						asOf: he.Time,
						stu:  stu,
					}
				}

				stage[tkey] = tus
			}
		}

		out := new(gtfsrt.FeedMessage)
		for _, tus := range stage {
			tu := tus.tu
			tu.StopTimeUpdate = nil

			debug := tu.GetTrip().GetTripId() == "16572623"

			for _, ste := range tus.stes {
				it := ste.stu.GetDeparture().GetTime()
				if it == 0 {
					it = ste.stu.GetArrival().GetTime()
				}
				itt := time.Unix(it, 0)

				itu64 := uint64(it)
				if itu64 > tu.GetTimestamp() {
					tu.Timestamp = &itu64
				}

				if diff := ste.asOf.Sub(itt); diff >= 5*time.Minute {
					if debug {
						log.Printf(
							"taking diff=%s asOf=%s stopSeq=%d arrival=%d departure=%d",
							diff, ste.asOf, ste.stu.GetStopSequence(),
							ste.stu.GetArrival().GetTime(), ste.stu.GetDeparture().GetTime(),
						)
					}

					tu.StopTimeUpdate = append(tu.StopTimeUpdate, ste.stu)
				} else if debug {
					log.Printf(
						"too new diff=%s asOf=%s stopSeq=%d arrival=%d departure=%d",
						diff, ste.asOf, ste.stu.GetStopSequence(),
						ste.stu.GetArrival().GetTime(), ste.stu.GetDeparture().GetTime(),
					)
				}
			}

			sort.Slice(tu.StopTimeUpdate, func(i, j int) bool {
				return tu.StopTimeUpdate[i].GetStopSequence() < tu.StopTimeUpdate[j].GetStopSequence()
			})

			if len(tu.StopTimeUpdate) > 0 {
				out.Entity = append(out.Entity, &gtfsrt.FeedEntity{
					TripUpdate: tu,
				})
			}
		}

		wj(w, out)
	}))

	log.Printf("ready")
	log.Fatal(http.ListenAndServe("127.0.0.1:5000", gziphandler.GzipHandler(mx)))
}

func wj(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.Encode(v)
}

package api

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
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
		fus, err := getFinalUpdates(hist, r)
		if err != nil {
			if serr, ok := err.(statusError); ok {
				http.Error(w, err.Error(), serr.status)
			} else {
				http.Error(w, "unknown error", http.StatusInternalServerError)
			}
			return
		}

		wj(w, fus)
	}))

	mx.Get("/final-updates.csv", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/csv")

		tz := st.Agencies[0].Timezone

		fum, err := getFinalUpdates(hist, r)
		if err != nil {
			if serr, ok := err.(statusError); ok {
				http.Error(w, err.Error(), serr.status)
			} else {
				http.Error(w, "unknown error", http.StatusInternalServerError)
			}
			return
		}

		cw := csv.NewWriter(w)

		cw.Write([]string{"service_date", "trip_id", "route_id", "vehicle_id", "stop_id", "sched_arrival", "actual_arrival", "sched_departure", "actual_departure"})
		for _, ent := range fum.GetEntity() {
			tu := ent.GetTripUpdate()
			if tu == nil {
				continue
			}

			serviceDate, err := parseDateAtNoonInLocation(tu.GetTrip().GetStartDate(), tz)
			if err != nil {
				http.Error(w, "error parsing trip start date", http.StatusInternalServerError)
				return
			}

			startTime, err := parseTimeAsDuration(tu.GetTrip().GetStartTime())
			if err != nil {
				http.Error(w, "error parsing trip start time", http.StatusInternalServerError)
				return
			}

			if startTime >= 24*time.Hour {
				serviceDate = serviceDate.AddDate(0, 0, -1)
			}

			var (
				tripID       = tu.GetTrip().GetTripId()
				routeID      = tu.GetTrip().GetRouteId()
				vehLabel     = tu.GetVehicle().GetLabel()
				serviceDateS = serviceDate.Format("20060102")
			)

			for _, stu := range tu.GetStopTimeUpdate() {
				var (
					stopID = stu.GetStopId()

					scharr string
					actarr string
					schdep string
					actdep string
				)

				log.Println(len(st.StopIDsToStopTimes), stopID, len(st.StopIDsToStopTimes[stopID]))
				for _, sst := range st.StopIDsToStopTimes[stopID] {
					if sst.TripID != tripID {
						continue
					}

					scharr = serviceDate.Add(sst.ArrivalTime).Format("15:04:05")
					schdep = serviceDate.Add(sst.DepartureTime).Format("15:04:05")

					break
				}

				if t := stu.GetArrival().GetTime(); t > 0 {
					actarr = time.Unix(t, 0).In(tz).Format("15:04:05")
				}

				if t := stu.GetDeparture().GetTime(); t > 0 {
					actdep = time.Unix(t, 0).In(tz).Format("15:04:05")
				}

				cw.Write([]string{
					serviceDateS,
					tripID,
					routeID,
					vehLabel,
					stopID,
					scharr,
					actarr,
					schdep,
					actdep,
				})
			}

			cw.Flush()
		}

	}))

	log.Printf("ready")
	log.Fatal(http.ListenAndServe("0.0.0.0:5000", gziphandler.GzipHandler(mx)))
}

type statusError struct {
	msg    string
	status int
}

func (s statusError) Error() string {
	return s.msg
}

func getFinalUpdates(hist history, r *http.Request) (*gtfsrt.FeedMessage, error) {
	// How long before an update is considered definitive.
	const stabilityWait = 5 * time.Minute

	sts, ets := r.URL.Query().Get("startTime"), r.URL.Query().Get("endTime")
	if sts == "" || ets == "" {
		return nil, statusError{"need startTime and endTime", http.StatusBadRequest}
	}

	st, err := time.Parse(time.RFC3339, sts)
	if err != nil {
		return nil, statusError{"startTime parse error: " + err.Error(), http.StatusBadRequest}
	}
	st = st.Truncate(time.Minute)

	et, err := time.Parse(time.RFC3339, ets)
	if err != nil {
		return nil, statusError{"endTime parse error: " + err.Error(), http.StatusBadRequest}
	}
	et = et.Truncate(time.Minute)

	if !et.After(st) {
		return nil, statusError{"endTime must be after startTime", http.StatusBadRequest}
	}

	if et.Sub(st) > 4*time.Hour {
		return nil, statusError{"endTime must be no more than 4 hours after startTime", http.StatusBadRequest}
	}

	// Subtract 2 * stable time from startTime so we can pick up things that left early.
	st = st.Add(-2 * stabilityWait)

	// Add stable time to endTime so we can pick up things that became final.
	et = et.Add(stabilityWait)

	var (
		tripIDs  = r.URL.Query()["tripID"]
		routeIDs = r.URL.Query()["routeID"]
		stopIDs  = r.URL.Query()["stopID"]
	)

	var minutes []time.Time
	for m := st; m.Before(et) || m.Equal(et); m = m.Add(time.Minute) {
		minutes = append(minutes, m)
	}

	type hejob struct {
		i   int
		min time.Time
	}

	type heres struct {
		i   int
		he  HistoryEntry
		err error
	}

	jch, rch := make(chan hejob), make(chan heres, len(minutes))
	defer close(jch)

	for i := 0; i < 10; i++ {
		go func() {
			for j := range jch {
				he, err := hist.GetAsOf("TripUpdates", j.min)
				hr := heres{
					i:   j.i,
					he:  he,
					err: err,
				}
				rch <- hr
			}
		}()

	}

	for i, m := range minutes {
		jch <- hejob{
			i:   i,
			min: m,
		}
	}

	hes := make([]HistoryEntry, len(minutes))
	for range minutes {
		r := <-rch
		if r.err != nil {
			log.Println(r.err)
			return nil, statusError{"error fetching entry", http.StatusInternalServerError}
		}
		log.Printf("fetched minute=%s i=%d", minutes[r.i], r.i)
		hes[r.i] = r.he
	}

	type tustage struct {
		tu   *gtfsrt.TripUpdate
		stus map[string]*gtfsrt.TripUpdate_StopTimeUpdate
	}

	stage := make(map[string]tustage)

	for _, he := range hes {
		for _, e := range he.Entry.GetEntity() {
			tu := e.GetTripUpdate()
			if tu == nil {
				continue
			}

			if !contains(tripIDs, tu.GetTrip().GetTripId()) || !contains(routeIDs, tu.GetTrip().GetRouteId()) {
				continue
			}

			tkey := tu.GetTrip().GetStartDate() + "-" + tu.GetTrip().GetTripId()
			tus, ok := stage[tkey]
			if !ok {
				tus = tustage{
					tu:   tu,
					stus: make(map[string]*gtfsrt.TripUpdate_StopTimeUpdate),
				}
			}

			for _, stu := range tu.GetStopTimeUpdate() {
				if !contains(stopIDs, stu.GetStopId()) {
					continue
				}

				tus.stus[tkey+"-"+stu.GetStopId()] = stu
			}

			stage[tkey] = tus
		}
	}

	out := new(gtfsrt.FeedMessage)
	for _, tus := range stage {
		tu := tus.tu
		tu.StopTimeUpdate = nil

		for _, stu := range tus.stus {
			it := stu.GetDeparture().GetTime()
			if it == 0 {
				it = stu.GetArrival().GetTime()
			}
			itt := time.Unix(it, 0)

			itu64 := uint64(it)
			if itu64 > tu.GetTimestamp() {
				tu.Timestamp = &itu64
			}

			if diff := et.Sub(itt); diff < stabilityWait {
				continue
			}

			tu.StopTimeUpdate = append(tu.StopTimeUpdate, stu)
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

	return out, nil
}

func wj(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.Encode(v)
}

func contains(h []string, n string) bool {
	// Nothing in h means allow all.
	if len(h) == 0 {
		return true
	}
	for _, x := range h {
		if x == n {
			return true
		}
	}
	return false
}

func parseDateAtNoonInLocation(ds string, loc *time.Location) (time.Time, error) {
	d, err := time.ParseInLocation("20060102 15:04:05", ds+" 12:00:00", loc)
	if err != nil {
		return d, err
	}
	return d.Add(-(12 * time.Hour)), nil
}

func parseTimeAsDuration(ts string) (time.Duration, error) {
	parts := strings.Split(ts, ":")
	if len(parts) != 3 {
		return 0, fmt.Errorf("time %q not in h:m:s format", ts)
	}

	h, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, err
	}
	m, err := strconv.Atoi(parts[1])
	if err != nil {
		return 0, err
	}
	s, err := strconv.Atoi(parts[2])
	if err != nil {
		return 0, err
	}
	return time.Hour*time.Duration(h) + time.Minute*time.Duration(m) + time.Second*time.Duration(s), nil
}

package api

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

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

	log.Printf("ready")
	log.Fatal(http.ListenAndServe("0.0.0.0:5000", mx))
}

func wj(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.Encode(v)
}

package planner

import (
	"sort"
	"time"

	"github.com/danp/catchbus/gtfs"
	"github.com/danp/catchbus/gtfs/gtfsrt"
	"github.com/danp/catchbus/gtfs/gtfsrt/feed"
)

type Planner struct {
	Static *gtfs.Static
	Feed   *feed.Feed
}

func (p *Planner) DeparturesForStop(stop *gtfs.Stop, targetTime time.Time) []Departure {
	tz := p.Static.Agencies[0].Timezone
	targetTime = targetTime.In(tz)
	targetNoonMinus12h := gtfs.AtNoonMinus12h(targetTime, tz)
	targetYestNoonMinus12h := gtfs.AtNoonMinus12h(targetNoonMinus12h.Add(-24*time.Hour), tz)

	targetActiveServices := p.Static.ActiveServicesForDate(targetNoonMinus12h)
	targetYestActiveServices := p.Static.ActiveServicesForDate(targetYestNoonMinus12h)

	targetTripIDs := p.Static.TripIDsForServiceIDs(targetActiveServices)
	targetYestTripIDs := p.Static.TripIDsForServiceIDs(targetYestActiveServices)

	type stopTimeWithServiceDateAndRealTime struct {
		st gtfs.StopTime
		sd time.Time
		dt time.Time
	}

	var (
		targetStartTime = targetTime.Add(-(5 * time.Minute))
		targetEndTime   = targetTime.Add(2 * time.Hour)
	)

	var stopTimes []stopTimeWithServiceDateAndRealTime
	for _, s := range p.Static.StopIDsToStopTimes[stop.ID] {
		if _, ok := targetTripIDs[s.TripID]; ok {
			dt := targetNoonMinus12h.Add(s.DepartureTime)
			if dt.After(targetStartTime) && dt.Before(targetEndTime) {
				stopTimes = append(stopTimes, stopTimeWithServiceDateAndRealTime{s, targetNoonMinus12h, dt})
			}
		}

		if _, ok := targetYestTripIDs[s.TripID]; ok {
			ydt := targetYestNoonMinus12h.Add(s.DepartureTime)
			if ydt.After(targetStartTime) && ydt.Before(targetEndTime) {
				stopTimes = append(stopTimes, stopTimeWithServiceDateAndRealTime{s, targetYestNoonMinus12h, ydt})
			}
		}
	}
	sort.Slice(stopTimes, func(i, j int) bool { return stopTimes[i].dt.Before(stopTimes[j].dt) })

	deps := make([]Departure, 0, len(stopTimes))
	for _, s := range stopTimes {
		tr := p.Static.TripIDsToTrips[s.st.TripID]
		rt := p.Static.RouteIDsToRoutes[tr.RouteID]

		dep := Departure{
			Route:         *rt,
			Trip:          *tr,
			StopTime:      s.st,
			DepartureTime: s.dt,
		}

		dep.StopTimeUpdate = p.Feed.StopTimeUpdateForTripAndStop(s.st.TripID, stop.ID)
		dep.VehiclePosition = p.Feed.VehiclePositionForTrip(s.st.TripID)

		if dep.StopTimeUpdate != nil {
			dep.Estimated = true
			dep.DepartureTime = time.Unix(int64(dep.StopTimeUpdate.GetDeparture().GetTime()), 0)
		}

		deps = append(deps, dep)
	}

	return deps
}

type Departure struct {
	Route    gtfs.Route
	Trip     gtfs.Trip
	StopTime gtfs.StopTime

	StopTimeUpdate  *gtfsrt.TripUpdate_StopTimeUpdate
	VehiclePosition *gtfsrt.VehiclePosition

	Estimated     bool
	DepartureTime time.Time
}

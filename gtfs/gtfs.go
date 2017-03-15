package gtfs

import (
	"fmt"
	"sort"
	"time"

	"github.com/pkg/errors"
)

type Static struct {
	Agencies      []Agency
	Stops         []Stop
	Routes        []Route
	Trips         []Trip
	StopTimes     []StopTime
	Calendar      []Calendar
	CalendarDates []CalendarDate
	Shapes        []Shape

	RouteIDsToRoutes   map[string]*Route
	StopIDsToStops     map[string]*Stop
	StopIDsToStopTimes map[string][]StopTime
	TripIDsToTrips     map[string]*Trip
}

func (s *Static) FillMaps() {
	ridtort := make(map[string]*Route)
	for _, rt := range s.Routes {
		rt := rt
		ridtort[rt.ID] = &rt
	}
	s.RouteIDsToRoutes = ridtort

	tidtotrp := make(map[string]*Trip)
	for _, tr := range s.Trips {
		tr := tr
		tidtotrp[tr.ID] = &tr
	}
	s.TripIDsToTrips = tidtotrp

	sidtostp := make(map[string]*Stop)
	for _, st := range s.Stops {
		st := st
		sidtostp[st.ID] = &st
	}
	s.StopIDsToStops = sidtostp

	s.StopIDsToStopTimes = makeStopIDsToStopTimes(s.StopTimes)
}

func (s *Static) ActiveServicesForDate(d time.Time) map[string]bool {
	out := make(map[string]bool)

	for _, c := range s.Calendar {
		if (d.Equal(c.StartDate) || d.After(c.StartDate)) &&
			(d.Equal(c.EndDate) || d.Before(c.EndDate)) {
			var active bool
			switch d.Weekday() {
			case time.Monday:
				active = c.Monday
			case time.Tuesday:
				active = c.Tuesday
			case time.Wednesday:
				active = c.Wednesday
			case time.Thursday:
				active = c.Thursday
			case time.Friday:
				active = c.Friday
			case time.Saturday:
				active = c.Saturday
			case time.Sunday:
				active = c.Sunday
			}
			if active {
				out[c.ServiceID] = true
			}
		}
	}

	for _, c := range s.CalendarDates {
		if !d.Equal(c.Date) {
			continue
		}

		switch c.ExceptionType {
		case "1":
			out[c.ServiceID] = true
		case "2":
			delete(out, c.ServiceID)
		}
	}

	return out
}

func (s *Static) CalendarForServiceID(serviceID string) (Calendar, error) {
	for _, c := range s.Calendar {
		if c.ServiceID == serviceID {
			return c, nil
		}
	}
	return Calendar{}, errors.New("calendar not found")
}

func (s *Static) TripIDsForServiceIDs(serviceIDs map[string]bool) map[string]bool {
	out := make(map[string]bool)
	for _, t := range s.Trips {
		if _, ok := serviceIDs[t.ServiceID]; !ok {
			continue
		}
		out[t.ID] = true
	}
	return out
}

func (s *Static) TripIDsForRouteID(routeID string) map[string]bool {
	out := make(map[string]bool)
	for _, t := range s.Trips {
		if t.RouteID == routeID {
			out[t.ID] = true
		}
	}
	return out
}

type Point struct {
	Lat float64
	Lon float64
}

func (p Point) String() string {
	return fmt.Sprintf("%f,%f", p.Lat, p.Lon)
}

var NoPoint = Point{-4242, -4242}

type Agency struct {
	ID       string
	Name     string
	URL      string
	Timezone *time.Location
	Lang     string
	Phone    string
	FareURL  string
	Email    string
}

type Stop struct {
	ID                 string
	Code               string
	Name               string
	Desc               string
	Point              Point
	ZoneID             string
	URL                string
	LocationType       int
	ParentStation      string
	Timezone           string
	WheelchairBoarding int
}

type Route struct {
	ID        string
	AgencyID  string
	ShortName string
	LongName  string
	Desc      string
	Type      int
	URL       string
	Color     string
	TextColor string
}

type Trip struct {
	RouteID              string
	ServiceID            string
	ID                   string
	Headsign             string
	ShortName            string
	DirectionID          int
	BlockID              string
	ShapeID              string
	WheelchairAccessible int
	BikesAllowed         int
}

const NoShapeDistTraveled = float64(-42.42)

type StopTime struct {
	TripID            string
	ArrivalTime       time.Duration
	DepartureTime     time.Duration
	StopID            string
	StopSequence      int
	StopHeadsign      string
	PickupType        int
	DropOffType       int
	ShapeDistTraveled float64
	Timepoint         int
}

type Calendar struct {
	ServiceID string
	Monday    bool
	Tuesday   bool
	Wednesday bool
	Thursday  bool
	Friday    bool
	Saturday  bool
	Sunday    bool
	StartDate time.Time
	EndDate   time.Time
}

type CalendarDate struct {
	ServiceID     string
	Date          time.Time
	ExceptionType string
}

type Shape struct {
	ID           string
	Point        Point
	PtSequence   int
	DistTraveled float64
}

func AtNoonMinus12h(t time.Time, loc *time.Location) time.Time {
	y, m, d := t.Date()
	return time.Date(y, m, d, 12, 0, 0, 0, loc).Add(-(12 * time.Hour))
}

func makeStopIDsToStopTimes(sts []StopTime) map[string][]StopTime {
	sort.Slice(sts, func(i, j int) bool { return sts[i].StopID < sts[j].StopID })

	// index them by stop ID
	out := make(map[string][]StopTime)

	var (
		stopStart  int
		lastStopID string
	)

	if len(sts) > 0 {
		lastStopID = sts[0].StopID
	}

	for i, st := range sts {
		if st.StopID != lastStopID || i == len(sts)-1 {
			out[lastStopID] = sts[stopStart:i]
			stopStart = i
		}
		lastStopID = st.StopID
	}

	return out
}

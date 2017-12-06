package gtfs

import (
	"archive/zip"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

func ReadZipFile(path string) (*Static, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	st, err := f.Stat()
	if err != nil {
		return nil, err
	}

	return ReadZip(f, st.Size())
}

func ReadZip(r io.ReaderAt, size int64) (*Static, error) {
	zr, err := zip.NewReader(r, size)
	if err != nil {
		return nil, err
	}

	out := &Static{}

	if err := readFile(zr, out, "agency.txt", agencyHandler); err != nil {
		return nil, err
	}

	if len(out.Agencies) == 0 {
		return nil, errors.New("need at least one agency in agencies.txt")
	}

	if err := readFile(zr, out, "stops.txt", stopHandler); err != nil {
		return nil, err
	}

	if err := readFile(zr, out, "routes.txt", routeHandler); err != nil {
		return nil, err
	}

	if err := readFile(zr, out, "trips.txt", tripHandler); err != nil {
		return nil, err
	}

	if err := readFile(zr, out, "stop_times.txt", stopTimeHandler); err != nil {
		return nil, err
	}

	if err := readFile(zr, out, "calendar.txt", calendarHandler); err != nil {
		return nil, err
	}

	if err := readFile(zr, out, "calendar_dates.txt", calendarDateHandler); err != nil {
		return nil, err
	}

	if err := readFile(zr, out, "shapes.txt", shapeHandler); err != nil {
		return nil, err
	}

	return out, nil
}

type fileHandler func(out *Static, rm map[string]string) error

func readFile(zr *zip.Reader, out *Static, fn string, h fileHandler) error {
	zf := findFile(zr.File, fn)
	if zf == nil {
		return errors.New(fn + " not found in zip")
	}

	f, err := zf.Open()
	if err != nil {
		return err
	}

	return read(f, out, h)
}

func read(r io.ReadCloser, out *Static, h fileHandler) error {
	defer r.Close()

	cr := csv.NewReader(r)

	hr, err := cr.Read()
	if err != nil {
		return err
	}

	for {
		rec, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		rm := make(map[string]string)
		for i, hf := range hr {
			rm[hf] = rec[i]
		}

		if err := h(out, rm); err != nil {
			return err
		}
	}

	return nil
}

func findFile(file []*zip.File, fn string) *zip.File {
	for _, f := range file {
		if f.Name == fn {
			return f
		}
	}
	return nil
}

func agencyHandler(out *Static, rm map[string]string) error {
	var a Agency
	a.ID = rm["agency_id"]
	a.Name = rm["agency_name"]
	a.URL = rm["agency_url"]
	a.Lang = rm["agency_lang"]
	a.Phone = rm["agency_phone"]
	a.FareURL = rm["agency_fare_url"]
	a.Email = rm["agency_email"]

	loc, err := time.LoadLocation(rm["agency_timezone"])
	if err != nil {
		return err
	}
	a.Timezone = loc

	for _, oa := range out.Agencies {
		if oa.Timezone.String() != a.Timezone.String() {
			return fmt.Errorf("agencies %s (%s) and %s (%s) have different time zones",
				oa.Name, oa.Timezone,
				a.Name, a.Timezone)
		}
	}

	out.Agencies = append(out.Agencies, a)
	return nil
}

func stopHandler(out *Static, rm map[string]string) error {
	var s Stop
	s.ID = rm["stop_id"]
	s.Code = rm["stop_code"]
	s.Name = rm["stop_name"]
	s.Desc = rm["stop_desc"]
	s.ZoneID = rm["zone_id"]
	s.URL = rm["stop_url"]
	s.ParentStation = rm["parent_station"]
	s.Timezone = rm["stop_timezone"]

	pt, err := parsePoint(rm["stop_lat"], rm["stop_lon"])
	if err != nil {
		return err
	}
	s.Point = pt

	lt, err := strconv.Atoi(rm["location_type"])
	if err != nil {
		return err
	}
	s.LocationType = lt

	if _, ok := rm["wheelchair_boarding"]; ok {
		wb, err := strconv.Atoi(rm["wheelchair_boarding"])
		if err != nil {
			return err
		}
		s.WheelchairBoarding = wb
	}

	out.Stops = append(out.Stops, s)
	return nil
}

func routeHandler(out *Static, rm map[string]string) error {
	var r Route
	r.ID = rm["route_id"]
	r.AgencyID = rm["agency_id"]
	r.ShortName = rm["route_short_name"]
	r.LongName = rm["route_long_name"]
	r.Desc = rm["route_desc"]
	r.URL = rm["route_url"]
	r.Color = rm["route_color"]
	r.TextColor = rm["text_color"]

	rt, err := strconv.Atoi(rm["route_type"])
	if err != nil {
		return err
	}
	r.Type = rt

	out.Routes = append(out.Routes, r)
	return nil
}

func tripHandler(out *Static, rm map[string]string) error {
	var t Trip
	t.ID = rm["trip_id"]
	t.RouteID = rm["route_id"]
	t.ServiceID = rm["service_id"]
	t.Headsign = rm["trip_headsign"]
	t.ShortName = rm["trip_short_name"]
	t.BlockID = rm["block_id"]
	t.ShapeID = rm["shape_id"]

	di, err := strconv.Atoi(rm["direction_id"])
	if err != nil {
		return err
	}
	t.DirectionID = di

	if was := rm["wheelchair_accessible"]; was != "" {
		wai, err := strconv.Atoi(was)
		if err != nil {
			return err
		}
		t.WheelchairAccessible = wai
	}

	if bas := rm["bikes_allowed"]; bas != "" {
		bai, err := strconv.Atoi(bas)
		if err != nil {
			return err
		}
		t.BikesAllowed = bai
	}

	out.Trips = append(out.Trips, t)
	return nil
}

func stopTimeHandler(out *Static, rm map[string]string) error {
	var s StopTime
	s.TripID = rm["trip_id"]

	at, err := parseTimeAsDuration(rm["arrival_time"])
	if err != nil {
		return err
	}
	s.ArrivalTime = at

	dt, err := parseTimeAsDuration(rm["departure_time"])
	if err != nil {
		return err
	}
	s.DepartureTime = dt

	s.StopID = rm["stop_id"]
	s.StopHeadsign = rm["stop_headsign"]

	ssi, err := strconv.Atoi(rm["stop_sequence"])
	if err != nil {
		return err
	}
	s.StopSequence = ssi

	if pts := rm["pickup_type"]; pts != "" {
		pti, err := strconv.Atoi(pts)
		if err != nil {
			return err
		}
		s.PickupType = pti
	}

	if dts := rm["drop_off_type"]; dts != "" {
		dti, err := strconv.Atoi(dts)
		if err != nil {
			return err
		}
		s.DropOffType = dti
	}

	if sds := rm["shape_dist_traveled"]; sds != "" {
		sdf, err := strconv.ParseFloat(sds, 64)
		if err != nil {
			return err
		}
		s.ShapeDistTraveled = sdf
	} else {
		s.ShapeDistTraveled = NoShapeDistTraveled
	}

	ts := rm["timepoint"]
	if ts == "" {
		// empty: Times are considered exact, which is the same as 1
		ts = "1"
	}
	ti, err := strconv.Atoi(ts)
	if err != nil {
		return err
	}
	s.Timepoint = ti

	out.StopTimes = append(out.StopTimes, s)
	return nil
}

var numsToDays = map[int]string{
	0: "monday",
	1: "tuesday",
	2: "wednesday",
	3: "thursday",
	4: "friday",
	5: "saturday",
	6: "sunday",
}

func calendarHandler(out *Static, rm map[string]string) error {
	// Guaranteed to have at least one Agency.
	tz := out.Agencies[0].Timezone

	var c Calendar
	c.ServiceID = rm["service_id"]

	sd, err := parseDateAtNoonInLocation(rm["start_date"], tz)
	if err != nil {
		return err
	}
	c.StartDate = sd

	ed, err := parseDateAtNoonInLocation(rm["end_date"], tz)
	if err != nil {
		return err
	}
	c.EndDate = ed

	for i, p := range []*bool{&c.Monday, &c.Tuesday, &c.Wednesday, &c.Thursday, &c.Friday, &c.Saturday, &c.Sunday} {
		*p = rm[numsToDays[i]] == "1"
	}

	out.Calendar = append(out.Calendar, c)
	return nil
}

func calendarDateHandler(out *Static, rm map[string]string) error {
	// Guaranteed to have at least one Agency.
	tz := out.Agencies[0].Timezone

	var c CalendarDate
	c.ServiceID = rm["service_id"]

	d, err := parseDateAtNoonInLocation(rm["date"], tz)
	if err != nil {
		return err
	}
	c.Date = d

	c.ExceptionType = rm["exception_type"]

	out.CalendarDates = append(out.CalendarDates, c)
	return nil
}

func shapeHandler(out *Static, rm map[string]string) error {
	var s Shape
	s.ID = rm["shape_id"]

	s.Point = NoPoint
	if rm["shape_pt_lat"] != "" && rm["shape_pt_lon"] != "" {
		pt, err := parsePoint(rm["shape_pt_lat"], rm["shape_pt_lon"])
		if err != nil {
			return err
		}
		s.Point = pt
	}

	si, err := strconv.Atoi(rm["shape_pt_sequence"])
	if err != nil {
		return err
	}
	s.PtSequence = si

	if sds := rm["shape_dist_traveled"]; sds != "" {
		sdf, err := strconv.ParseFloat(sds, 64)
		if err != nil {
			return err
		}
		s.DistTraveled = sdf
	} else {
		s.DistTraveled = NoShapeDistTraveled
	}

	out.Shapes = append(out.Shapes, s)
	return nil
}

func parsePoint(lat, lon string) (Point, error) {
	var p Point

	latf, err := strconv.ParseFloat(lat, 64)
	if err != nil {
		return p, err
	}

	lonf, err := strconv.ParseFloat(lon, 64)
	if err != nil {
		return p, err
	}

	return Point{Lat: latf, Lon: lonf}, nil
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

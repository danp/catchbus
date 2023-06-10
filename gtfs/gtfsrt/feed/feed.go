package feed

import (
	"io"
	"log"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/danp/catchbus/gtfs/gtfsrt"
	"google.golang.org/protobuf/proto"
)

// DefaultInterval is the default interval between feed fetches.
// It is used when Feed.Interval is zero.
const DefaultInterval = 10 * time.Second

type Feed struct {
	TripUpdatesURL      string
	VehiclePositionsURL string
	AlertsURL           string

	// Interval specifies how often to fetch the feed data.
	// An Interval of zero means the DefaultInterval will be used.
	Interval time.Duration

	tripUpdates      atomic.Value
	vehiclePositions atomic.Value
	alerts           atomic.Value
}

func (f *Feed) Start() {
	if f.Interval == 0 {
		f.Interval = DefaultInterval
	}

	go f.monitor()
}

func (f *Feed) CurrentTripUpdates() *gtfsrt.FeedMessage {
	c := f.tripUpdates.Load()
	if c == nil {
		return nil
	}
	return c.(*gtfsrt.FeedMessage)
}

func (f *Feed) CurrentVehiclePositions() *gtfsrt.FeedMessage {
	c := f.vehiclePositions.Load()
	if c == nil {
		return nil
	}
	return c.(*gtfsrt.FeedMessage)
}

func (f *Feed) CurrentAlerts() *gtfsrt.FeedMessage {
	c := f.alerts.Load()
	if c == nil {
		return nil
	}
	return c.(*gtfsrt.FeedMessage)
}

func (f *Feed) StopTimeUpdateForTripAndStop(tripID, stopID string) *gtfsrt.TripUpdate_StopTimeUpdate {
	tus := f.CurrentTripUpdates()

	for _, t := range tus.GetEntity() {
		tu := t.GetTripUpdate()
		if tu.GetTrip().GetTripId() == tripID {
			for _, su := range tu.GetStopTimeUpdate() {
				if su.GetStopId() == stopID {
					return su
				}
			}
		}
	}

	return nil
}

func (f *Feed) VehiclePositionForTrip(tripID string) *gtfsrt.VehiclePosition {
	vps := f.CurrentVehiclePositions()

	for _, p := range vps.GetEntity() {
		v := p.GetVehicle()
		if v.GetTrip().GetTripId() == tripID {
			return v
		}
	}

	return nil
}

func (f *Feed) monitor() {
	tick := time.NewTicker(f.Interval)
	defer tick.Stop()

	for {
		f.fetch()
		<-tick.C
	}
}

func (f *Feed) fetch() {
	for _, t := range []struct {
		v   *atomic.Value
		url string
	}{
		{&f.tripUpdates, f.TripUpdatesURL},
		{&f.vehiclePositions, f.VehiclePositionsURL},
		{&f.alerts, f.AlertsURL},
	} {
		if t.url == "" {
			continue
		}

		m, err := f.get(t.url)
		if err != nil {
			// logged by get
			continue
		}

		t.v.Store(m)
	}
}

var client = &http.Client{
	Timeout: 10 * time.Second,
	Transport: &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	},
}

func (f *Feed) get(url string) (*gtfsrt.FeedMessage, error) {
	resp, err := client.Get(url)
	if err != nil {
		log.Printf("ns=feed fn=get url=%q at=err err=%q", url, err)
		return nil, err
	}
	defer resp.Body.Close()

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("ns=feed fn=get url=%q at=err err=%q", url, err)
		return nil, err
	}

	m := new(gtfsrt.FeedMessage)
	if err := proto.Unmarshal(b, m); err != nil {
		log.Printf("ns=feed fn=get url=%q at=err err=%q", url, err)
		return nil, err
	}

	age := int(time.Since(time.Unix(int64(m.GetHeader().GetTimestamp()), 0)).Seconds())
	log.Printf("ns=feed fn=get url=%q at=done items=%d age=%d", url, len(m.GetEntity()), age)
	return m, nil
}

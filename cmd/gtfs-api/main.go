package main

import (
	"flag"
	"log"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/danp/catchbus/api"
	"github.com/danp/catchbus/gtfs"
	"github.com/danp/catchbus/gtfs/gtfsrt/feed"
	"github.com/danp/catchbus/planner"
)

var (
	zipPath = flag.String("zip", "google_transit.zip", "path to GTFS zip file to load")

	tripUpdatesURL      = flag.String("trip-updates-url", "", "URL for GTFS-RT trip updates")
	vehiclePositionsURL = flag.String("vehicle-positions-url", "", "URL for GTFS-RT vehicle positions")
	alertsURL           = flag.String("alerts-url", "", "URL for GTFS-RT alerts")
)

func main() {
	flag.Parse()

	st, err := gtfs.ReadZipFile(*zipPath)
	if err != nil {
		log.Fatal(err)
	}
	st.FillMaps()

	fd := &feed.Feed{
		TripUpdatesURL:      *tripUpdatesURL,
		VehiclePositionsURL: *vehiclePositionsURL,
		AlertsURL:           *alertsURL,
	}
	fd.Start()

	sess := session.Must(session.NewSession())

	hist := &history{
		S3:     s3.New(sess),
		Bucket: "hfxtransit-gtfs-archive",
	}

	pl := &planner.Planner{
		Static: st,
		Feed:   fd,
	}

	api.Start(st, pl, fd, hist)
}

type history struct {
	S3     s3iface.S3API
	Bucket string
}

func (h *history) GetEntriesBetween(kind string, start, end time.Time) ([]api.HistoryEntry, error) {
	return nil, nil
}

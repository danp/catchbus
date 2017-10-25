package main

import (
	"compress/gzip"
	"flag"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/danp/catchbus/api"
	"github.com/danp/catchbus/gtfs"
	"github.com/danp/catchbus/gtfs/gtfsrt"
	"github.com/danp/catchbus/gtfs/gtfsrt/feed"
	"github.com/danp/catchbus/planner"
	"github.com/golang/protobuf/proto"
)

var (
	zipPath = flag.String("zip", "google_transit.zip", "path to GTFS zip file to load, or URL")

	tripUpdatesURL      = flag.String("trip-updates-url", "", "URL for GTFS-RT trip updates")
	vehiclePositionsURL = flag.String("vehicle-positions-url", "", "URL for GTFS-RT vehicle positions")
	alertsURL           = flag.String("alerts-url", "", "URL for GTFS-RT alerts")
)

func main() {
	flag.Parse()

	if _, err := url.Parse(*zipPath); err == nil {
		*zipPath = fetchZip(*zipPath)
	}

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
		S3:     s3.New(sess, &aws.Config{Region: aws.String("us-east-1")}),
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
	var entries []api.HistoryEntry

	resp, err := h.S3.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(h.Bucket),
		Key:    aws.String("TripUpdates/2017/10/25/00/TripUpdates-2017-10-25T00-09-02Z.pb.gz"),
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	gzr, err := gzip.NewReader(resp.Body)
	if err != nil {
		return nil, err
	}

	b, err := ioutil.ReadAll(gzr)
	if err != nil {
		return nil, err
	}

	m := new(gtfsrt.FeedMessage)
	if err := proto.Unmarshal(b, m); err != nil {
		return nil, err
	}

	entries = append(entries, api.HistoryEntry{
		Time:  time.Now(),
		Entry: m,
	})

	return entries, nil
}

func fetchZip(zurl string) string {
	f, err := ioutil.TempFile("", "gtfs-zip")
	if err != nil {
		log.Fatal(err)
	}

	resp, err := http.Get(zurl)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	if _, err := io.Copy(f, resp.Body); err != nil {
		log.Fatal(err)
	}

	return f.Name()
}

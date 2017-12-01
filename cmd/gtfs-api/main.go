package main

import (
	"compress/gzip"
	"errors"
	"flag"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"sort"
	"strings"
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

	if u, err := url.Parse(*zipPath); err == nil && (u.Scheme == "http" || u.Scheme == "https") {
		*zipPath = fetchZip(*zipPath)
	}

	st, err := gtfs.ReadZipFile(*zipPath)
	if err != nil {
		log.Fatal(err)
	}
	// st.FillMaps()

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

func (h *history) GetAsOf(kind string, ts time.Time) (api.HistoryEntry, error) {
	var entry api.HistoryEntry

	prefix := kind + "/" + ts.Format("2006/01/02/15") + "/" + kind + "-" + ts.Format("2006-01-02T15-04-")

	req := &s3.ListObjectsInput{
		Bucket: aws.String(h.Bucket),
		Prefix: aws.String(prefix),
	}

	lr, err := h.S3.ListObjects(req)
	if err != nil {
		return entry, err
	}

	if len(lr.Contents) == 0 {
		return entry, errors.New("nothing found")
	}

	// Get newest key first in the slice.
	sort.Slice(lr.Contents, func(i, j int) bool { return *lr.Contents[i].Key > *lr.Contents[j].Key })
	key := lr.Contents[0].Key

	resp, err := h.S3.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(h.Bucket),
		Key:    key,
	})
	if err != nil {
		return entry, err
	}
	defer resp.Body.Close()

	var r io.Reader = resp.Body
	if strings.HasSuffix(*key, ".gz") {
		gzr, err := gzip.NewReader(resp.Body)
		if err != nil {
			return entry, err
		}
		defer gzr.Close()
		r = gzr
	}

	b, err := ioutil.ReadAll(r)
	if err != nil {
		return entry, err
	}

	m := new(gtfsrt.FeedMessage)
	if err := proto.Unmarshal(b, m); err != nil {
		return entry, err
	}

	entry = api.HistoryEntry{
		Time:  time.Unix(int64(m.GetHeader().GetTimestamp()), 0).UTC(),
		Entry: m,
	}

	return entry, nil
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

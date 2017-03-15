package main

import (
	"bytes"
	"flag"
	"log"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/danp/catchbus/gtfs/gtfsrt"
	"github.com/danp/catchbus/gtfs/gtfsrt/feed"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
)

const archiveInterval = 5 * time.Second

var (
	s3BucketName = flag.String("s3-bucket-name", "", "S3 bucket for archived feeds")

	tripUpdatesURL      = flag.String("trip-updates-url", "", "URL for GTFS-RT trip updates")
	vehiclePositionsURL = flag.String("vehicle-positions-url", "", "URL for GTFS-RT vehicle positions")
	alertsURL           = flag.String("alerts-url", "", "URL for GTFS-RT alerts")

	s3c *s3.S3
)

func main() {
	flag.Parse()
	if *s3BucketName == "" {
		log.Fatal("need s3 bucket name")
	}
	if *tripUpdatesURL == "" && *vehiclePositionsURL == "" && *alertsURL == "" {
		log.Fatal("need at least one feed URL")
	}

	sess, err := session.NewSession()
	if err != nil {
		log.Fatalln("error setting up S3 session:", err)
	}
	s3c = s3.New(sess, &aws.Config{Region: aws.String("us-east-1")})

	fd := &feed.Feed{
		TripUpdatesURL:      *tripUpdatesURL,
		VehiclePositionsURL: *vehiclePositionsURL,
		AlertsURL:           *alertsURL,

		Interval: archiveInterval,
	}
	fd.Start()

	sources := []*source{
		{name: "TripUpdates", get: fd.CurrentTripUpdates},
		{name: "VehiclePositions", get: fd.CurrentVehiclePositions},
		{name: "Alerts", get: fd.CurrentAlerts},
	}

	tick := time.NewTicker(archiveInterval)
	for {
		archive(sources)
		<-tick.C
	}
}

func archive(sources []*source) {
	for _, s := range sources {
		if err := s.archive(); err != nil {
			log.Printf("fn=archive source=%s at=error err=%q", s, err)
		}
	}
}

type source struct {
	name       string
	get        func() *gtfsrt.FeedMessage
	lastUpdate time.Time
}

func (s *source) String() string {
	return s.name
}

const (
	pathFormat = "2006/01/02/15"
	fnFormat   = "2006-01-02T15-04-05Z"
)

func (s *source) archive() error {
	d := s.get()
	if len(d.GetEntity()) == 0 {
		return nil
	}

	ts := time.Unix(int64(d.GetHeader().GetTimestamp()), 0).UTC()
	if !ts.After(s.lastUpdate) {
		return nil
	}

	path := s.name + "/" + ts.Format(pathFormat) + "/" + s.name + "-" + ts.Format(fnFormat) + ".pb"

	b, err := proto.Marshal(d)
	if err != nil {
		return errors.Wrap(err, "marshaling data")
	}

	_, err = s3c.PutObject(&s3.PutObjectInput{
		Body:   bytes.NewReader(b),
		Bucket: s3BucketName,
		Key:    aws.String(path),
	})
	if err != nil {
		return errors.Wrap(err, "archiving to S3")
	}

	s.lastUpdate = ts
	return nil
}

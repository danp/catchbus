package main

import (
	"encoding/json"
	"io"
	"log"
	"os"

	"github.com/danp/catchbus/gtfs/gtfsrt"
	"google.golang.org/protobuf/proto"
)

func main() {
	var rc io.ReadCloser

	if len(os.Args) > 1 {
		f, err := os.Open(os.Args[1])
		if err != nil {
			log.Fatal(err)
		}
		rc = f
	} else {
		rc = os.Stdin
	}

	b, err := io.ReadAll(rc)
	if err != nil {
		log.Fatal(err)
	}
	defer rc.Close()

	m := new(gtfsrt.FeedMessage)
	if err := proto.Unmarshal(b, m); err != nil {
		log.Fatal(err)
	}

	if err := json.NewEncoder(os.Stdout).Encode(m); err != nil {
		log.Fatal(err)
	}
}

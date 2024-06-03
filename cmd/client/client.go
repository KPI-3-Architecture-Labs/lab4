package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

var target = flag.String("target", "http://localhost:8081", "request target")

func main() {
	flag.Parse()
	client := new(http.Client)
	client.Timeout = 10 * time.Second

	for range time.Tick(1 * time.Second) {
		resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data?key=teamye", *target))
		if err == nil {
			log.Printf("response %d", resp.StatusCode)
		} else {
			log.Printf("error %s", err)
		}
		defer resp.Body.Close()

		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Printf("error reading response body: %s", err)
			continue
		}

		log.Printf("response %d: %s", resp.StatusCode, body)
	}
}

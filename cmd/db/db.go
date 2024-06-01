package main

import (
	"encoding/json"
	"flag"
	"github.com/KPI-3-Architecture-Labs/lab4/datastore"
	"github.com/KPI-3-Architecture-Labs/lab4/httptools"
	"github.com/KPI-3-Architecture-Labs/lab4/signal"
	"io/ioutil"
	"log"
	"net/http"
)

var port = flag.Int("port", 8083, "server port")

type Request struct {
	Value string `json:"value"`
}

type Response struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func main() {
	flag.Parse()

	dir, err := ioutil.TempDir("", "temp-dir")
	if err != nil {
		log.Fatal(err)
	}

	db, err := datastore.NewDb(dir, 250)
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	h := http.NewServeMux()

	h.HandleFunc("/db/", func(rw http.ResponseWriter, req *http.Request) {
		key := req.URL.Path[4:]

		switch req.Method {
		case http.MethodGet:

			value, err := db.Get(key)

			if err != nil {
				rw.WriteHeader(http.StatusNotFound)
				json.NewEncoder(rw).Encode(map[string]string{"error 404": "Not found"})
				return
			}

			resp := Response{
				Key:   key,
				Value: value,
			}

			rw.Header().Set("Content-Type", "application/json")

			rw.WriteHeader(http.StatusOK)

			_ = json.NewEncoder(rw).Encode(resp)

		case http.MethodPost:
			var body Request

			err := json.NewDecoder(req.Body).Decode(&body)

			if err != nil {
				rw.WriteHeader(http.StatusBadRequest)
				json.NewEncoder(rw).Encode(map[string]string{"error": "Bad request"})
				return
			}

			err = db.Put(key, body.Value)

			if err != nil {
				rw.WriteHeader(http.StatusInternalServerError)
				json.NewEncoder(rw).Encode(map[string]string{"error": "InternalServerError"})
				return
			}

			rw.WriteHeader(http.StatusCreated)

		default:
			rw.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(rw).Encode(map[string]string{"error": "Bad request"})
		}
	})

	server := httptools.CreateServer(*port, h)

	server.Start()

	signal.WaitForTerminationSignal()
}

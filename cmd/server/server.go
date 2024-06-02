package main

import (
	"bytes"
	"encoding/json"
	"github.com/KPI-3-Architecture-Labs/lab4/httptools"
	"github.com/KPI-3-Architecture-Labs/lab4/signal"
	"log"
	"net/http"
	"time"
)

func main() {
	dbURL := "http://localhost:8083"

	saveCurrentDate(dbURL, "teamye")

	http.HandleFunc("/api/v1/some-data", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")

		if key == "" {
			http.Error(w, "Key required", http.StatusBadRequest)
			return
		}

		resp, err := http.Get(dbURL + "/db/" + key)

		if err != nil {
			http.Error(w, "Service is not available", http.StatusServiceUnavailable)
			return
		}

		defer resp.Body.Close()

		if resp.StatusCode == http.StatusNotFound {
			http.NotFound(w, r)
			return
		}

		var data map[string]string
		if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
			http.Error(w, "Error while decoding response", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")

		json.NewEncoder(w).Encode(data)
	})

	server := httptools.CreateServer(8081, nil)
	server.Start()
	signal.WaitForTerminationSignal()
}

func saveCurrentDate(dbURL, teamKey string) {
	currentDate := time.Now().Format("2006-01-02")
	data := map[string]string{
		"value": currentDate,
	}
	jsonData, err := json.Marshal(data)
	if err != nil {
		log.Fatal("JSON marshalling error:", err)
	}

	_, err = http.Post(dbURL+"/db/"+teamKey, "application/json", bytes.NewBuffer(jsonData))

	if err != nil {
		log.Fatal("Error saving current date:", err)
	}
}

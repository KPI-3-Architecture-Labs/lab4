package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/KPI-3-Architecture-Labs/lab4/httptools"
	"github.com/KPI-3-Architecture-Labs/lab4/signal"
)

var (
	port       = flag.Int("port", 8090, "load balancer port")
	timeoutSec = flag.Int("timeout-sec", 3, "request timeout time in seconds")
	https      = flag.Bool("https", false, "whether backends support HTTPs")

	traceEnabled = flag.Bool("trace", false, "whether to include tracing information into responses")
)

var (
	timeout     = time.Duration(*timeoutSec) * time.Second
	serversPool = []string{
		"server1:8080",
		"server2:8080",
		"server3:8080",
	}
	poolOfHealthyServers = make([]string, len(serversPool))
	poolLock             sync.Mutex

	bytesServed     = make(map[string]int64)
	bytesServedLock sync.Mutex
)

func scheme() string {
	if *https {
		return "https"
	}
	return "http"
}

func health(dst string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("%s://%s/health", scheme(), dst), nil)
	if err != nil {
		return false
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return false
	}
	return true
}

func forward(dst string, rw http.ResponseWriter, r *http.Request) error {
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()
	fwdRequest := r.Clone(ctx)
	fwdRequest.RequestURI = ""
	fwdRequest.URL.Host = dst
	fwdRequest.URL.Scheme = scheme()
	fwdRequest.Host = dst

	resp, err := http.DefaultClient.Do(fwdRequest)
	if err != nil {
		log.Printf("Failed to get response from %s: %s", dst, err)
		rw.WriteHeader(http.StatusServiceUnavailable)
		return err
	}
	defer resp.Body.Close()

	for k, values := range resp.Header {
		for _, value := range values {
			rw.Header().Add(k, value)
		}
	}

	if *traceEnabled {
		rw.Header().Set("lb-from", dst)
	}

	log.Println("fwd", resp.StatusCode, resp.Request.URL)
	rw.WriteHeader(resp.StatusCode)

	bytes, err := io.Copy(rw, resp.Body)
	if err != nil {
		log.Printf("Failed to write response: %s", err)
	}

	bytesServedLock.Lock()
	bytesServed[dst] += bytes
	bytesServedLock.Unlock()

	return nil
}

func getSmallestTraffic() int {
	bytesServedLock.Lock()
	defer bytesServedLock.Unlock()

	var minBytes int64 = -1
	minIndex := -1

	for i, server := range poolOfHealthyServers {
		if server == "" {
			continue
		}
		if minBytes == -1 || bytesServed[server] < minBytes {
			minBytes = bytesServed[server]
			minIndex = i
		}
	}

	return minIndex
}

func getServer(index int) string {
	poolLock.Lock()
	defer poolLock.Unlock()
	return poolOfHealthyServers[index]
}

func healthCheck(servers []string, result []string) {
	healthStatus := make(map[string]bool)
	for _, server := range servers {
		healthStatus[server] = true
	}

	for i, server := range servers {
		i := i
		go func(server string) {
			for range time.Tick(10 * time.Second) {
				isHealthy := health(server)
				poolLock.Lock()

				if isHealthy {
					healthStatus[server] = true
					result[i] = server
				} else {
					healthStatus[server] = false
					result[i] = ""
				}

				poolOfHealthyServers = nil

				for _, server := range servers {
					if healthStatus[server] {
						poolOfHealthyServers = append(poolOfHealthyServers, server)
					}
				}

				poolLock.Unlock()
				log.Println(server, isHealthy)
			}
		}(server)
	}
}

func main() {
	flag.Parse()

	healthCheck(serversPool, poolOfHealthyServers)

	frontend := httptools.CreateServer(*port, http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		serverIndex := getSmallestTraffic()
		if serverIndex == -1 {
			http.Error(rw, "No available servers", http.StatusServiceUnavailable)
			return
		}
		dst := getServer(serverIndex)
		err := forward(dst, rw, r)
		if err != nil {
			return
		}
	}))

	log.Println("Starting load balancer...")
	log.Printf("Tracing support enabled: %t", *traceEnabled)
	frontend.Start()
	signal.WaitForTerminationSignal()
}

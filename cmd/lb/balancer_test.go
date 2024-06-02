package main

import (
	"github.com/stretchr/testify/assert"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"
)

func Test(t *testing.T) {
	suite := new(TestSuite)
	suite.SetupSuite()
	t.Run("TestBalancer", suite.TestBalancer)
	t.Run("TestHealth", suite.TestHealth)
}

type TestSuite struct {
	serversPool []string
}

func (s *TestSuite) SetupSuite() {
	s.serversPool = []string{
		"server1:8080",
		"server2:80",
		"server3:80",
	}
}

func (s *TestSuite) TestBalancer(t *testing.T) {
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server1.Close()

	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server2.Close()

	server3 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server3.Close()

	parsedURL1, _ := url.Parse(server1.URL)
	hostURL1 := parsedURL1.Host

	parsedURL2, _ := url.Parse(server2.URL)
	hostURL2 := parsedURL2.Host

	parsedURL3, _ := url.Parse(server3.URL)
	hostURL3 := parsedURL3.Host

	serversPool = []string{
		hostURL1,
		hostURL2,
		hostURL3,
	}

	poolOfHealthyServers = []string{
		hostURL1,
		hostURL2,
	}

	bytesServed = make(map[string]int64)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()

	err := forward(hostURL1, w, req)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, w.Code)

	bytesServedLock.Lock()
	assert.GreaterOrEqual(t, bytesServed[hostURL1], int64(0))
	bytesServedLock.Unlock()
}

func (s *TestSuite) TestHealth(t *testing.T) {
	serverURL := os.Getenv("SERVER_URL")
	if serverURL == "" {
		serverURL = "http://localhost:8080"
	}

	resp, err := http.Get(serverURL + "/health")
	if err != nil {
		t.Fatalf("Failed to make request to %s: %s", serverURL+"/health", err)
	}
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode, "Health check failed for the main server")
}

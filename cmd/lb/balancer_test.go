package main

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test(t *testing.T) {
	suite := new(TestSuite)
	suite.SetupSuite()
	t.Run("TestBalancer", suite.TestBalancer)
	t.Run("TestHealth", suite.TestHealth)
	t.Run("TestTraffic", suite.TestGetSmallestTraffic)
	t.Run("TestTraffic2", suite.TestGetSmallestTrafficTwo)
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

func (s *TestSuite) TestGetSmallestTraffic(t *testing.T) {
	poolOfHealthyServers = []string{"server1:8080", "server2:8080", "server3:8080"}
	bytesServed = map[string]int64{
		"server1:8080": 100,
		"server2:8080": 50,
		"server3:8080": 150,
	}

	expectedIndex := 1
	actualIndex := getSmallestTraffic()
	assert.Equal(t, expectedIndex, actualIndex)
}

func (s *TestSuite) TestGetSmallestTrafficTwo(t *testing.T) {
	poolOfHealthyServers = []string{"server1:8080", "server2:8080", "server3:8080"}
	bytesServed = map[string]int64{
		"server1:8080": 1488,
		"server2:8080": 5252,
		"server3:8080": 228,
	}

	expectedIndex := 2
	actualIndex := getSmallestTraffic()
	assert.Equal(t, expectedIndex, actualIndex)
}

func (s *TestSuite) TestHealth(t *testing.T) {
	result := make([]string, len(s.serversPool))

	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server1.Close()

	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server2.Close()

	parsedURL1, _ := url.Parse(server1.URL)
	hostURL1 := parsedURL1.Host

	parsedURL2, _ := url.Parse(server2.URL)
	hostURL2 := parsedURL2.Host

	servers := []string{
		hostURL1,
		hostURL2,
		"server3:8080",
	}

	healthCheck(servers, result)
	time.Sleep(12 * time.Second)

	assert.Equal(t, hostURL1, result[0])
	assert.Equal(t, hostURL2, result[1])
	assert.Equal(t, "", result[2])
}

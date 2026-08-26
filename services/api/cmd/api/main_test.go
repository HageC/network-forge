package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestStatelessSimulationEndpoint(t *testing.T) {
	body := `{"nodes":[{"id":"source","kind":"client","concurrency":1,"queueCapacity":1,"serviceTimeUs":0}],"edges":[],"workload":{"source":"source","requestsPerSecond":1,"durationSeconds":1,"seed":42}}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/simulations", strings.NewReader(body))
	rec := httptest.NewRecorder()
	newHandler(nil).ServeHTTP(rec, req)
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"generated":1`) {
		t.Fatalf("response: %d %s", rec.Code, rec.Body.String())
	}
}
func TestValidationEndpoint(t *testing.T) {
	handler := newHandler(nil)
	valid := `{"nodes":[{"id":"s","kind":"service","concurrency":1}],"edges":[],"workload":{"source":"s","requestsPerSecond":1,"durationSeconds":1}}`
	for _, test := range []struct {
		body   string
		status int
	}{{valid, 200}, {`{"nodes":[],"edges":[],"workload":{"source":"missing","requestsPerSecond":1,"durationSeconds":1}}`, 400}} {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/validate", strings.NewReader(test.body)))
		if rec.Code != test.status {
			t.Fatalf("validation status=%d body=%s", rec.Code, rec.Body.String())
		}
	}
}
func TestProjectsRequireDatabase(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/v1/projects", nil)
	rec := httptest.NewRecorder()
	newHandler(nil).ServeHTTP(rec, req)
	if rec.Code != 503 {
		t.Fatalf("status: %d", rec.Code)
	}
}
func TestExperimentEndpoint(t *testing.T) {
	body := `{"simulation":{"nodes":[{"id":"source","kind":"client","concurrency":1,"queueCapacity":1,"serviceTimeUs":0}],"edges":[],"workload":{"source":"source","requestsPerSecond":1,"durationSeconds":1,"seed":42}},"repetitions":3,"sloP95Us":1000}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/experiments", strings.NewReader(body))
	rec := httptest.NewRecorder()
	newHandler(nil).ServeHTTP(rec, req)
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"repetitions":3`) {
		t.Fatalf("response: %d %s", rec.Code, rec.Body.String())
	}
}
func TestComparisonEndpoint(t *testing.T) {
	model := `{"nodes":[{"id":"source","kind":"client","concurrency":1,"queueCapacity":1,"serviceTimeUs":0}],"edges":[],"workload":{"source":"source","requestsPerSecond":1,"durationSeconds":1,"seed":42}}`
	body := `{"baseline":` + model + `,"candidate":` + model + `,"repetitions":2,"sloP95Us":1000,"objective":"throughput"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/comparisons", strings.NewReader(body))
	rec := httptest.NewRecorder()
	newHandler(nil).ServeHTTP(rec, req)
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"winner":"tie"`) {
		t.Fatalf("response: %d %s", rec.Code, rec.Body.String())
	}
}

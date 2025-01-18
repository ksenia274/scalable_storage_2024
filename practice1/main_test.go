package main

import (
	"bytes"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
)

func TestSimple(t *testing.T) {
	mux := http.NewServeMux()

	s := NewStorage(mux, "test", []string{}, true)
	go func() { s.Run() }()

	r := NewRouter(mux, [][]string{{"test"}})
	go func() { r.Run() }()
	t.Cleanup(r.Stop)

	testCases := []struct {
		name       string
		method     string
		endpoint   string
		body       []byte
		wantStatus int
	}{
		{
			name:     "insert",
			method:   http.MethodPost,
			endpoint: "/insert",
			body: func() []byte {
				feature := geojson.NewFeature(orb.Point{rand.Float64(), rand.Float64()})
				body, err := feature.MarshalJSON()
				if err != nil {
					t.Fatal(err)
				}
				return body
			}(),
			wantStatus: http.StatusOK,
		},
		{
			name:     "replace",
			method:   http.MethodPost,
			endpoint: "/replace",
			body: func() []byte {
				feature := geojson.NewFeature(orb.Point{rand.Float64(), rand.Float64()})
				feature.Properties["id"] = "test_id"
				body, err := feature.MarshalJSON()
				if err != nil {
					t.Fatal(err)
				}
				return body
			}(),
			wantStatus: http.StatusOK,
		},
		{
			name:     "delete",
			method:   http.MethodPost,
			endpoint: "/delete",
			body: func() []byte {
				feature := geojson.NewFeature(orb.Point{rand.Float64(), rand.Float64()})
				feature.Properties["id"] = "test_id"
				body, err := feature.MarshalJSON()
				if err != nil {
					t.Fatal(err)
				}
				return body
			}(),
			wantStatus: http.StatusOK,
		},
		{
			name:       "select",
			method:     http.MethodGet,
			endpoint:   "/select",
			wantStatus: http.StatusOK,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest(tc.method, tc.endpoint, bytes.NewReader(tc.body))
			if err != nil {
				t.Fatal(err)
			}

			rr := httptest.NewRecorder()
			mux.ServeHTTP(rr, req)

			if rr.Code == http.StatusTemporaryRedirect {
				req, err := http.NewRequest(tc.method, rr.Header().Get("location"), bytes.NewReader(tc.body))
				if err != nil {
					t.Fatal(err)
				}

				rr = httptest.NewRecorder()
				mux.ServeHTTP(rr, req)

				if rr.Code != tc.wantStatus {
					t.Errorf("handler returned wrong status code: got %v want %v", rr.Code, tc.wantStatus)
				}
			} else if rr.Code != tc.wantStatus {
				t.Errorf("handler returned wrong status code: got %v want %v", rr.Code, tc.wantStatus)
			}
		})
	}
}

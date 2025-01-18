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

func TestGeoJSONStorageWithReplicas(t *testing.T) {
	mux := http.NewServeMux()

	s := NewStorage(mux, "test", "checkpoint.json", "journal.json")
	go func() { s.Run() }()

	r := NewRouter(mux, [][]string{{"test"}})
	go func() { r.Run() }()
	t.Cleanup(func() {
		r.Stop()
		s.Stop()
	})

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
			endpoint: "/test/insert",
			body: func() []byte {
				feature := geojson.NewFeature(orb.Point{rand.Float64(), rand.Float64()})
				body, err := feature.MarshalJSON()
				if err != nil {
					t.Fatal(err)
				}
				return body
			}(),
			wantStatus: http.StatusCreated,
		},
		{
			name:     "replace",
			method:   http.MethodPost,
			endpoint: "/test/replace",
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
			endpoint: "/test/delete",
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
			endpoint:   "/test/select",
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
				req, err := http.NewRequest(tc.method, rr.Header().Get("Location"), bytes.NewReader(tc.body))
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

	t.Run("replica tests", func(t *testing.T) {
		replicaEndpoint := "/test/replica" // Предполагаемое конечное устройство для работы с репликами

		replicaTestCases := []struct {
			name       string
			method     string
			body       []byte
			wantStatus int
		}{
			{
				name:   "replica insert",
				method: http.MethodPost,
				body: func() []byte {
					feature := geojson.NewFeature(orb.Point{rand.Float64(), rand.Float64()})
					body, err := feature.MarshalJSON()
					if err != nil {
						t.Fatal(err)
					}
					return body
				}(),
				wantStatus: http.StatusCreated,
			},
			{
				name:   "replica replace",
				method: http.MethodPost,
				body: func() []byte {
					feature := geojson.NewFeature(orb.Point{rand.Float64(), rand.Float64()})
					feature.Properties["id"] = "test_id_replica"
					body, err := feature.MarshalJSON()
					if err != nil {
						t.Fatal(err)
					}
					return body
				}(),
				wantStatus: http.StatusOK,
			},
			{
				name:   "replica delete",
				method: http.MethodPost,
				body: func() []byte {
					feature := geojson.NewFeature(orb.Point{rand.Float64(), rand.Float64()})
					feature.Properties["id"] = "test_id_replica"
					body, err := feature.MarshalJSON()
					if err != nil {
						t.Fatal(err)
					}
					return body
				}(),
				wantStatus: http.StatusOK,
			},
			{
				name:       "replica select",
				method:     http.MethodGet,
				wantStatus: http.StatusOK,
			},
		}

		for _, rc := range replicaTestCases {
			t.Run(rc.name, func(t *testing.T) {
				req, err := http.NewRequest(rc.method, replicaEndpoint, bytes.NewReader(rc.body))
				if err != nil {
					t.Fatal(err)
				}

				rr := httptest.NewRecorder()
				mux.ServeHTTP(rr, req)

				if rr.Code == http.StatusTemporaryRedirect {
					req, err := http.NewRequest(rc.method, rr.Header().Get("Location"), bytes.NewReader(rc.body))
					if err != nil {
						t.Fatal(err)
					}

					rr = httptest.NewRecorder()
					mux.ServeHTTP(rr, req)

					if rr.Code != rc.wantStatus {
						t.Errorf("handler returned wrong status code for replica test: got %v want %v", rr.Code, rc.wantStatus)
					}
				} else if rr.Code != rc.wantStatus {
					t.Errorf("handler returned wrong status code for replica test: got %v want %v", rr.Code, rc.wantStatus)
				}
			})
		}
	})
}

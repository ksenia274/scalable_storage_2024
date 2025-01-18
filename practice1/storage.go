package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log/slog"
	"net/http"
	"os"
	"sync"

	"github.com/google/uuid"
	"github.com/paulmach/orb/geojson"
)

type Storage struct {
	mux    *http.ServeMux
	name   string
	data   map[string]*geojson.Feature
	mu     sync.RWMutex
	dbPath string
}

func NewStorage(mux *http.ServeMux, name string, replicas []string, leader bool) *Storage {
	dbPath := "geo.db.json"
	storage := &Storage{
		mux:    mux,
		name:   name,
		data:   make(map[string]*geojson.Feature),
		dbPath: dbPath,
	}

	storage.loadFromDB()

	mux.HandleFunc("/"+name+"/select", storage.selectHandler)
	mux.HandleFunc("/"+name+"/insert", storage.insertHandler)
	mux.HandleFunc("/"+name+"/replace", storage.replaceHandler)
	mux.HandleFunc("/"+name+"/delete", storage.deleteHandler)

	return storage
}

func (s *Storage) Run() {
	// Ничего не делаем
}

func (s *Storage) Stop() {
	s.saveToDB()
}

func (s *Storage) loadFromDB() {
	_, err := os.Stat(s.dbPath)
	if os.IsNotExist(err) {
		slog.Info("geo.db.json does not exist. Creating new one")
		s.saveToDB()
		return
	}

	file, err := os.Open(s.dbPath)
	if err != nil {
		slog.Error("failed to open db file", "error", err)
		return
	}
	defer file.Close()

	data, err := ioutil.ReadAll(file)
	if err != nil {
		slog.Error("failed to read db file", "error", err)
		return
	}

	if len(data) == 0 {
		return // empty file
	}

	fc := &geojson.FeatureCollection{}
	if err := json.Unmarshal(data, fc); err != nil {
		slog.Error("failed to unmarshal db file", "error", err)
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	for _, feature := range fc.Features {
		if id, ok := feature.Properties["id"].(string); ok {
			s.data[id] = feature
		}
	}
}

func (s *Storage) saveToDB() {
	s.mu.RLock()
	defer s.mu.RUnlock()
	features := make([]*geojson.Feature, 0, len(s.data))
	for _, feature := range s.data {
		features = append(features, feature)
	}

	// Правильное создание FeatureCollection
	fc := geojson.NewFeatureCollection()
	fc.Features = features

	data, err := json.MarshalIndent(fc, "", "  ")
	if err != nil {
		slog.Error("failed to marshal db data", "error", err)
		return
	}

	err = ioutil.WriteFile(s.dbPath, data, 0644)
	if err != nil {
		slog.Error("failed to write db file", "error", err)
	}
}

func (s *Storage) selectHandler(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	features := make([]*geojson.Feature, 0, len(s.data))
	for _, feature := range s.data {
		features = append(features, feature)
	}

	// Правильное создание FeatureCollection
	fc := geojson.NewFeatureCollection()
	fc.Features = features

	w.Header().Set("Content-Type", "application/json")
	err := json.NewEncoder(w).Encode(fc)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to marshal geojson: %s", err.Error()), http.StatusInternalServerError)
		return
	}
}

func (s *Storage) insertHandler(w http.ResponseWriter, r *http.Request) {
	feature, err := s.getFeatureFromBody(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	id := uuid.New().String()
	feature.Properties["id"] = id
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[id] = feature

	w.WriteHeader(http.StatusOK)

}

func (s *Storage) replaceHandler(w http.ResponseWriter, r *http.Request) {
	feature, err := s.getFeatureFromBody(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	id, ok := feature.Properties["id"].(string)
	if !ok || id == "" {
		http.Error(w, "id must be defined for replace operation", http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.data[id]; !ok {
		http.Error(w, fmt.Sprintf("feature with id %s not found", id), http.StatusNotFound)
		return
	}

	s.data[id] = feature
	w.WriteHeader(http.StatusOK)
}

func (s *Storage) deleteHandler(w http.ResponseWriter, r *http.Request) {
	feature, err := s.getFeatureFromBody(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	id, ok := feature.Properties["id"].(string)
	if !ok || id == "" {
		http.Error(w, "id must be defined for delete operation", http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, id)
	w.WriteHeader(http.StatusOK)

}

func (s *Storage) getFeatureFromBody(r *http.Request) (*geojson.Feature, error) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read request body: %w", err)
	}

	feature := &geojson.Feature{}
	if err := json.Unmarshal(body, feature); err != nil {
		return nil, fmt.Errorf("failed to unmarshal geojson: %w", err)
	}

	if feature.Properties == nil {
		feature.Properties = make(map[string]interface{})
	}

	return feature, nil
}

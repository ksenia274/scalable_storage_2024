package main

import (
 "context"
 "encoding/json"
 "errors"
 "fmt"
 "io/ioutil"
 "log/slog"
 "net/http"
 "os"
 "os/signal"
 "sync"
 "syscall"
 "time"

 "github.com/google/uuid"
 "github.com/paulmach/orb/geojson"
 "github.com/tidwall/rtree"
)

type Transaction struct {
 Action  string      json:"action"
 Name    string      json:"name"
 LSN     uint64      json:"lsn"
 Feature interface{} json:"feature" // GeoJSON feature
}

type Engine struct {
 mu          sync.Mutex
 transactions []Transaction
 index       *rtree.Rtree
 lsn         uint64
 checkpointFile string
 journalFile string
}

func NewEngine(checkpointFile, journalFile string) *Engine {
 return &Engine{
  transactions: []Transaction{},
  index:       rtree.New(2),
  checkpointFile: checkpointFile,
  journalFile: journalFile,
 }
}

func (e *Engine) loadCheckpoint() {
 file, err := os.Open(e.checkpointFile)
 if err != nil {
  fmt.Println("Error opening checkpoint file:", err)
  return
 }
 defer file.Close()

 if err := json.NewDecoder(file).Decode(&e.transactions); err != nil {
  fmt.Println("Error decoding checkpoint file:", err)
 }
}

func (e *Engine) applyTransaction(txn Transaction) {
	e.mu.Lock()
	defer e.mu.Unlock()
   
	e.lsn++
	txn.LSN = e.lsn
	e.transactions = append(e.transactions, txn)
   
	if txn.Action == "insert" || txn.Action == "replace" {
	 // Приводим txn.Feature к типу geojson.Feature
	 feature, ok := txn.Feature.(*geojson.Feature)
	 if !ok {
	  fmt.Println("Error: txn.Feature is not of type *geojson.Feature")
	  return
	 }
   
	 if feature.Geometry != nil {
	  switch feature.Geometry.Type {
	  case "Point":
	   if len(feature.Geometry.Coordinates) == 2 {
		e.index.Insert(rtree.Rect{
		 Min: [2]float64{feature.Geometry.Coordinates[0], feature.Geometry.Coordinates[1]},
		 Max: [2]float64{feature.Geometry.Coordinates[0], feature.Geometry.Coordinates[1]},
		}, txn)
	   }
	  case "Polygon":
	   if len(feature.Geometry.Coordinates) > 0 {
		minX, minY := feature.Geometry.Coordinates[0][0][0], feature.Geometry.Coordinates[0][0][1]
		maxX, maxY := minX, minY
		for _, ring := range feature.Geometry.Coordinates[0] {
		 for _, coord := range ring {
		  if coord[0] < minX {
		   minX = coord[0]
		  }
		  if coord[0] > maxX {
		   maxX = coord[0]
		  }
		  if coord[1] < minY {
		   minY = coord[1]
		  }
		  if coord[1] > maxY {
		   maxY = coord[1]
		  }
		 }
		}
		e.index.Insert(rtree.Rect{
		 Min: [2]float64{minX, minY},
		 Max: [2]float64{maxX, maxY},
		}, txn)
	   }
	  default:
	   fmt.Println("Unsupported geometry type:", feature.Geometry.Type)
	  }
	 } else {
	  fmt.Println("Error: geometry is nil in feature")
	 }
	}
   }
   

type Storage struct {
 mux    *http.ServeMux
 name   string
 data   map[string]*geojson.Feature
 mu     sync.RWMutex
 dbPath string
 engine *Engine
}

func NewStorage(mux *http.ServeMux, name string, checkpointFile, journalFile string) *Storage {
 dbPath := "geo.db.json"
 engine := NewEngine(checkpointFile, journalFile)

 storage := &Storage{
  mux:    mux,
  name:   name,
  data:   make(map[string]*geojson.Feature),
  dbPath: dbPath,
  engine: engine,
 }

 storage.loadFromDB()

 mux.HandleFunc("/"+name+"/select", storage.selectHandler)
 mux.HandleFunc("/"+name+"/insert", storage.insertHandler)
 mux.HandleFunc("/"+name+"/replace", storage.replaceHandler)
 mux.HandleFunc("/"+name+"/delete", storage.deleteHandler)

 return storage
}

func (s *Storage) loadFromDB() {
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
   s.engine.applyTransaction(Transaction{
    Action:  "insert",
    Name:    id,
    Feature: feature,
   })
  }
 }
}

func (s *Storage) saveToDB() {
 s.mu.RLock()
 defer s.mu.RUnlock()
 
 fc := geojson.NewFeatureCollection()
 for _, feature := range s.data {
  fc.Features = append(fc.Features, feature)
 }

 data, err := json.MarshalIndent(fc, "", "  ")
 if err != nil {
  slog.Error("failed to marshal db data", "error", err)
  return
 }

 if err := ioutil.WriteFile(s.dbPath, data, 0644); err != nil {
  slog.Error("failed to save db data", "error", err)
 }
}

// HTTP Handlers for Storage (select, insert, replace, delete)
func (s *Storage) selectHandler(w http.ResponseWriter, r *http.Request) {
 s.mu.RLock()
 defer s.mu.RUnlock()

 results := make([]*geojson.Feature, 0, len(s.data))
 for _, feature := range s.data {
  results = append(results, feature)
 }

 w.Header().Set("Content-Type", "application/json")
 json.NewEncoder(w).Encode(results)
}

func (s *Storage) insertHandler(w http.ResponseWriter, r *http.Request) {
 var feature geojson.Feature
 if err := json.NewDecoder(r.Body).Decode(&feature); err != nil {
  http.Error(w, "Invalid input", http.StatusBadRequest)
  return
 }
 
 id := uuid.New().String()
 if feature.Properties == nil {
  feature.Properties = make(map[string]interface{})
 }
 feature.Properties["id"] = id

 s.mu.Lock()
 s.data[id] = &feature
 s.engine.applyTransaction(Transaction{
  Action:  "insert",
  Name:    id,
  Feature: &feature,
 })
 s.mu.Unlock()

 w.WriteHeader(http.StatusCreated)
 json.NewEncoder(w).Encode(feature)
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

    s.engine.applyTransaction(Transaction{
        Action:  "replace",
        Name:    id,
        Feature: feature,
    })

    s.data[id] = feature

    w.WriteHeader(http.StatusOK)
    json.NewEncoder(w).Encode(feature) 
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

    if _, ok := s.data[id]; !ok {
        http.Error(w, fmt.Sprintf("feature with id %s not found", id), http.StatusNotFound)
        return
    }

    s.engine.applyTransaction(Transaction{
        Action:  "delete",
        Name:    id,
        Feature: s.data[id],
    })

    delete(s.data, id)

    w.WriteHeader(http.StatusOK)
}

type Router struct {
 mux   *http.ServeMux
 nodes [][]string
}

func NewRouter(mux *http.ServeMux, nodes [][]string) *Router {
 result := Router{
  mux:   mux,
  nodes: nodes,
 }

 mux.Handle("/", http.FileServer(http.Dir("./front/dist")))
 mux.Handle("/select", http.RedirectHandler("/"+nodes[0][0]+"/select", http.StatusTemporaryRedirect))
 mux.Handle("/insert", http.RedirectHandler("/"+nodes[0][0]+"/insert", http.StatusTemporaryRedirect))
 mux.Handle("/replace", http.RedirectHandler("/"+nodes[0][0]+"/replace", http.StatusTemporaryRedirect))
 mux.Handle("/delete", http.RedirectHandler("/"+nodes[0][0]+"/delete", http.StatusTemporaryRedirect))
 return &result
}

func (r *Router) Run() {}

func (r *Router) Stop() {}

func main() {
 mux := http.NewServeMux()

 storage := NewStorage(mux, "storage", "checkpoint.json", "journal.json")
 go func() { storage.Run() }()

 router := NewRouter(mux, [][]string{{"storage"}})
 go func() { router.Run() }()

 l := http.Server{}
 l.Addr = "127.0.0.1:8080"
 l.Handler = mux

 go func() {
  sigs := make(chan os.Signal, 1)
  signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
  sig := <-sigs
  slog.Info("got signal", "signal", sig)
  
        ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
        defer cancel()
        l.Shutdown(ctx)
    }()

 defer func() {
        router.Stop()
        storage.Stop()
        slog.Info("we are going down")
    }()

 slog.Info("listen http://" + l.Addr)
 err := l.ListenAndServe()
 if !errors.Is(err, http.ErrServerClosed) {
        slog.Info("err", "err", err)
    }
}

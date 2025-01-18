package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/paulmach/orb/geojson"
	"github.com/tidwall/rtree"
)

type Transaction struct {
	Action  string      `json:"action"`
	Name    string      `json:"name"`
	LSN     uint64      `json:"lsn"`
	Feature interface{} `json:"feature"`
	NodeID  string      `json:"node_id"`
}

type Engine struct {
	mu             sync.Mutex
	transactions   []Transaction
	index          *rtree.Rtree
	lsn            uint64
	checkpointFile string
	journalFile    string
	nodeID         string
	vclock         map[string]uint64
	replicas       []string
	isLeader       bool
	connMu         sync.Mutex
	replicaConns   map[string]*websocket.Conn
}

func NewEngine(checkpointFile, journalFile string, nodeID string, replicas []string, isLeader bool) *Engine {
	return &Engine{
		transactions:   []Transaction{},
		index:          rtree.New(2),
		checkpointFile: checkpointFile,
		journalFile:    journalFile,
		nodeID:         nodeID,
		vclock:         make(map[string]uint64),
		replicas:       replicas,
		isLeader:       isLeader,
		replicaConns:   make(map[string]*websocket.Conn),
	}
}

func (e *Engine) Run() {
	e.loadCheckpoint()
	e.loadJournal()

	if e.isLeader {
		e.connectToReplicas()
	}
	go e.startReplicationServer()
}

func (e *Engine) Stop() {
	e.disconnectReplicas()
	e.saveCheckpoint()
	e.saveJournal()
}

func (e *Engine) loadCheckpoint() {
	file, err := os.Open(e.checkpointFile)
	if err != nil {
		fmt.Println("Error opening checkpoint file:", err)
		return
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	err = decoder.Decode(&e.transactions)
	if err != nil {
		fmt.Println("Error decoding transactions from checkpoint file:", err)
		return
	}
	err = decoder.Decode(&e.vclock)
	if err != nil {
		if !errors.Is(err, http.ErrBodyEOF) {
			fmt.Println("Error decoding vclock from checkpoint file:", err)
		}
	}
	for _, txn := range e.transactions {
		if txn.LSN > e.lsn {
			e.lsn = txn.LSN
		}
		e.applyIndex(txn)
	}
	fmt.Println("Checkpoint loaded. Last LSN:", e.lsn, "Vector Clock:", e.vclock)

}

func (e *Engine) saveCheckpoint() {
	file, err := os.Create(e.checkpointFile)
	if err != nil {
		slog.Error("failed to open checkpoint file for writing", "error", err)
		return
	}
	defer file.Close()

	enc := json.NewEncoder(file)
	if err := enc.Encode(e.transactions); err != nil {
		slog.Error("failed to encode transactions to checkpoint file", "error", err)
	}
	if err := enc.Encode(e.vclock); err != nil {
		slog.Error("failed to encode vclock to checkpoint file", "error", err)
	}
	fmt.Println("Checkpoint saved. LSN:", e.lsn, "Vector Clock:", e.vclock)
}

func (e *Engine) loadJournal() {
	file, err := os.Open(e.journalFile)
	if err != nil {
		fmt.Println("Error opening journal file:", err)
		return
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	for decoder.More() {
		var txn Transaction
		if err := decoder.Decode(&txn); err != nil {
			fmt.Println("Error decoding journal entry:", err)
			return
		}

		if txn.LSN > e.lsn {
			if e.shouldApplyTransaction(txn) {
				e.applyTransactionLocal(txn)
			}
		}
	}
	fmt.Println("Journal loaded. Last LSN:", e.lsn, "Vector Clock:", e.vclock)
}

func (e *Engine) saveJournal() {
	file, err := os.Create(e.journalFile)
	if err != nil {
		slog.Error("failed to open journal file for writing", "error", err)
		return
	}
	defer file.Close()

	enc := json.NewEncoder(file)
	for _, txn := range e.transactions {
		if err := enc.Encode(txn); err != nil {
			slog.Error("failed to encode transaction to journal file", "error", err)
			return
		}
	}
	fmt.Println("Journal saved. LSN:", e.lsn, "Vector Clock:", e.vclock)
}

func (e *Engine) applyTransaction(txn Transaction) {
	txn.NodeID = e.nodeID
	if e.isLeader {
		e.applyTransactionLocal(txn)
		e.replicateTransaction(txn)
	} else {
		fmt.Println("Replica received transaction but only leader should create new")
	}
}

func (e *Engine) applyTransactionLocal(txn Transaction) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.shouldApplyTransaction(txn) {
		fmt.Println("Transaction already applied, skipping LSN:", txn.LSN, "NodeID:", txn.NodeID, "Current VClock:", e.vclock, "Txn VClock:", txn.NodeID, txn.LSN)
		return
	}

	e.lsn++
	txn.LSN = e.lsn
	e.transactions = append(e.transactions, txn)

	e.applyIndex(txn)
	e.updateVectorClock(txn)
	e.journalTransaction(txn)
	fmt.Println("Transaction applied locally. LSN:", e.lsn, "Action:", txn.Action, "NodeID:", txn.NodeID, "Vector Clock:", e.vclock)

}

func (e *Engine) applyRemoteTransaction(msg ReplicationMessage) {
	txn := msg.Transaction

	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.shouldApplyTransaction(txn) {
		fmt.Println("Remote transaction already applied, skipping LSN:", txn.LSN, "NodeID:", txn.NodeID, "Current VClock:", e.vclock, "Txn VClock:", txn.NodeID, txn.LSN)
		return
	}

	e.lsn++
	txn.LSN = e.lsn
	e.transactions = append(e.transactions, txn)

	e.applyIndex(txn)
	e.updateVectorClockFromMessage(msg)
	e.journalTransaction(txn)
	fmt.Println("Remote transaction applied. LSN:", e.lsn, "Action:", txn.Action, "NodeID:", txn.NodeID, "Vector Clock:", e.vclock, "Received VClock:", msg.VectorClock)

}

func (e *Engine) shouldApplyTransaction(txn Transaction) bool {
	currentLSN, ok := e.vclock[txn.NodeID]
	if !ok {
		return true
	}
	return txn.LSN > currentLSN
}

func (e *Engine) updateVectorClock(txn Transaction) {
	e.vclock[e.nodeID] = txn.LSN
}

func (e *Engine) updateVectorClockFromMessage(msg ReplicationMessage) {
	for nodeID, lsn := range msg.VectorClock {
		if lsn > e.vclock[nodeID] {
			e.vclock[nodeID] = lsn
		}
	}
}

func (e *Engine) journalTransaction(txn Transaction) {
	file, err := os.OpenFile(e.journalFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		slog.Error("failed to open journal file for appending", "error", err)
		return
	}
	defer file.Close()

	enc := json.NewEncoder(file)
	if err := enc.Encode(txn); err != nil {
		slog.Error("failed to encode transaction to journal file", "error", err)
	}
}

func (e *Engine) connectToReplicas() {
	for _, replica := range e.replicas {
		conn, _, err := websocket.DefaultDialer.Dial(replica, nil)
		if err != nil {
			log.Printf("Failed to connect to replica %s: %v", replica, err)
			continue
		}
		e.connMu.Lock()
		e.replicaConns[replica] = conn
		e.connMu.Unlock()
		log.Printf("Connected to replica %s", replica)
	}
}

func (e *Engine) startReplicationServer() {
	http.HandleFunc("/replication", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Println("Failed to upgrade connection:", err)
			return
		}
		defer conn.Close()

		for {
			var msg Message
			err := conn.ReadJSON(&msg)
			if err != nil {
				log.Println("Error reading message:", err)
				break
			}
			e.processMessage(msg)
		}
	})

	log.Fatal(http.ListenAndServe(":8081", nil))
}

func (e *Engine) disconnectReplicas() {
	e.connMu.Lock()
	defer e.connMu.Unlock()

	for replica, conn := range e.replicaConns {
		if err := conn.Close(); err != nil {
			log.Printf("Error closing connection to replica %s: %v", replica, err)
		} else {
			log.Printf("Disconnected from replica %s", replica)
		}
	}
	e.replicaConns = make(map[string]*websocket.Conn) // Очищаем карту соединений
}

type Storage struct {
	mux      *http.ServeMux
	name     string
	data     map[string]*geojson.Feature
	mu       sync.RWMutex
	dbPath   string
	engine   *Engine
	replicas []string
	leader   bool
}

func NewStorage(mux *http.ServeMux, name string, checkpointFile, journalFile string, replicas []string, leader bool) *Storage {
	dbPath := "geo.db.json"
	engine := NewEngine(checkpointFile, journalFile)

	storage := &Storage{
		mux:      mux,
		name:     name,
		data:     make(map[string]*geojson.Feature),
		dbPath:   dbPath,
		engine:   engine,
		replicas: replicas,
		leader:   leader,
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

	// Если это лидер, синхронизируем данные с репликами
	if s.leader {
		s.syncWithReplicas(data)
	}
}

func (s *Storage) syncWithReplicas(data []byte) {
	var wg sync.WaitGroup

	for _, replica := range s.replicas {
		wg.Add(1)
		go func(replica string) {
			defer wg.Done()

			resp, err := http.Post(replica+"/sync", "application/json", bytes.NewBuffer(data))
			if err != nil {
				slog.Error("failed to sync with replica", "replica", replica, "error", err)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				slog.Error("failed to sync with replica", "replica", replica, "status", resp.Status)
			}
		}(replica)
	}

	wg.Wait()
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
	if !s.leader {
		http.Error(w, "Not a leader", http.StatusForbidden)
		return
	}

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

	s.saveToDB()
}

func (s *Storage) replaceHandler(w http.ResponseWriter, r *http.Request) {
	if !s.leader {
		http.Error(w, "Not a leader", http.StatusForbidden)
		return
	}

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

	s.saveToDB()
}

func (s *Storage) deleteHandler(w http.ResponseWriter, r *http.Request) {
	if !s.leader {
		http.Error(w, "Not a leader", http.StatusForbidden)
		return
	}

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

	s.saveToDB()
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

	// Создаем экземпляры Storage
	storage1 := NewStorage(mux, "storage1", "checkpoint1.json", "journal1.json", []string{"http://127.0.0.1:8081", "http://127.0.0.1:8082"}, true)
	storage2 := NewStorage(mux, "storage2", "checkpoint2.json", "journal2.json", []string{"http://127.0.0.1:8080", "http://127.0.0.1:8082"}, false)
	storage3 := NewStorage(mux, "storage3", "checkpoint3.json", "journal3.json", []string{"http://127.0.0.1:8080", "http://127.0.0.1:8081"}, false)

	// Запускаем горутины для каждого хранилища
	go func() { storage1.Run() }()
	go func() { storage2.Run() }()
	go func() { storage3.Run() }()

	// Создаем и запускаем роутер
	router := NewRouter(mux, [][]string{{"storage"}})
	go func() { router.Run() }()

	// Настраиваем HTTP сервер
	server := &http.Server{
		Addr:    "127.0.0.1:8080",
		Handler: mux,
	}

	// Обработка сигналов для корректного завершения работы
	go func() {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
		sig := <-sigs
		log.Println("Received signal:", sig)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			log.Println("Server Shutdown:", err)
		}
	}()

	// Завершение работы при остановке
	defer func() {
		router.Stop()
		storage1.Stop()
		storage2.Stop()
		storage3.Stop()
		log.Println("Shutting down")
	}()

	log.Println("Listening on http://" + server.Addr)
	if err := server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		log.Println("Error:", err)
	}
}

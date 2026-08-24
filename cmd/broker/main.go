package main

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
	"distributed-event-log/internal/broker"
)

type publishRequest struct {
	Key string `json:"key"`
	Value string `json:"value"`
}

type publishResponse struct {
	Offset uint64 `json:"offset"`
}

type readResponse struct {
	Offset uint64 `json:"offset"`
	Timestamp int64 `json:"timestamp"`
	Key string `json:"key"`
	Value string `json:"value"`
}

// main starts the three Phase 1 brokers and the HTTP API.
func main() {
	peers := []broker.PeerAddress{
		{ID:"broker-1",Address:"broker-1"},
		{ID:"broker-2",Address:"broker-2"},
		{ID:"broker-3",Address:"broker-3"},
	}

	broker2,e := broker.NewBroker("broker-2","data/broker-2",peers)
	if e!=nil {
		log.Fatalf("start broker-2: %v",e)
	}

	broker3,e := broker.NewBroker("broker-3","data/broker-3",peers)
	if e!=nil {
		_ = broker2.Close()
		log.Fatalf("start broker-3: %v", e)
	}

	broker1,e := broker.NewBroker("broker-1","data/broker-1",peers)
	if e!=nil {
		_ = broker3.Close()
		_ = broker2.Close()
		log.Fatalf("start broker-1: %v", e)
	}

	brokers := map[string]*broker.Broker{
		"broker-1": broker1,
		"broker-2": broker2,
		"broker-3": broker3,
	}

	if err := waitForLeader(broker1); err != nil {
		_ = broker1.Close()
		_ = broker2.Close()
		_ = broker3.Close()
		log.Fatal(err)
	}

	server := &http.Server{
		Addr:":8080",
	}

	http.HandleFunc("/publish",func(w http.ResponseWriter,r *http.Request) {
		handlePublish(w,r,brokers)
	})

	http.HandleFunc("/read",func(w http.ResponseWriter,r *http.Request) {
		handleRead(w,r,broker1)
	})

	go func() {
		if e := server.ListenAndServe(); e!=nil && e!=http.ErrServerClosed {
			log.Printf("HTTP server: %v",e)
		}
	}()

	log.Println("broker cluster started on :8080")

	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel,syscall.SIGINT,syscall.SIGTERM)
	<-signalChannel

	_ = server.Close()

	if e := broker1.Close(); e != nil {
		log.Printf("close broker-1: %v", e)
	}

	if e := broker2.Close(); e != nil {
		log.Printf("close broker-2: %v",e)
	}

	if e := broker3.Close(); e != nil {
		log.Printf("close broker-3: %v",e)
	}
}

// waitForLeader waits up to ten seconds for broker-1 to observe a Raft leader.
func waitForLeader(broker1 *broker.Broker) error {
	deadline := time.Now().Add(10*time.Second)

	for time.Now().Before(deadline) {
		if broker1.Leader() != "" {
			return nil
		}

		time.Sleep(100*time.Millisecond)
	}

	return errors.New("timed out waiting for Raft leader")
}

// handlePublish decodes a publish request and forwards it to the current leader.
func handlePublish(w http.ResponseWriter,r *http.Request,brokers map[string]*broker.Broker) {
	if r.Method != http.MethodPost {
		http.Error(w,"method not allowed",http.StatusMethodNotAllowed)
		return
	}

	var request publishRequest
	if e := json.NewDecoder(r.Body).Decode(&request); e!=nil {
		http.Error(w,e.Error(),http.StatusBadRequest)
		return
	}

	var leader *broker.Broker
	for _, candidate := range brokers {
		if candidate.IsLeader() {
			leader=candidate
			break
		}
	}

	if leader==nil {
		http.Error(w,"no leader available",http.StatusServiceUnavailable)
		return
	}

	offset, e := leader.Publish([]byte(request.Key),[]byte(request.Value))
	if e!=nil {
		http.Error(w,e.Error(),http.StatusServiceUnavailable)
		return
	}

	w.Header().Set("Content-Type","application/json")
	_ = json.NewEncoder(w).Encode(publishResponse{
		Offset: offset,
	})
}

// handleRead reads a message by offset from broker-1's commit log.
func handleRead(w http.ResponseWriter,r *http.Request,broker1 *broker.Broker) {
	if r.Method!=http.MethodGet {
		http.Error(w,"method not allowed",http.StatusMethodNotAllowed)
		return
	}

	offsetString := r.URL.Query().Get("offset")
	if offsetString=="" {
		http.Error(w, "missing offset",http.StatusBadRequest)
		return
	}

	offset,err:=strconv.ParseUint(offsetString,10,64)
	if err != nil {
		http.Error(w, "invalid offset", http.StatusBadRequest)
		return
	}

	message, err := broker1.Read(offset)
	if err!=nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(readResponse{
		Offset:    message.Offset,
		Timestamp: message.Timestamp,
		Key:       string(message.Key),
		Value:     string(message.Value),
	})
}
package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"distributed-event-log/internal/broker"
)

// main creates one broker process from environment configuration and waits for termination.
func main() {
	required := []string{
		"BROKER_ID",
		"BROKER_HTTP_ADDR",
		"BROKER_RAFT_ADDR",
		"BROKER_DATA_DIR",
	}

	for _, name := range required {
		if os.Getenv(name) == "" {
			log.Fatalf("required environment variable %s is missing", name)
		}
	}

	id := os.Getenv("BROKER_ID")
	httpAddr := os.Getenv("BROKER_HTTP_ADDR")
	raftAddr := os.Getenv("BROKER_RAFT_ADDR")
	dataDir := os.Getenv("BROKER_DATA_DIR")
	peers := os.Getenv("BROKER_PEERS")

	b, err := broker.NewBroker(id, httpAddr, raftAddr, dataDir, peers)
	if err != nil {
		log.Fatalf("create broker %s: %v", id, err)
	}

	log.Printf("broker %s started", id)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals

	if err := b.Close(); err != nil {
		log.Printf("close broker %s: %v", id, err)
	}
}
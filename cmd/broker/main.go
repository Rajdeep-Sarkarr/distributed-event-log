package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"distributed-event-log/internal/broker"
)

// main starts one broker process and waits for termination signals.
func main() {
	id := os.Getenv("BROKER_ID")
	httpAddr := os.Getenv("BROKER_HTTP_ADDR")
	raftAddr := os.Getenv("BROKER_RAFT_ADDR")
	dataDir := os.Getenv("BROKER_DATA_DIR")
	peers := os.Getenv("BROKER_PEERS")

	certFile := os.Getenv("BROKER_CERT_FILE")
	keyFile := os.Getenv("BROKER_KEY_FILE")
	caFile := os.Getenv("BROKER_CA_FILE")

	required := map[string]string{
		"BROKER_ID":        id,
		"BROKER_HTTP_ADDR": httpAddr,
		"BROKER_RAFT_ADDR": raftAddr,
		"BROKER_DATA_DIR":  dataDir,
	}

	for name, value := range required {
		if value == "" {
			log.Fatalf("%s is required", name)
		}
	}

	b, err := broker.NewBroker(
		id,
		httpAddr,
		raftAddr,
		dataDir,
		peers,
		certFile,
		keyFile,
		caFile,
	)
	if err != nil {
		log.Fatalf(
			"failed to start broker: %v",
			err,
		)
	}

	log.Printf(
		"broker %s started",
		id,
	)

	signalCh := make(chan os.Signal, 1)

	signal.Notify(
		signalCh,
		syscall.SIGINT,
		syscall.SIGTERM,
	)

	<-signalCh

	if err := b.Close(); err != nil {
		fmt.Fprintf(
			os.Stderr,
			"broker %s shutdown error: %v\n",
			id,
			err,
		)
	}
}
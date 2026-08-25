package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	hraft "github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"

	"distributed-event-log/internal/log"
	raftfsm "distributed-event-log/internal/raft"
	pb "distributed-event-log/internal/proto"
)

var (
	messagesProducedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "event_log_messages_produced_total",
		Help: "Total number of successfully produced messages.",
	})
	messagesConsumedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "event_log_messages_consumed_total",
		Help: "Total number of successfully consumed messages.",
	})
	produceErrorsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "event_log_produce_errors_total",
		Help: "Total number of failed produce requests.",
	})
	consumeErrorsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "event_log_consume_errors_total",
		Help: "Total number of failed consume requests.",
	})
)

// mustRegisterOnce registers a Prometheus collector unless it is already registered.
func mustRegisterOnce(c prometheus.Collector) {
	if err := prometheus.Register(c); err != nil {
		if _, ok := err.(prometheus.AlreadyRegisteredError); !ok {
			panic(err)
		}
	}
}

func init() {
	mustRegisterOnce(messagesProducedTotal)
	mustRegisterOnce(messagesConsumedTotal)
	mustRegisterOnce(produceErrorsTotal)
	mustRegisterOnce(consumeErrorsTotal)
}

// Broker owns a commit log, a Raft node, an HTTP server, and a gRPC server.
type Broker struct {
	pb.UnimplementedBrokerServiceServer
	log        *log.Log
	raft       *hraft.Raft
	httpServer *http.Server
	grpcServer *grpc.Server
}

// NewBroker creates a broker with a TCP Raft transport, persistent Raft stores,
// a file snapshot store, an HTTP API, and a gRPC API.
func NewBroker(id, httpAddr, raftAddr, dataDir, peers string) (*Broker, error) {
	commitLog, err := log.NewLog(dataDir)
	if err != nil {
		return nil, err
	}

	raftDir := filepath.Join(dataDir, "raft")
	if err := os.MkdirAll(raftDir, 0755); err != nil {
		_ = commitLog.Close()
		return nil, err
	}

	fsm := raftfsm.NewFSM(commitLog)

	transport, err := hraft.NewTCPTransport(
		raftAddr,
		nil,
		3,
		10*time.Second,
		os.Stderr,
	)
	if err != nil {
		_ = commitLog.Close()
		return nil, err
	}

	logStore, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, "raft-log.db"))
	if err != nil {
		_ = transport.Close()
		_ = commitLog.Close()
		return nil, err
	}

	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, "raft-stable.db"))
	if err != nil {
		_ = logStore.Close()
		_ = transport.Close()
		_ = commitLog.Close()
		return nil, err
	}

	snapshotStore, err := hraft.NewFileSnapshotStore(
		dataDir,
		2,
		os.Stderr,
	)
	if err != nil {
		_ = stableStore.Close()
		_ = logStore.Close()
		_ = transport.Close()
		_ = commitLog.Close()
		return nil, err
	}

	config := hraft.DefaultConfig()
	config.LocalID = hraft.ServerID(id)

	raftNode, err := hraft.NewRaft(
		config,
		fsm,
		logStore,
		stableStore,
		snapshotStore,
		transport,
	)
	if err != nil {
		_ = stableStore.Close()
		_ = logStore.Close()
		_ = transport.Close()
		_ = commitLog.Close()
		return nil, err
	}

	b := &Broker{
		log:  commitLog,
		raft: raftNode,
	}

	if peers == "" {
		configuration := hraft.Configuration{
			Servers: []hraft.Server{
				{
					ID:      hraft.ServerID(id),
					Address: hraft.ServerAddress(raftAddr),
				},
			},
		}

		if err := raftNode.BootstrapCluster(configuration).Error(); err != nil {
			_ = raftNode.Shutdown().Error()
			_ = stableStore.Close()
			_ = logStore.Close()
			_ = transport.Close()
			_ = commitLog.Close()
			return nil, err
		}
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/join", b.handleJoin)
	mux.HandleFunc("/produce", b.handleProduce)
	mux.HandleFunc("/consume", b.handleConsume)
	mux.Handle("/metrics", promhttp.Handler())

	b.httpServer = &http.Server{
		Addr:    httpAddr,
		Handler: mux,
	}

	go func() {
		if err := b.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Fprintf(os.Stderr, "broker %s HTTP server: %v\n", id, err)
		}
	}()

	grpcAddr, err := grpcAddress(httpAddr)
	if err != nil {
		_ = b.Close()
		return nil, err
	}

	grpcListener, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		_ = b.Close()
		return nil, err
	}

	b.grpcServer = grpc.NewServer()
	pb.RegisterBrokerServiceServer(b.grpcServer, b)

	go func() {
		if err := b.grpcServer.Serve(grpcListener); err != nil {
			fmt.Fprintf(os.Stderr, "broker %s gRPC server: %v\n", id, err)
		}
	}()

	if peers != "" {
		if err := b.joinPeer(id, raftAddr, peers); err != nil {
			_ = b.Close()
			return nil, err
		}
	}

	return b, nil
}

// IsLeader reports whether this broker is currently the Raft leader.
func (b *Broker) IsLeader() bool {
	return b.raft.State() == hraft.Leader
}

// Leader returns the current Raft leader's address, or empty string if unknown.
func (b *Broker) Leader() string {
	address, _ := b.raft.LeaderWithID()
	return string(address)
}

// Publish publishes a key-value command through Raft and returns its commit log offset.
func (b *Broker) Publish(key, value []byte) (uint64, error) {
	if b.raft.State() != hraft.Leader {
		return 0, errors.New("broker is not the Raft leader")
	}

	command := raftfsm.Command{
		Key:   key,
		Value: value,
	}

	data, err := json.Marshal(command)
	if err != nil {
		return 0, err
	}

	future := b.raft.Apply(data, 5*time.Second)
	if err := future.Error(); err != nil {
		return 0, err
	}

	response := future.Response()

	switch v := response.(type) {
	case uint64:
		return v, nil
	case error:
		return 0, v
	default:
		return 0, fmt.Errorf("unexpected FSM response type %T", response)
	}
}

// Join adds a broker as a voter to the Raft cluster.
func (b *Broker) Join(id, address string) error {
	return b.raft.AddVoter(
		hraft.ServerID(id),
		hraft.ServerAddress(address),
		0,
		5*time.Second,
	).Error()
}

// Read reads a message directly from the broker's commit log.
func (b *Broker) Read(offset uint64) (*log.Message, error) {
	return b.log.Read(offset)
}

// Produce implements the gRPC BrokerService Produce RPC.
func (b *Broker) Produce(ctx context.Context, req *pb.ProduceRequest) (*pb.ProduceResponse, error) {
	offset, err := b.Publish(req.Key, req.Value)
	if err != nil {
		produceErrorsTotal.Inc()
		return nil, err
	}

	messagesProducedTotal.Inc()

	return &pb.ProduceResponse{
		Offset: offset,
	}, nil
}

// Consume implements the gRPC BrokerService Consume RPC.
func (b *Broker) Consume(ctx context.Context, req *pb.ConsumeRequest) (*pb.ConsumeResponse, error) {
	message, err := b.Read(req.Offset)
	if err != nil {
		consumeErrorsTotal.Inc()
		return nil, err
	}

	messagesConsumedTotal.Inc()

	return &pb.ConsumeResponse{
		Offset: message.Offset,
		Key:    message.Key,
		Value:  message.Value,
	}, nil
}

// Close gracefully shuts down the gRPC server, HTTP server, Raft node, and commit log.
func (b *Broker) Close() error {
	var firstErr error

	if b.grpcServer != nil {
		b.grpcServer.GracefulStop()
	}

	if b.httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := b.httpServer.Shutdown(ctx)
		cancel()

		if err != nil && firstErr == nil {
			firstErr = err
		}
	}

	if err := b.raft.Shutdown().Error(); err != nil && firstErr == nil {
		firstErr = err
	}

	if err := b.log.Close(); err != nil && firstErr == nil {
		firstErr = err
	}

	return firstErr
}

// handleJoin handles requests to add a broker as a Raft voter.
func (b *Broker) handleJoin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var request struct {
		ID   string `json:"id"`
		Addr string `json:"addr"`
	}

	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if request.ID == "" || request.Addr == "" {
		http.Error(w, "id and addr are required", http.StatusBadRequest)
		return
	}

	if err := b.Join(request.ID, request.Addr); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// handleProduce handles HTTP publish requests through the local Raft node.
func (b *Broker) handleProduce(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var request struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}

	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		produceErrorsTotal.Inc()
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	offset, err := b.Publish([]byte(request.Key), []byte(request.Value))
	if err != nil {
		produceErrorsTotal.Inc()
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	messagesProducedTotal.Inc()

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		Offset uint64 `json:"offset"`
	}{
		Offset: offset,
	})
}

// handleConsume handles HTTP requests to read a message from the local commit log.
func (b *Broker) handleConsume(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		consumeErrorsTotal.Inc()
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	offsetString := r.URL.Query().Get("offset")
	if offsetString == "" {
		consumeErrorsTotal.Inc()
		http.Error(w, "missing offset", http.StatusBadRequest)
		return
	}

	offset, err := strconv.ParseUint(offsetString, 10, 64)
	if err != nil {
		consumeErrorsTotal.Inc()
		http.Error(w, "invalid offset", http.StatusBadRequest)
		return
	}

	message, err := b.Read(offset)
	if err != nil {
		consumeErrorsTotal.Inc()
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	messagesConsumedTotal.Inc()

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		Offset uint64 `json:"offset"`
		Key    string `json:"key"`
		Value  string `json:"value"`
	}{
		Offset: message.Offset,
		Key:    string(message.Key),
		Value:  string(message.Value),
	})
}

// joinPeer registers this broker with the configured peer, retrying up to five times.
func (b *Broker) joinPeer(id, addr, peer string) error {
	body := fmt.Sprintf(`{"id":%q,"addr":%q}`, id, addr)
	url := "http://" + peer + "/join"

	var lastErr error

	for attempt := 0; attempt < 5; attempt++ {
		response, err := http.Post(
			url,
			"application/json",
			strings.NewReader(body),
		)
		if err == nil {
			if response.StatusCode >= 200 && response.StatusCode < 300 {
				_ = response.Body.Close()
				return nil
			}

			response.Body.Close()
			lastErr = fmt.Errorf("join request returned status %s", response.Status)
		} else {
			lastErr = err
		}

		if attempt < 4 {
			time.Sleep(2 * time.Second)
		}
	}

	return fmt.Errorf("failed to join peer %s after 5 attempts: %w", peer, lastErr)
}

// grpcAddress derives the gRPC address by adding 1000 to the HTTP port.
func grpcAddress(httpAddr string) (string, error) {
	host, portString, err := net.SplitHostPort(httpAddr)
	if err != nil {
		return "", fmt.Errorf("invalid HTTP address %q: %w", httpAddr, err)
	}

	port, err := strconv.Atoi(portString)
	if err != nil {
		return "", fmt.Errorf("invalid HTTP port %q: %w", portString, err)
	}

	if port > 65535-1000 {
		return "", fmt.Errorf("HTTP port %d is too high for gRPC port offset", port)
	}

	return net.JoinHostPort(host, strconv.Itoa(port+1000)), nil
}
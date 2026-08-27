package broker

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	hraft "github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"distributed-event-log/internal/log"
	pb "distributed-event-log/internal/proto"
	raftfsm "distributed-event-log/internal/raft"
	deltatls "distributed-event-log/internal/tls"
)

const defaultNumPartitions = 3

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

// Broker owns topic partitions, consumer-group offsets, a Raft node,
// an HTTP server, and a gRPC server.
type Broker struct {
	pb.UnimplementedBrokerServiceServer

	logs map[string][]*log.Log
	mu   sync.RWMutex

	roundRobin map[string]uint64
	dataDir    string

	consumerGroups *ConsumerGroupStore

	certFile string
	keyFile  string
	caFile   string

	raft       *hraft.Raft
	httpServer *http.Server
	grpcServer *grpc.Server
}

// tlsStreamLayer wraps a network listener and applies TLS to Raft connections.
type tlsStreamLayer struct {
	net.Listener
	ServerTLSConfig *tls.Config
	ClientTLSConfig *tls.Config
}

// Accept accepts a Raft connection and wraps it with server-side TLS.
func (s *tlsStreamLayer) Accept() (net.Conn, error) {
	conn, err := s.Listener.Accept()
	if err != nil {
		return nil, err
	}

	if s.ServerTLSConfig == nil {
		return conn, nil
	}

	return tls.Server(conn, s.ServerTLSConfig), nil
}

// Dial connects to a Raft peer and wraps the connection with client-side TLS.
func (s *tlsStreamLayer) Dial(
	address hraft.ServerAddress,
	timeout time.Duration,
) (net.Conn, error) {
	dialer := &net.Dialer{
		Timeout: timeout,
	}

	if s.ClientTLSConfig == nil {
		return dialer.Dial("tcp", string(address))
	}

	config := s.ClientTLSConfig.Clone()

	host, _, err := net.SplitHostPort(string(address))
	if err != nil {
		config.ServerName = string(address)
	} else {
		config.ServerName = host
	}

	return tls.DialWithDialer(
		dialer,
		"tcp",
		string(address),
		config,
	)
}

// NewBroker creates a broker with TCP or TLS-protected Raft transport,
// persistent stores, topic partitions, consumer-group storage, HTTP,
// and gRPC servers.
func NewBroker(
	id string,
	httpAddr string,
	raftAddr string,
	dataDir string,
	peers string,
	certFile string,
	keyFile string,
	caFile string,
) (*Broker, error) {
	b := &Broker{
		logs:       make(map[string][]*log.Log),
		roundRobin: make(map[string]uint64),
		dataDir:    dataDir,
		certFile:   certFile,
		keyFile:    keyFile,
		caFile:     caFile,
	}

	consumerGroups, err := NewConsumerGroupStore(
		filepath.Join(dataDir, "consumer-groups.db"),
	)
	if err != nil {
		return nil, err
	}

	b.consumerGroups = consumerGroups

	raftDir := filepath.Join(dataDir, "raft")

	if err := os.MkdirAll(raftDir, 0755); err != nil {
		_ = consumerGroups.Close()
		return nil, err
	}

	var transport *hraft.NetworkTransport
	var errTransport error

	tlsEnabled := certFile != "" &&
		keyFile != "" &&
		caFile != ""

	if tlsEnabled {
		serverTLSConfig, err := deltatls.LoadServerTLSConfig(
			certFile,
			keyFile,
			caFile,
		)
		if err != nil {
			_ = consumerGroups.Close()
			return nil, err
		}

		clientTLSConfig, err := deltatls.LoadClientTLSConfig(
			certFile,
			keyFile,
			caFile,
		)
		if err != nil {
			_ = consumerGroups.Close()
			return nil, err
		}

		listener, err := net.Listen("tcp", raftAddr)
		if err != nil {
			_ = consumerGroups.Close()
			return nil, err
		}

		streamLayer := &tlsStreamLayer{
			Listener:         listener,
			ServerTLSConfig:  serverTLSConfig,
			ClientTLSConfig:  clientTLSConfig,
		}

		transport = hraft.NewNetworkTransport(
			streamLayer,
			3,
			10*time.Second,
			os.Stderr,
		)
	} else {
		transport, errTransport = hraft.NewTCPTransport(
			raftAddr,
			nil,
			3,
			10*time.Second,
			os.Stderr,
		)

		if errTransport != nil {
			_ = consumerGroups.Close()
			return nil, errTransport
		}
	}

	logStore, err := raftboltdb.NewBoltStore(
		filepath.Join(raftDir, "raft-log.db"),
	)
	if err != nil {
		_ = transport.Close()
		_ = consumerGroups.Close()
		return nil, err
	}

	stableStore, err := raftboltdb.NewBoltStore(
		filepath.Join(raftDir, "raft-stable.db"),
	)
	if err != nil {
		_ = logStore.Close()
		_ = transport.Close()
		_ = consumerGroups.Close()
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
		_ = consumerGroups.Close()
		return nil, err
	}

	fsm := raftfsm.NewFSM(b)

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
		_ = consumerGroups.Close()
		return nil, err
	}

	b.raft = raftNode

	if peers == "" {
    hasState, err := hraft.HasExistingState(logStore, stableStore, snapshotStore)
    if err != nil {
        _ = raftNode.Shutdown().Error()
        _ = stableStore.Close()
        _ = logStore.Close()
        _ = transport.Close()
        _ = consumerGroups.Close()
        return nil, fmt.Errorf("failed to check raft state: %w", err)
    }

    if !hasState {
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
            _ = consumerGroups.Close()
            return nil, err
        }
    }
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/join", b.handleJoin)
	mux.HandleFunc("/produce", b.handleProduce)
	mux.HandleFunc("/consume", b.handleConsume)
	mux.HandleFunc("/commit-offset", b.handleCommitOffset)
	mux.HandleFunc("/fetch-offset", b.handleFetchOffset)
	mux.Handle("/metrics", promhttp.Handler())

	b.httpServer = &http.Server{
		Addr:    httpAddr,
		Handler: mux,
	}

	go func() {
		if err := b.httpServer.ListenAndServe(); err != nil &&
			err != http.ErrServerClosed {
			fmt.Fprintf(
				os.Stderr,
				"broker %s HTTP server: %v\n",
				id,
				err,
			)
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

	var grpcOptions []grpc.ServerOption

	if tlsEnabled {
		serverTLSConfig, err := deltatls.LoadServerTLSConfig(
			certFile,
			keyFile,
			caFile,
		)
		if err != nil {
			_ = grpcListener.Close()
			_ = b.Close()
			return nil, err
		}

		grpcOptions = append(
			grpcOptions,
			grpc.Creds(
				credentials.NewTLS(serverTLSConfig),
			),
		)
	}

	b.grpcServer = grpc.NewServer(grpcOptions...)

	pb.RegisterBrokerServiceServer(
		b.grpcServer,
		b,
	)

	go func() {
		if err := b.grpcServer.Serve(grpcListener); err != nil {
			fmt.Fprintf(
				os.Stderr,
				"broker %s gRPC server: %v\n",
				id,
				err,
			)
		}
	}()

	if peers != "" {
		if err := b.joinPeer(
			id,
			raftAddr,
			peers,
		); err != nil {
			_ = b.Close()
			return nil, err
		}
	}

	return b, nil
}

// GetPartition returns the commit log for the requested topic partition,
// creating the topic and its partitions when necessary.
func (b *Broker) GetPartition(
	topic string,
	partition int,
) (*log.Log, error) {
	if topic == "" {
		topic = "default"
	}

	if partition < 0 ||
		partition >= defaultNumPartitions {
		return nil, fmt.Errorf(
			"partition %d out of range",
			partition,
		)
	}

	b.mu.RLock()

	partitions, ok := b.logs[topic]

	if ok && len(partitions) > partition {
		partitionLog := partitions[partition]
		b.mu.RUnlock()
		return partitionLog, nil
	}

	b.mu.RUnlock()

	b.mu.Lock()
	defer b.mu.Unlock()

	if partitions, ok := b.logs[topic]; ok &&
		len(partitions) > partition {
		return partitions[partition], nil
	}

	partitions = make([]*log.Log, defaultNumPartitions)

	for i := 0; i < defaultNumPartitions; i++ {
		partitionDir := filepath.Join(
			b.dataDir,
			"topics",
			topic,
			"partitions",
			strconv.Itoa(i),
		)

		partitionLog, err := log.NewLog(partitionDir)
		if err != nil {
			for _, existing := range partitions {
				if existing != nil {
					_ = existing.Close()
				}
			}

			return nil, err
		}

		partitions[i] = partitionLog
	}

	b.logs[topic] = partitions

	return partitions[partition], nil
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

// Publish selects a partition, replicates the command through Raft, and returns
// the offset assigned by the selected partition.
func (b *Broker) Publish(
	topic string,
	key []byte,
	value []byte,
	partition *int32,
) (uint64, error) {
	if topic == "" {
		topic = "default"
	}

	if b.raft.State() != hraft.Leader {
		return 0, errors.New(
			"broker is not the Raft leader",
		)
	}

	selectedPartition, err := b.selectPartition(
		topic,
		key,
		partition,
	)
	if err != nil {
		return 0, err
	}

	_, err = b.GetPartition(
		topic,
		selectedPartition,
	)
	if err != nil {
		return 0, err
	}

	command := raftfsm.Command{
		Topic:     topic,
		Partition: selectedPartition,
		Key:       key,
		Value:     value,
	}

	data, err := json.Marshal(command)
	if err != nil {
		return 0, err
	}

	future := b.raft.Apply(
		data,
		5*time.Second,
	)

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
		return 0, fmt.Errorf(
			"unexpected FSM response type %T",
			response,
		)
	}
}

// Join adds a broker as a voter to the Raft cluster.
func (b *Broker) Join(
	id string,
	address string,
) error {
	return b.raft.AddVoter(
		hraft.ServerID(id),
		hraft.ServerAddress(address),
		0,
		5*time.Second,
	).Error()
}

// Read reads a message from a specific topic partition.
func (b *Broker) Read(
	topic string,
	partition int,
	offset uint64,
) (*log.Message, error) {
	partitionLog, err := b.GetPartition(
		topic,
		partition,
	)
	if err != nil {
		return nil, err
	}

	return partitionLog.Read(offset)
}

// Produce implements the gRPC BrokerService Produce RPC.
func (b *Broker) Produce(
	ctx context.Context,
	req *pb.ProduceRequest,
) (*pb.ProduceResponse, error) {
	offset, err := b.Publish(
		req.Topic,
		req.Key,
		req.Value,
		req.Partition,
	)
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
func (b *Broker) Consume(
	ctx context.Context,
	req *pb.ConsumeRequest,
) (*pb.ConsumeResponse, error) {
	partition := int32(0)

	if req.Partition != nil {
		partition = *req.Partition
	}

	message, err := b.Read(
		req.Topic,
		int(partition),
		req.Offset,
	)
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

// CommitOffset implements the gRPC BrokerService CommitOffset RPC.
func (b *Broker) CommitOffset(
	ctx context.Context,
	req *pb.CommitOffsetRequest,
) (*pb.CommitOffsetResponse, error) {
	if req.GroupId == "" {
		return nil, errors.New(
			"group_id is required",
		)
	}

	if req.Topic == "" {
		req.Topic = "default"
	}

	if req.Partition < 0 ||
		req.Partition >= defaultNumPartitions {
		return nil, fmt.Errorf(
			"partition %d out of range",
			req.Partition,
		)
	}

	if err := b.consumerGroups.CommitOffset(
		req.GroupId,
		req.Topic,
		req.Partition,
		req.Offset,
	); err != nil {
		return nil, err
	}

	return &pb.CommitOffsetResponse{}, nil
}

// FetchOffset implements the gRPC BrokerService FetchOffset RPC.
func (b *Broker) FetchOffset(
	ctx context.Context,
	req *pb.FetchOffsetRequest,
) (*pb.FetchOffsetResponse, error) {
	if req.GroupId == "" {
		return nil, errors.New(
			"group_id is required",
		)
	}

	if req.Topic == "" {
		req.Topic = "default"
	}

	if req.Partition < 0 ||
		req.Partition >= defaultNumPartitions {
		return nil, fmt.Errorf(
			"partition %d out of range",
			req.Partition,
		)
	}

	offset := b.consumerGroups.FetchOffset(
		req.GroupId,
		req.Topic,
		req.Partition,
	)

	return &pb.FetchOffsetResponse{
		Offset: offset,
	}, nil
}

// Close gracefully shuts down the gRPC server, HTTP server, Raft node,
// all topic partition logs, and consumer-group storage.
func (b *Broker) Close() error {
	var firstErr error

	if b.grpcServer != nil {
		b.grpcServer.GracefulStop()
	}

	if b.httpServer != nil {
		ctx, cancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)

		err := b.httpServer.Shutdown(ctx)
		cancel()

		if err != nil && firstErr == nil {
			firstErr = err
		}
	}

	if b.raft != nil {
		if err := b.raft.Shutdown().Error(); err != nil &&
			firstErr == nil {
			firstErr = err
		}
	}

	b.mu.Lock()

	for _, partitions := range b.logs {
		for _, partitionLog := range partitions {
			if partitionLog != nil {
				if err := partitionLog.Close(); err != nil &&
					firstErr == nil {
					firstErr = err
				}
			}
		}
	}

	b.mu.Unlock()

	if b.consumerGroups != nil {
		if err := b.consumerGroups.Close(); err != nil &&
			firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

// handleJoin handles requests to add a broker as a Raft voter.
func (b *Broker) handleJoin(
	w http.ResponseWriter,
	r *http.Request,
) {
	if r.Method != http.MethodPost {
		http.Error(
			w,
			"method not allowed",
			http.StatusMethodNotAllowed,
		)
		return
	}

	var request struct {
		ID   string `json:"id"`
		Addr string `json:"addr"`
	}

	if err := json.NewDecoder(
		r.Body,
	).Decode(&request); err != nil {
		http.Error(
			w,
			err.Error(),
			http.StatusBadRequest,
		)
		return
	}

	if request.ID == "" ||
		request.Addr == "" {
		http.Error(
			w,
			"id and addr are required",
			http.StatusBadRequest,
		)
		return
	}

	if err := b.Join(
		request.ID,
		request.Addr,
	); err != nil {
		http.Error(
			w,
			err.Error(),
			http.StatusInternalServerError,
		)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// handleProduce handles HTTP publish requests for a topic partition.
func (b *Broker) handleProduce(
	w http.ResponseWriter,
	r *http.Request,
) {
	if r.Method != http.MethodPost {
		produceErrorsTotal.Inc()

		http.Error(
			w,
			"method not allowed",
			http.StatusMethodNotAllowed,
		)
		return
	}

	var request struct {
		Topic     string `json:"topic"`
		Partition *int32 `json:"partition"`
		Key       string `json:"key"`
		Value     string `json:"value"`
	}

	if err := json.NewDecoder(
		r.Body,
	).Decode(&request); err != nil {
		produceErrorsTotal.Inc()

		http.Error(
			w,
			err.Error(),
			http.StatusBadRequest,
		)
		return
	}

	if request.Topic == "" {
		request.Topic = "default"
	}

	offset, err := b.Publish(
		request.Topic,
		[]byte(request.Key),
		[]byte(request.Value),
		request.Partition,
	)
	if err != nil {
		produceErrorsTotal.Inc()

		http.Error(
			w,
			err.Error(),
			http.StatusInternalServerError,
		)
		return
	}

	messagesProducedTotal.Inc()

	w.Header().Set(
		"Content-Type",
		"application/json",
	)

	_ = json.NewEncoder(w).Encode(
		struct {
			Offset    uint64 `json:"offset"`
			Topic     string `json:"topic"`
			Partition int    `json:"partition"`
		}{
			Offset:    offset,
			Topic:     request.Topic,
			Partition: partitionValue(request.Partition),
		},
	)
}

// handleConsume handles HTTP requests to read a message from a topic partition.
func (b *Broker) handleConsume(
	w http.ResponseWriter,
	r *http.Request,
) {
	if r.Method != http.MethodGet {
		consumeErrorsTotal.Inc()

		http.Error(
			w,
			"method not allowed",
			http.StatusMethodNotAllowed,
		)
		return
	}

	topic := r.URL.Query().Get("topic")

	if topic == "" {
		topic = "default"
	}

	partition := 0

	if value := r.URL.Query().Get(
		"partition",
	); value != "" {
		parsed, err := strconv.Atoi(value)

		if err != nil {
			consumeErrorsTotal.Inc()

			http.Error(
				w,
				"invalid partition",
				http.StatusBadRequest,
			)
			return
		}

		partition = parsed
	}

	offsetString := r.URL.Query().Get("offset")

	if offsetString == "" {
		consumeErrorsTotal.Inc()

		http.Error(
			w,
			"missing offset",
			http.StatusBadRequest,
		)
		return
	}

	offset, err := strconv.ParseUint(
		offsetString,
		10,
		64,
	)
	if err != nil {
		consumeErrorsTotal.Inc()

		http.Error(
			w,
			"invalid offset",
			http.StatusBadRequest,
		)
		return
	}

	message, err := b.Read(
		topic,
		partition,
		offset,
	)
	if err != nil {
		consumeErrorsTotal.Inc()

		http.Error(
			w,
			err.Error(),
			http.StatusNotFound,
		)
		return
	}

	messagesConsumedTotal.Inc()

	w.Header().Set(
		"Content-Type",
		"application/json",
	)

	_ = json.NewEncoder(w).Encode(
		struct {
			Offset    uint64 `json:"offset"`
			Topic     string `json:"topic"`
			Partition int    `json:"partition"`
			Key       string `json:"key"`
			Value     string `json:"value"`
		}{
			Offset:    message.Offset,
			Topic:     topic,
			Partition: partition,
			Key:       string(message.Key),
			Value:     string(message.Value),
		},
	)
}

// handleCommitOffset handles HTTP consumer-group offset commits.
func (b *Broker) handleCommitOffset(
	w http.ResponseWriter,
	r *http.Request,
) {
	if r.Method != http.MethodPost {
		http.Error(
			w,
			"method not allowed",
			http.StatusMethodNotAllowed,
		)
		return
	}

	var request struct {
		GroupID   string `json:"group_id"`
		Topic     string `json:"topic"`
		Partition int32  `json:"partition"`
		Offset    uint64 `json:"offset"`
	}

	if err := json.NewDecoder(
		r.Body,
	).Decode(&request); err != nil {
		http.Error(
			w,
			err.Error(),
			http.StatusBadRequest,
		)
		return
	}

	if request.GroupID == "" {
		http.Error(
			w,
			"group_id is required",
			http.StatusBadRequest,
		)
		return
	}

	if request.Topic == "" {
		request.Topic = "default"
	}

	if request.Partition < 0 ||
		request.Partition >= defaultNumPartitions {
		http.Error(
			w,
			"invalid partition",
			http.StatusBadRequest,
		)
		return
	}

	if err := b.consumerGroups.CommitOffset(
		request.GroupID,
		request.Topic,
		request.Partition,
		request.Offset,
	); err != nil {
		http.Error(
			w,
			err.Error(),
			http.StatusInternalServerError,
		)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// handleFetchOffset handles HTTP requests for a committed group offset.
func (b *Broker) handleFetchOffset(
	w http.ResponseWriter,
	r *http.Request,
) {
	if r.Method != http.MethodGet {
		http.Error(
			w,
			"method not allowed",
			http.StatusMethodNotAllowed,
		)
		return
	}

	groupID := r.URL.Query().Get("group_id")

	if groupID == "" {
		http.Error(
			w,
			"missing group_id",
			http.StatusBadRequest,
		)
		return
	}

	topic := r.URL.Query().Get("topic")

	if topic == "" {
		topic = "default"
	}

	partitionString := r.URL.Query().Get("partition")

	if partitionString == "" {
		http.Error(
			w,
			"missing partition",
			http.StatusBadRequest,
		)
		return
	}

	partition, err := strconv.ParseInt(
		partitionString,
		10,
		32,
	)

	if err != nil ||
		partition < 0 ||
		partition >= defaultNumPartitions {
		http.Error(
			w,
			"invalid partition",
			http.StatusBadRequest,
		)
		return
	}

	offset := b.consumerGroups.FetchOffset(
		groupID,
		topic,
		int32(partition),
	)

	w.Header().Set(
		"Content-Type",
		"application/json",
	)

	_ = json.NewEncoder(w).Encode(
		struct {
			Offset uint64 `json:"offset"`
		}{
			Offset: offset,
		},
	)
}

// joinPeer registers this broker with the configured peer.
func (b *Broker) joinPeer(
	id string,
	addr string,
	peer string,
) error {
	body := fmt.Sprintf(
		`{"id":%q,"addr":%q}`,
		id,
		addr,
	)

	url := "http://" + peer + "/join"

	var lastErr error

	for attempt := 0; attempt < 5; attempt++ {
		response, err := http.Post(
			url,
			"application/json",
			strings.NewReader(body),
		)

		if err == nil {
			if response.StatusCode >= 200 &&
				response.StatusCode < 300 {
				_ = response.Body.Close()
				return nil
			}

			_ = response.Body.Close()

			lastErr = fmt.Errorf(
				"join request returned status %s",
				response.Status,
			)
		} else {
			lastErr = err
		}

		if attempt < 4 {
			time.Sleep(2 * time.Second)
		}
	}

	return fmt.Errorf(
		"failed to join peer %s after 5 attempts: %w",
		peer,
		lastErr,
	)
}

// grpcAddress derives the gRPC address by adding 1000 to the HTTP port.
func grpcAddress(httpAddr string) (string, error) {
	host, portString, err := net.SplitHostPort(httpAddr)

	if err != nil {
		return "", fmt.Errorf(
			"invalid HTTP address %q: %w",
			httpAddr,
			err,
		)
	}

	port, err := strconv.Atoi(portString)

	if err != nil {
		return "", fmt.Errorf(
			"invalid HTTP port %q: %w",
			portString,
			err,
		)
	}

	if port > 65535-1000 {
		return "", fmt.Errorf(
			"HTTP port %d is too high for gRPC port offset",
			port,
		)
	}

	return net.JoinHostPort(
		host,
		strconv.Itoa(port+1000),
	), nil
}

// selectPartition selects an explicit partition, a key-hashed partition,
// or the next round-robin partition.
func (b *Broker) selectPartition(
	topic string,
	key []byte,
	partition *int32,
) (int, error) {
	if partition != nil {
		if *partition < 0 ||
			*partition >= defaultNumPartitions {
			return 0, fmt.Errorf(
				"partition %d out of range",
				*partition,
			)
		}

		return int(*partition), nil
	}

	if len(key) > 0 {
		hash := fnv.New32a()
		_, _ = hash.Write(key)

		return int(
			hash.Sum32()%defaultNumPartitions,
		), nil
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	next := b.roundRobin[topic] % defaultNumPartitions
	b.roundRobin[topic] =
		(next + 1) % defaultNumPartitions

	return int(next), nil
}

// partitionValue returns the explicit partition value or zero.
func partitionValue(partition *int32) int {
	if partition == nil {
		return 0
	}

	return int(*partition)
}
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

// TopicConfig controls retention for one topic.
// A zero value disables the corresponding retention policy.
type TopicConfig struct {
	RetentionBytes int64 `json:"retention_bytes,omitempty"`
	RetentionMs    int64 `json:"retention_ms,omitempty"`
	Compacted      bool  `json:"compacted,omitempty"`
	Compressed     bool  `json:"compressed,omitempty"`
}

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

	consumerGroupLag = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "consumer_group_lag",
			Help: "Consumer group lag in messages for each topic partition.",
		},
		[]string{"group_id", "topic", "partition"},
	)
	compressedBytesSavedTotal = prometheus.NewCounterVec(
			prometheus.CounterOpts{
					Name: "compressed_bytes_saved_total",
					Help: "Total bytes saved by Snappy compression for each topic.",
			},
			[]string{"topic"},
	)
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
	mustRegisterOnce(consumerGroupLag)
	mustRegisterOnce(compressedBytesSavedTotal)
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

	consumerGroupLagStop chan struct{}
	consumerGroupLagWG   sync.WaitGroup

	topicConfigs     map[string]TopicConfig
	retentionStarted map[string]bool
	retentionStop    chan struct{}
	retentionWG      sync.WaitGroup
	compactionStop chan struct{}
	compactionWG   sync.WaitGroup
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
		logs:                 make(map[string][]*log.Log),
		roundRobin:           make(map[string]uint64),
		dataDir:              dataDir,
		certFile:             certFile,
		keyFile:              keyFile,
		caFile:               caFile,
		consumerGroupLagStop: make(chan struct{}),
		topicConfigs:         make(map[string]TopicConfig),
		retentionStarted:     make(map[string]bool),
		retentionStop:        make(chan struct{}),
		compactionStop: 			make(chan struct{}),
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
			Listener:        listener,
			ServerTLSConfig: serverTLSConfig,
			ClientTLSConfig: clientTLSConfig,
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
		hasState, err := hraft.HasExistingState(
			logStore,
			stableStore,
			snapshotStore,
		)
		if err != nil {
			_ = raftNode.Shutdown().Error()
			_ = stableStore.Close()
			_ = logStore.Close()
			_ = transport.Close()
			_ = consumerGroups.Close()
			return nil, fmt.Errorf(
				"failed to check raft state: %w",
				err,
			)
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

			if err := raftNode.
				BootstrapCluster(configuration).
				Error(); err != nil {
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
	mux.HandleFunc("/admin/topics", b.handleAdminTopics)
	mux.HandleFunc("/admin/groups", b.handleAdminGroups)
	mux.HandleFunc("/admin/cluster", b.handleAdminCluster)

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

	b.startConsumerGroupLagUpdater()
	b.startCompactionUpdater()

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
		b.configurePartitionCompression(topic, partitionLog)
		return partitionLog, nil
	}

	b.mu.RUnlock()

	b.mu.Lock()

	if partitions, ok := b.logs[topic]; ok &&
		len(partitions) > partition {
		partitionLog := partitions[partition]
		b.mu.Unlock()
		b.configurePartitionCompression(topic, partitionLog)
		return partitionLog, nil
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
			b.mu.Unlock()

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

	shouldStartRetention := !b.retentionStarted[topic]
	if shouldStartRetention {
		b.retentionStarted[topic] = true
	}

	b.mu.Unlock()

	for _, partitionLog := range partitions {
		b.configurePartitionCompression(topic, partitionLog)
	}

	if shouldStartRetention {
		b.startTopicRetentionUpdater(topic)
	}

	return partitions[partition], nil
}

// SetTopicConfig updates the retention configuration for a topic.
func (b *Broker) SetTopicConfig(
	topic string,
	config TopicConfig,
) error {
	if topic == "" {
		topic = "default"
	}

	if config.RetentionBytes < 0 {
		return errors.New("retention_bytes must be non-negative")
	}

	if config.RetentionMs < 0 {
		return errors.New("retention_ms must be non-negative")
	}

	b.mu.Lock()
	b.topicConfigs[topic] = config
	started := b.retentionStarted[topic]
	b.mu.Unlock()

	if !started {
		if _, err := b.GetPartition(topic, 0); err != nil {
			return err
		}
	}

	return nil
}

// TopicConfig returns the current retention configuration for a topic.
func (b *Broker) TopicConfig(topic string) TopicConfig {
	if topic == "" {
		topic = "default"
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.topicConfigs[topic]
}

// startTopicRetentionUpdater starts one background retention worker per topic.
func (b *Broker) startTopicRetentionUpdater(topic string) {
	b.retentionWG.Add(1)

	go func() {
		defer b.retentionWG.Done()

		ticker := time.NewTicker(60 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				b.enforceTopicRetention(topic)

			case <-b.retentionStop:
				return
			}
		}
	}()
}

// enforceTopicRetention applies retention policies on the Raft leader only.
func (b *Broker) enforceTopicRetention(topic string) {
	if b.raft == nil ||
		b.raft.State() != hraft.Leader {
		return
	}

	b.mu.RLock()

	config := b.topicConfigs[topic]
	partitions := append(
		[]*log.Log(nil),
		b.logs[topic]...,
	)

	b.mu.RUnlock()

	if config.RetentionBytes <= 0 &&
		config.RetentionMs <= 0 {
		return
	}

	now := time.Now()

	for _, partitionLog := range partitions {
		if partitionLog == nil {
			continue
		}

		if err := partitionLog.EnforceRetention(
			config.RetentionBytes,
			config.RetentionMs,
			now,
		); err != nil {
			fmt.Fprintf(
				os.Stderr,
				"topic %s retention: %v\n",
				topic,
				err,
			)
		}
	}
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
		return 0, errors.New("broker is not the Raft leader")
	}

	selectedPartition, err := b.selectPartition(
		topic,
		key,
		partition,
	)
	if err != nil {
		return 0, err
	}

	_, err = b.GetPartition(topic, selectedPartition)
	if err != nil {
		return 0, err
	}

	// Zero-allocation binary encoding
	data := raftfsm.EncodeCommand(topic, selectedPartition, key, value)

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

// PublishBatch replicates an entire batch through Raft and returns the
// offsets assigned to the records.
func (b *Broker) PublishBatch(
	topic string,
	partition int32,
	records []*pb.Record,
) ([]int64, error) {
	if topic == "" {
		topic = "default"
	}

	if b.raft.State() != hraft.Leader {
		return nil, errors.New("broker is not the Raft leader")
	}

	if partition < 0 || partition >= defaultNumPartitions {
		return nil, fmt.Errorf(
			"partition %d out of range",
			partition,
		)
	}

	if len(records) == 0 {
		return nil, errors.New("records must not be empty")
	}

	_, err := b.GetPartition(topic, int(partition))
	if err != nil {
		return nil, err
	}

	commandRecords := make([]raftfsm.BatchRecord, len(records))
	for i, record := range records {
		if record == nil {
			return nil, fmt.Errorf("record %d is nil", i)
		}
		commandRecords[i] = raftfsm.BatchRecord{
			Key:   record.Key,
			Value: record.Value,
		}
	}

	// Zero-allocation binary encoding
	data := raftfsm.EncodeBatchCommand(topic, int(partition), commandRecords)

	future := b.raft.Apply(data, 5*time.Second)
	if err := future.Error(); err != nil {
		return nil, err
	}

	response := future.Response()
	switch v := response.(type) {
	case []uint64:
		offsets := make([]int64, len(v))
		for i, offset := range v {
			if offset > uint64(^uint64(0)>>1) {
				return nil, fmt.Errorf("offset %d exceeds int64 range", offset)
			}
			offsets[i] = int64(offset)
		}
		return offsets, nil
	case error:
		return nil, v
	default:
		return nil, fmt.Errorf("unexpected FSM response type %T", response)
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
	partitionLog, err := b.GetPartition(topic, partition)
	if err != nil {
		return nil, err
	}

	return partitionLog.Read(offset)
}

// ReadBatch reads up to maxCount messages from a topic partition.
func (b *Broker) ReadBatch(
	topic string,
	partition int,
	offset int64,
	maxCount int32,
) ([]*log.Message, error) {
	if offset < 0 {
		return nil, errors.New("offset must be non-negative")
	}

	if maxCount <= 0 {
		return nil, errors.New("max_count must be greater than zero")
	}

	if maxCount > 1000 {
		maxCount = 1000
	}

	if partition < 0 || partition >= defaultNumPartitions {
		return nil, fmt.Errorf(
			"partition %d out of range",
			partition,
		)
	}

	partitionLog, err := b.GetPartition(topic, partition)
	if err != nil {
		return nil, err
	}

	return partitionLog.ReadBatch(uint64(offset), maxCount)
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

// ProduceBatch implements the gRPC BrokerService ProduceBatch RPC.
func (b *Broker) ProduceBatch(
	ctx context.Context,
	req *pb.ProduceBatchRequest,
) (*pb.ProduceBatchResponse, error) {
	if req.Topic == "" {
		req.Topic = "default"
	}

	if req.Partition < 0 ||
		req.Partition >= defaultNumPartitions {
		produceErrorsTotal.Inc()

		return &pb.ProduceBatchResponse{
			Error: fmt.Sprintf(
				"partition %d out of range",
				req.Partition,
			),
		}, nil
	}

	if len(req.Records) == 0 {
		produceErrorsTotal.Inc()

		return &pb.ProduceBatchResponse{
			Error: "records must not be empty",
		}, nil
	}

	offsets, err := b.PublishBatch(
		req.Topic,
		req.Partition,
		req.Records,
	)
	if err != nil {
		produceErrorsTotal.Inc()

		return &pb.ProduceBatchResponse{
			Error: err.Error(),
		}, nil
	}

	messagesProducedTotal.Add(float64(len(offsets)))

	return &pb.ProduceBatchResponse{
		Offsets: offsets,
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

// ConsumeBatch implements the gRPC BrokerService ConsumeBatch RPC.
func (b *Broker) ConsumeBatch(
	ctx context.Context,
	req *pb.ConsumeBatchRequest,
) (*pb.ConsumeBatchResponse, error) {
	if req.Topic == "" {
		req.Topic = "default"
	}

	if req.Partition < 0 ||
		req.Partition >= defaultNumPartitions {
		consumeErrorsTotal.Inc()

		return nil, fmt.Errorf(
			"partition %d out of range",
			req.Partition,
		)
	}

	if req.Offset < 0 {
		consumeErrorsTotal.Inc()
		return nil, errors.New("offset must be non-negative")
	}

	if req.MaxCount <= 0 {
		consumeErrorsTotal.Inc()
		return nil, errors.New("max_count must be greater than zero")
	}

	if req.MaxCount > 1000 {
		req.MaxCount = 1000
	}

	messages, err := b.ReadBatch(
		req.Topic,
		int(req.Partition),
		req.Offset,
		req.MaxCount,
	)
	if err != nil {
		consumeErrorsTotal.Inc()
		return nil, err
	}

	response := &pb.ConsumeBatchResponse{
		Records: make([]*pb.ConsumedRecord, 0, len(messages)),
	}

	for _, message := range messages {
		response.Records = append(
			response.Records,
			&pb.ConsumedRecord{
				Offset:    int64(message.Offset),
				Key:       message.Key,
				Value:     message.Value,
				Timestamp: message.Timestamp,
			},
		)
	}

	messagesConsumedTotal.Add(float64(len(response.Records)))

	return response, nil
}

// CommitOffset implements the gRPC BrokerService CommitOffset RPC.
func (b *Broker) CommitOffset(
	ctx context.Context,
	req *pb.CommitOffsetRequest,
) (*pb.CommitOffsetResponse, error) {
	if req.GroupId == "" {
		return nil, errors.New("group_id is required")
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

	b.updateConsumerGroupLagFor(
		req.GroupId,
		req.Topic,
		req.Partition,
		req.Offset,
	)

	return &pb.CommitOffsetResponse{}, nil
}

// FetchOffset implements the gRPC BrokerService FetchOffset RPC.
func (b *Broker) FetchOffset(
	ctx context.Context,
	req *pb.FetchOffsetRequest,
) (*pb.FetchOffsetResponse, error) {
	if req.GroupId == "" {
		return nil, errors.New("group_id is required")
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

	if b.compactionStop != nil {
		close(b.compactionStop)
		b.compactionStop = nil
		b.compactionWG.Wait()
	}

	if b.retentionStop != nil {
		close(b.retentionStop)
		b.retentionStop = nil
		b.retentionWG.Wait()
	}

	if b.consumerGroupLagStop != nil {
		close(b.consumerGroupLagStop)
		b.consumerGroupLagStop = nil
		b.consumerGroupLagWG.Wait()
	}

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

// updateConsumerGroupLagFor updates the lag gauge for one committed group/topic/partition.
func (b *Broker) updateConsumerGroupLagFor(
	groupID string,
	topic string,
	partition int32,
	committedOffset uint64,
) {
	partitionLog, err := b.GetPartition(topic, int(partition))
	if err != nil {
		return
	}

	latestOffset := partitionLog.LatestOffset()
	lag := uint64(0)

	if latestOffset >= committedOffset {
		lag = latestOffset - committedOffset
	}

	consumerGroupLag.WithLabelValues(
		groupID,
		topic,
		strconv.FormatInt(int64(partition), 10),
	).Set(float64(lag))
}

// updateConsumerGroupLag refreshes lag for all persisted consumer-group offsets.
func (b *Broker) updateConsumerGroupLag() {
	for _, committed := range b.consumerGroups.Snapshot() {
		b.updateConsumerGroupLagFor(
			committed.GroupID,
			committed.Topic,
			committed.Partition,
			committed.Offset,
		)
	}
}

// startConsumerGroupLagUpdater refreshes consumer-group lag every 15 seconds.
func (b *Broker) startConsumerGroupLagUpdater() {
	b.consumerGroupLagWG.Add(1)

	go func() {
		defer b.consumerGroupLagWG.Done()

		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				b.updateConsumerGroupLag()

			case <-b.consumerGroupLagStop:
				return
			}
		}
	}()
}

// handleJoin handles requests to add a broker as a Raft voter.
func (b *Broker) handleJoin(
	w http.ResponseWriter,
	r *http.Request,
) {
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

	configurationFuture := b.raft.GetConfiguration()

	if err := configurationFuture.Error(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	for _, server := range configurationFuture.Configuration().Servers {
		if server.ID == hraft.ServerID(request.ID) &&
			server.Suffrage == hraft.Voter {
			w.WriteHeader(http.StatusOK)
			return
		}
	}

	if err := b.Join(request.ID, request.Addr); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
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
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var request struct {
		Topic     string `json:"topic"`
		Partition *int32 `json:"partition"`
		Key       string `json:"key"`
		Value     string `json:"value"`
	}

	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		produceErrorsTotal.Inc()
		http.Error(w, err.Error(), http.StatusBadRequest)
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
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	messagesProducedTotal.Inc()

	w.Header().Set("Content-Type", "application/json")

	_ = json.NewEncoder(w).Encode(struct {
		Offset    uint64 `json:"offset"`
		Topic     string `json:"topic"`
		Partition int    `json:"partition"`
	}{
		Offset:    offset,
		Topic:     request.Topic,
		Partition: partitionValue(request.Partition),
	})
}

// handleConsume handles HTTP requests to read a message from a topic partition.
func (b *Broker) handleConsume(
	w http.ResponseWriter,
	r *http.Request,
) {
	if r.Method != http.MethodGet {
		consumeErrorsTotal.Inc()
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	topic := r.URL.Query().Get("topic")
	if topic == "" {
		topic = "default"
	}

	partition := 0

	if value := r.URL.Query().Get("partition"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil {
			consumeErrorsTotal.Inc()
			http.Error(w, "invalid partition", http.StatusBadRequest)
			return
		}

		partition = parsed
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

	message, err := b.Read(topic, partition, offset)
	if err != nil {
		consumeErrorsTotal.Inc()
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	messagesConsumedTotal.Inc()

	w.Header().Set("Content-Type", "application/json")

	_ = json.NewEncoder(w).Encode(struct {
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
	})
}

// handleCommitOffset handles HTTP consumer-group offset commits.
func (b *Broker) handleCommitOffset(
	w http.ResponseWriter,
	r *http.Request,
) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var request struct {
		GroupID   string `json:"group_id"`
		Topic     string `json:"topic"`
		Partition int32  `json:"partition"`
		Offset    uint64 `json:"offset"`
	}

	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if request.GroupID == "" {
		http.Error(w, "group_id is required", http.StatusBadRequest)
		return
	}

	if request.Topic == "" {
		request.Topic = "default"
	}

	if request.Partition < 0 ||
		request.Partition >= defaultNumPartitions {
		http.Error(w, "invalid partition", http.StatusBadRequest)
		return
	}

	if err := b.consumerGroups.CommitOffset(
		request.GroupID,
		request.Topic,
		request.Partition,
		request.Offset,
	); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	b.updateConsumerGroupLagFor(
		request.GroupID,
		request.Topic,
		request.Partition,
		request.Offset,
	)

	w.WriteHeader(http.StatusOK)
}

// handleFetchOffset handles HTTP requests for a committed group offset.
func (b *Broker) handleFetchOffset(
	w http.ResponseWriter,
	r *http.Request,
) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	groupID := r.URL.Query().Get("group_id")
	if groupID == "" {
		http.Error(w, "missing group_id", http.StatusBadRequest)
		return
	}

	topic := r.URL.Query().Get("topic")
	if topic == "" {
		topic = "default"
	}

	partitionString := r.URL.Query().Get("partition")
	if partitionString == "" {
		http.Error(w, "missing partition", http.StatusBadRequest)
		return
	}

	partition, err := strconv.ParseInt(partitionString, 10, 32)
	if err != nil ||
		partition < 0 ||
		partition >= defaultNumPartitions {
		http.Error(w, "invalid partition", http.StatusBadRequest)
		return
	}

	offset := b.consumerGroups.FetchOffset(
		groupID,
		topic,
		int32(partition),
	)

	w.Header().Set("Content-Type", "application/json")

	_ = json.NewEncoder(w).Encode(struct {
		Offset uint64 `json:"offset"`
	}{
		Offset: offset,
	})
}

// joinPeer registers this broker with the configured peer.
func (b *Broker) joinPeer(
	id string,
	addr string,
	peer string,
) error {
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

	return net.JoinHostPort(host, strconv.Itoa(port+1000)), nil
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

		return int(hash.Sum32() % defaultNumPartitions), nil
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	next := b.roundRobin[topic] % defaultNumPartitions
	b.roundRobin[topic] = (next + 1) % defaultNumPartitions

	return int(next), nil
}

// partitionValue returns the explicit partition value or zero.
func partitionValue(partition *int32) int {
	if partition == nil {
		return 0
	}

	return int(*partition)
}

// handleAdminTopics handles the read-only topic/partition admin endpoint.
func (b *Broker) handleAdminTopics(
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

	type partitionInfo struct {
		ID           int    `json:"id"`
		LatestOffset uint64 `json:"latest_offset"`
	}

	type topicInfo struct {
		Topic      string          `json:"topic"`
		Partitions []partitionInfo `json:"partitions"`
	}

	b.mu.RLock()

	result := make([]topicInfo, 0, len(b.logs))

	for topic, partitions := range b.logs {
		info := topicInfo{
			Topic:      topic,
			Partitions: make([]partitionInfo, 0, len(partitions)),
		}

		for partitionID, partitionLog := range partitions {
			if partitionLog == nil {
				continue
			}

			info.Partitions = append(info.Partitions, partitionInfo{
				ID:           partitionID,
				LatestOffset: partitionLog.LatestOffset(),
			})
		}

		result = append(result, info)
	}

	b.mu.RUnlock()

	b.writeAdminJSON(w, result)
}

// handleAdminGroups handles the read-only consumer-group admin endpoint.
func (b *Broker) handleAdminGroups(
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

	type offsetInfo struct {
		Topic     string `json:"topic"`
		Partition int32  `json:"partition"`
		Committed uint64 `json:"committed"`
		Lag       uint64 `json:"lag"`
	}

	type groupInfo struct {
		GroupID string       `json:"group_id"`
		Offsets []offsetInfo `json:"offsets"`
	}

	groupMap := make(map[string]*groupInfo)
	committedOffsets := b.consumerGroups.Snapshot()

	b.mu.RLock()

	for _, committed := range committedOffsets {
		group, ok := groupMap[committed.GroupID]
		if !ok {
			group = &groupInfo{
				GroupID: committed.GroupID,
				Offsets: make([]offsetInfo, 0),
			}

			groupMap[committed.GroupID] = group
		}

		var latestOffset uint64

		if partitions, ok := b.logs[committed.Topic]; ok &&
			int(committed.Partition) >= 0 &&
			int(committed.Partition) < len(partitions) {
			if partitionLog := partitions[int(committed.Partition)]; partitionLog != nil {
				latestOffset = partitionLog.LatestOffset()
			}
		}

		lag := uint64(0)

		if latestOffset >= committed.Offset {
			lag = latestOffset - committed.Offset
		}

		group.Offsets = append(
			group.Offsets,
			offsetInfo{
				Topic:     committed.Topic,
				Partition: committed.Partition,
				Committed: committed.Offset,
				Lag:       lag,
			},
		)
	}

	b.mu.RUnlock()

	result := make([]groupInfo, 0, len(groupMap))

	for _, group := range groupMap {
		result = append(result, *group)
	}

	b.writeAdminJSON(w, result)
}

// handleAdminCluster handles the read-only Raft cluster admin endpoint.
func (b *Broker) handleAdminCluster(
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

	configurationFuture := b.raft.GetConfiguration()

	if err := configurationFuture.Error(); err != nil {
		http.Error(
			w,
			err.Error(),
			http.StatusInternalServerError,
		)
		return
	}

	leader, _ := b.raft.LeaderWithID()
	configuration := configurationFuture.Configuration()

	peers := make([]string, 0, len(configuration.Servers))

	for _, server := range configuration.Servers {
		peers = append(
			peers,
			string(server.Address),
		)
	}

	result := struct {
		State  string   `json:"state"`
		Leader string   `json:"leader"`
		Peers  []string `json:"peers"`
	}{
		State:  b.raft.State().String(),
		Leader: string(leader),
		Peers:  peers,
	}

	b.writeAdminJSON(w, result)
}

// writeAdminJSON writes a pretty-printed JSON response for admin endpoints.
func (b *Broker) writeAdminJSON(
	w http.ResponseWriter,
	v interface{},
) {
	data, err := json.MarshalIndent(
		v,
		"",
		"  ",
	)
	if err != nil {
		http.Error(
			w,
			err.Error(),
			http.StatusInternalServerError,
		)
		return
	}

	w.Header().Set(
		"Content-Type",
		"application/json",
	)

	w.WriteHeader(http.StatusOK)

	_, _ = w.Write(data)
	_, _ = w.Write([]byte("\n"))
}

// startCompactionUpdater starts the background topic compaction worker.
// Compaction runs every five minutes and only on the current Raft leader.
func (b *Broker) startCompactionUpdater() {
	b.compactionWG.Add(1)

	go func() {
		defer b.compactionWG.Done()

		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				b.compactConfiguredTopics()

			case <-b.compactionStop:
				return
			}
		}
	}()
}

// compactConfiguredTopics compacts all configured compacted topics.
// Only the Raft leader performs compaction.
func (b *Broker) compactConfiguredTopics() {
	if b.raft == nil ||
		b.raft.State() != hraft.Leader {
		return
	}

	b.mu.RLock()

	type topicPartitions struct {
		topic      string
		partitions []*log.Log
	}

	topics := make(
		[]topicPartitions,
		0,
	)

	for topic, config := range b.topicConfigs {
		if !config.Compacted {
			continue
		}

		partitions, ok := b.logs[topic]
		if !ok {
			continue
		}

		topics = append(
			topics,
			topicPartitions{
				topic: topic,
				partitions: append(
					[]*log.Log(nil),
					partitions...,
				),
			},
		)
	}

	b.mu.RUnlock()

	for _, topic := range topics {
		// Leadership may change while a compaction cycle is running.
		// Stop processing immediately if this broker is no longer leader.
		if b.raft == nil ||
			b.raft.State() != hraft.Leader {
			return
		}

		for _, partitionLog := range topic.partitions {
			if partitionLog == nil {
				continue
			}

			if err := partitionLog.Compact(); err != nil {
				fmt.Fprintf(
					os.Stderr,
					"topic %s compaction: %v\n",
					topic.topic,
					err,
				)
			}
		}
	}
}

func (b *Broker) configurePartitionCompression(
	topic string,
	partitionLog *log.Log,
) {
	if partitionLog == nil {
		return
	}

	config := b.TopicConfig(topic)
	partitionLog.SetCompressed(config.Compressed)
	partitionLog.SetCompressionSavedObserver(func(saved uint64) {
		compressedBytesSavedTotal.WithLabelValues(topic).Add(float64(saved))
	})
}
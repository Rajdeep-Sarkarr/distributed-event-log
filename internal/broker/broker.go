package broker

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	hraft "github.com/hashicorp/raft"

	"distributed-event-log/internal/log"
	"distributed-event-log/internal/raft"
)

// PeerAddress identifies a Raft peer by ID and in-memory transport address.
type PeerAddress struct {
	ID string
	Address string
}

// Broker owns a commit log and a Raft node.
type Broker struct {
	log *log.Log
	raft *hraft.Raft
}

var (
	transportsMu sync.Mutex
	transports=make(map[string]*hraft.InmemTransport)
)

// NewBroker creates a broker with a commit log and an in-memory Raft node.
func NewBroker(id string, dir string, peers []PeerAddress) (*Broker, error) {
	commitLog, err := log.NewLog(dir)
	if err != nil {
		return nil, err
	}

	fsm := raft.NewFSM(commitLog)

	raftConfig := hraft.DefaultConfig()
	raftConfig.LocalID=hraft.ServerID(id)

	address := hraft.ServerAddress(id)
	for _, peer := range peers {
		if peer.ID == id && peer.Address != "" {
			address=hraft.ServerAddress(peer.Address)
			break
		}
	}

	localAddress, transport := hraft.NewInmemTransport(address)

	transportsMu.Lock()
	transports[string(localAddress)]=transport

	for _, peer := range peers {
		if existing, ok := transports[peer.Address]; ok {
			transport.Connect(hraft.ServerAddress(peer.Address), existing)
			existing.Connect(localAddress, transport)
		}
	}
	transportsMu.Unlock()

	logStore := hraft.NewInmemStore()
	stableStore := hraft.NewInmemStore()
	snapshotStore := hraft.NewInmemSnapshotStore()

	hasState, err := hraft.HasExistingState(logStore, stableStore, snapshotStore)
	if err != nil {
		_=commitLog.Close()
		return nil, err
	}

	raftNode, err := hraft.NewRaft(
		raftConfig,
		fsm,
		logStore,
		stableStore,
		snapshotStore,
		transport,
	)
	if err != nil {
		_=commitLog.Close()
		return nil, err
	}

	if !hasState {
		servers := []hraft.Server{
			{
				ID:      hraft.ServerID(id),
				Address: localAddress,
			},
		}

		for _, peer := range peers {
			if peer.ID != id {
				servers=append(servers, hraft.Server{
					ID:      hraft.ServerID(peer.ID),
					Address: hraft.ServerAddress(peer.Address),
				})
			}
		}

		configuration := hraft.Configuration{
			Servers: servers,
		}

		if err := raftNode.BootstrapCluster(configuration).Error(); err != nil {
			_=raftNode.Shutdown().Error()
			_=commitLog.Close()
			return nil, err
		}
	}

	return &Broker{
		log:  commitLog,
		raft: raftNode,
	}, nil
}

// IsLeader reports whether this broker is currently the Raft leader.
func (b *Broker) IsLeader() bool {
	return b.raft.State() == hraft.Leader
}

// Publish publishes a key-value command through Raft and returns its commit log offset.
func (b *Broker) Publish(key, value []byte) (uint64, error) {
	if b.raft.State() != hraft.Leader {
		return 0, errors.New("broker is not the Raft leader")
	}

	command := raft.Command{
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

// Leader returns the current Raft leader's address, or empty string if unknown.
func (b *Broker) Leader() string {
	address, _ := b.raft.LeaderWithID()
	return string(address)
}

// Read reads a message directly from the broker's commit log.
func (b *Broker) Read(offset uint64) (*log.Message, error) {
	return b.log.Read(offset)
}

// Close shuts down the Raft node and then closes the commit log.
func (b *Broker) Close() error {
	var firstErr error

	if err := b.raft.Shutdown().Error(); err != nil {
		firstErr=err
	}

	if err := b.log.Close(); err != nil && firstErr == nil {
		firstErr=err
	}

	return firstErr
}
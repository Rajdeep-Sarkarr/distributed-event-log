package raft

import (
	"encoding/json"
	"fmt"
	"io"

	hraft "github.com/hashicorp/raft"

	"distributed-event-log/internal/log"
)

// Command represents a replicated message append operation for a topic partition.
type Command struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Key       []byte `json:"key"`
	Value     []byte `json:"value"`
}

// LogProvider returns the commit log for a topic partition.
type LogProvider interface {
	GetPartition(topic string, partition int) (*log.Log, error)
}

// FSM applies Raft commands to the appropriate topic partition commit log.
type FSM struct {
	logs LogProvider
}

// NewFSM creates a Raft FSM backed by the supplied topic partition log provider.
func NewFSM(logs LogProvider) *FSM {
	return &FSM{
		logs: logs,
	}
}

// Apply decodes a Raft command, appends it to the selected partition log,
// and returns the assigned partition offset.
func (f *FSM) Apply(entry *hraft.Log) interface{} {
	var command Command

	if err := json.Unmarshal(entry.Data, &command); err != nil {
		return fmt.Errorf("decode raft command: %w", err)
	}

	if command.Topic == "" {
		command.Topic = "default"
	}

	partitionLog, err := f.logs.GetPartition(command.Topic, command.Partition)
	if err != nil {
		return fmt.Errorf("get partition log: %w", err)
	}

	offset, err := partitionLog.Append(int64(entry.Index), command.Key, command.Value)
	if err != nil {
		return fmt.Errorf("append to partition: %w", err)
	}

	return offset
}

// Snapshot returns a no-op Raft snapshot for Phase 3A.
func (f *FSM) Snapshot() (hraft.FSMSnapshot, error) {
	return &noopSnapshot{}, nil
}

// Restore performs no state restoration for the Phase 3A no-op snapshot.
func (f *FSM) Restore(_ io.ReadCloser) error {
	return nil
}

// noopSnapshot is an empty Raft snapshot implementation.
type noopSnapshot struct{}

// Persist writes no snapshot state and marks the snapshot successful.
func (s *noopSnapshot) Persist(sink hraft.SnapshotSink) error {
	if err := sink.Close(); err != nil {
		return err
	}

	return nil
}

// Release releases the no-op snapshot.
func (s *noopSnapshot) Release() {}
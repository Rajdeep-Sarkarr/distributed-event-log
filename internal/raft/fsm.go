package raft

import (
	"encoding/json"
	"io"

	hraft "github.com/hashicorp/raft"

	"distributed-event-log/internal/log"
)

// Command represents a command applied to the commit log through Raft.
type Command struct {
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
}

// FSM implements the hashicorp/raft FSM interface using the commit log.
type FSM struct {
	log *log.Log
}

// NewFSM creates a Raft FSM backed by the supplied commit log.
func NewFSM(commitLog *log.Log) *FSM {
	return &FSM{
		log: commitLog,
	}
}

// Apply decodes a Raft log entry as a Command, appends it to the commit log,
// and returns the assigned commit log offset.
func (f *FSM) Apply(entry *hraft.Log) interface{} {
	var command Command

	if err := json.Unmarshal(entry.Data, &command); err != nil {
		return err
	}

	offset, err := f.log.Append(entry.AppendedAt.UnixNano(), command.Key, command.Value)
	if err != nil {
		return err
	}

	return offset
}

// Snapshot returns a no-op snapshot for Phase 1.
func (f *FSM) Snapshot() (hraft.FSMSnapshot, error) {
	return &snapshot{}, nil
}

// Restore performs no operation for Phase 1.
func (f *FSM) Restore(reader io.ReadCloser) error {
	return nil
}

// snapshot is a no-op Raft snapshot implementation for Phase 1.
type snapshot struct{}

// Persist performs no operation for the Phase 1 no-op snapshot.
func (s *snapshot) Persist(sink hraft.SnapshotSink) error {
    return sink.Close()
}

// Release performs no operation for the Phase 1 no-op snapshot.
func (s *snapshot) Release() {}
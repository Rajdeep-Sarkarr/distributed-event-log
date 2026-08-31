package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"

	hraft "github.com/hashicorp/raft"

	"distributed-event-log/internal/log"
)

const (
	CmdTypeSingle byte = 1
	CmdTypeBatch  byte = 2
)

// BatchRecord represents one message in a replicated batch.
type BatchRecord struct {
	Key   []byte
	Value []byte
}

// EncodeCommand encodes a single message append command into a compact binary frame.
func EncodeCommand(topic string, partition int, key, value []byte) []byte {
	topicBytes := []byte(topic)
	totalLen := 1 + 2 + len(topicBytes) + 4 + 4 + len(key) + 4 + len(value)
	buf := make([]byte, totalLen)

	buf[0] = CmdTypeSingle
	binary.BigEndian.PutUint16(buf[1:3], uint16(len(topicBytes)))
	copy(buf[3:3+len(topicBytes)], topicBytes)

	offset := 3 + len(topicBytes)
	binary.BigEndian.PutUint32(buf[offset:offset+4], uint32(partition))
	offset += 4

	binary.BigEndian.PutUint32(buf[offset:offset+4], uint32(len(key)))
	offset += 4
	copy(buf[offset:offset+len(key)], key)
	offset += len(key)

	binary.BigEndian.PutUint32(buf[offset:offset+4], uint32(len(value)))
	offset += 4
	copy(buf[offset:offset+len(value)], value)

	return buf
}

// EncodeBatchCommand encodes a batch append command into a compact binary frame.
func EncodeBatchCommand(topic string, partition int, records []BatchRecord) []byte {
	topicBytes := []byte(topic)
	totalLen := 1 + 2 + len(topicBytes) + 4 + 4
	for _, r := range records {
		totalLen += 4 + len(r.Key) + 4 + len(r.Value)
	}

	buf := make([]byte, totalLen)
	buf[0] = CmdTypeBatch
	binary.BigEndian.PutUint16(buf[1:3], uint16(len(topicBytes)))
	copy(buf[3:3+len(topicBytes)], topicBytes)

	offset := 3 + len(topicBytes)
	binary.BigEndian.PutUint32(buf[offset:offset+4], uint32(partition))
	offset += 4

	binary.BigEndian.PutUint32(buf[offset:offset+4], uint32(len(records)))
	offset += 4

	for _, r := range records {
		binary.BigEndian.PutUint32(buf[offset:offset+4], uint32(len(r.Key)))
		offset += 4
		copy(buf[offset:offset+len(r.Key)], r.Key)
		offset += len(r.Key)

		binary.BigEndian.PutUint32(buf[offset:offset+4], uint32(len(r.Value)))
		offset += 4
		copy(buf[offset:offset+len(r.Value)], r.Value)
		offset += len(r.Value)
	}

	return buf
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

// Apply decodes a binary Raft command and appends it to the selected partition log.
func (f *FSM) Apply(entry *hraft.Log) interface{} {
	data := entry.Data
	if len(data) < 7 {
		return errors.New("malformed raft command: payload too short")
	}

	cmdType := data[0]
	topicLen := int(binary.BigEndian.Uint16(data[1:3]))
	if len(data) < 3+topicLen+4 {
		return errors.New("malformed raft command: topic truncated")
	}

	topic := string(data[3 : 3+topicLen])
	if topic == "" {
		topic = "default"
	}

	offset := 3 + topicLen
	partition := int(binary.BigEndian.Uint32(data[offset : offset+4]))
	offset += 4

	partitionLog, err := f.logs.GetPartition(topic, partition)
	if err != nil {
		return fmt.Errorf("get partition log: %w", err)
	}

	now := time.Now().UnixNano()

	switch cmdType {
	case CmdTypeSingle:
		if len(data) < offset+4 {
			return errors.New("malformed single command: key length truncated")
		}
		keyLen := int(binary.BigEndian.Uint32(data[offset : offset+4]))
		offset += 4
		if len(data) < offset+keyLen+4 {
			return errors.New("malformed single command: key payload truncated")
		}
		key := data[offset : offset+keyLen]
		offset += keyLen

		valLen := int(binary.BigEndian.Uint32(data[offset : offset+4]))
		offset += 4
		if len(data) < offset+valLen {
			return errors.New("malformed single command: value payload truncated")
		}
		val := data[offset : offset+valLen]

		appendedOffset, err := partitionLog.Append(now, key, val)
		if err != nil {
			return fmt.Errorf("append to partition: %w", err)
		}
		return appendedOffset

	case CmdTypeBatch:
		if len(data) < offset+4 {
			return errors.New("malformed batch command: count truncated")
		}
		count := int(binary.BigEndian.Uint32(data[offset : offset+4]))
		offset += 4

		records := make([]log.Record, count)
		for i := 0; i < count; i++ {
			if len(data) < offset+4 {
				return fmt.Errorf("malformed batch record %d: key length truncated", i)
			}
			keyLen := int(binary.BigEndian.Uint32(data[offset : offset+4]))
			offset += 4
			if len(data) < offset+keyLen+4 {
				return fmt.Errorf("malformed batch record %d: key payload truncated", i)
			}
			key := data[offset : offset+keyLen]
			offset += keyLen

			valLen := int(binary.BigEndian.Uint32(data[offset : offset+4]))
			offset += 4
			if len(data) < offset+valLen {
				return fmt.Errorf("malformed batch record %d: value payload truncated", i)
			}
			val := data[offset : offset+valLen]
			offset += valLen

			records[i] = log.Record{
				Timestamp: now,
				Key:       key,
				Value:     val,
			}
		}

		offsets, err := partitionLog.AppendBatch(records)
		if err != nil {
			return fmt.Errorf("append batch to partition: %w", err)
		}
		return offsets

	default:
		return fmt.Errorf("unknown raft command type: %d", cmdType)
	}
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
	return sink.Close()
}

// Release releases the no-op snapshot.
func (s *noopSnapshot) Release() {}
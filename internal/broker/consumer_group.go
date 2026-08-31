package broker

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"sync"

	bbolt "go.etcd.io/bbolt"
)

const consumerGroupsBucket = "consumer-groups"

// ConsumerGroupOffset represents one committed consumer-group offset.
type ConsumerGroupOffset struct {
	GroupID   string
	Topic     string
	Partition int32
	Offset    uint64
}

// ConsumerGroupStore persists and caches committed consumer-group offsets.
type ConsumerGroupStore struct {
	db *bbolt.DB

	mu      sync.RWMutex
	offsets map[string]uint64
}

// NewConsumerGroupStore opens the consumer-group BoltDB database and loads
// all existing committed offsets into memory.
func NewConsumerGroupStore(path string) (*ConsumerGroupStore, error) {
	db, err := bbolt.Open(path, 0644, nil)
	if err != nil {
		return nil, err
	}

	store := &ConsumerGroupStore{
		db:      db,
		offsets: make(map[string]uint64),
	}

	err = db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(consumerGroupsBucket))
		return err
	})
	if err != nil {
		_ = db.Close()
		return nil, err
	}

	err = store.load()
	if err != nil {
		_ = db.Close()
		return nil, err
	}

	return store, nil
}

// load reads all persisted consumer-group offsets into the in-memory cache.
func (s *ConsumerGroupStore) load() error {
	return s.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(consumerGroupsBucket))
		if bucket == nil {
			return fmt.Errorf("consumer-groups bucket does not exist")
		}

		return bucket.ForEach(func(k, v []byte) error {
			if len(v) != 8 {
				return fmt.Errorf(
					"invalid committed offset for key %q",
					string(k),
				)
			}

			offset := binary.BigEndian.Uint64(v)
			s.offsets[string(k)] = offset

			return nil
		})
	})
}

// CommitOffset stores a group's committed offset in memory and persists it to BoltDB.
func (s *ConsumerGroupStore) CommitOffset(
	groupID string,
	topic string,
	partition int32,
	offset uint64,
) error {
	if strings.ContainsRune(groupID, 0) || strings.ContainsRune(topic, 0) {
		return errors.New("groupID and topic must not contain null bytes")
	}

	key := offsetKey(groupID, topic, partition)

	var value [8]byte
	binary.BigEndian.PutUint64(value[:], offset)

	// Persist to BoltDB first without holding the in-memory cache lock
	err := s.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(consumerGroupsBucket))
		if bucket == nil {
			return fmt.Errorf("consumer-groups bucket does not exist")
		}

		return bucket.Put([]byte(key), value[:])
	})
	if err != nil {
		return err
	}

	// Update in-memory cache under write lock
	s.mu.Lock()
	s.offsets[key] = offset
	s.mu.Unlock()

	return nil
}

// FetchOffset returns a group's committed offset, or zero if none exists.
func (s *ConsumerGroupStore) FetchOffset(
	groupID string,
	topic string,
	partition int32,
) uint64 {
	key := offsetKey(groupID, topic, partition)

	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.offsets[key]
}

// Snapshot returns a copy of all committed consumer-group offsets.
func (s *ConsumerGroupStore) Snapshot() []ConsumerGroupOffset {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]ConsumerGroupOffset, 0, len(s.offsets))

	for key, offset := range s.offsets {
		firstSeparator := strings.IndexByte(key, 0)
		if firstSeparator < 0 {
			continue
		}

		secondSeparator := strings.IndexByte(
			key[firstSeparator+1:],
			0,
		)
		if secondSeparator < 0 {
			continue
		}

		secondSeparator += firstSeparator + 1

		partitionBytes := key[secondSeparator+1:]
		if len(partitionBytes) != 4 {
			continue
		}

		partition := int32(
			binary.BigEndian.Uint32([]byte(partitionBytes)),
		)

		result = append(result, ConsumerGroupOffset{
			GroupID:   key[:firstSeparator],
			Topic:     key[firstSeparator+1 : secondSeparator],
			Partition: partition,
			Offset:    offset,
		})
	}

	return result
}

// Close closes the consumer-group BoltDB database.
func (s *ConsumerGroupStore) Close() error {
	return s.db.Close()
}

// offsetKey creates the unique cache/database key for a group topic partition.
func offsetKey(groupID string, topic string, partition int32) string {
	var partitionBytes [4]byte
	binary.BigEndian.PutUint32(
		partitionBytes[:],
		uint32(partition),
	)

	return groupID +
		"\x00" +
		topic +
		"\x00" +
		string(partitionBytes[:])
}
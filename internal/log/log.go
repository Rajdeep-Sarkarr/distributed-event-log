package log

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/golang/snappy"
)

const (
	// DefaultMaxSegmentSize is the default maximum segment size of one gigabyte.
	DefaultMaxSegmentSize uint64 = 1024 * 1024 * 1024
)

// Record represents one message to append to the commit log.
type Record struct {
	Timestamp int64
	Key       []byte
	Value     []byte
}

// Log manages the ordered collection of commit log segments.
type Log struct {
	dir                      string
	maxSegmentSize           uint64
	segments                 []*Segment
	compressed               bool
	compressionSavedObserver func(uint64)

	mu sync.RWMutex
}

// NewLog opens an existing log directory and loads its segments in offset order.
func NewLog(dir string, maxSegmentSize ...uint64) (*Log, error) {
	segmentSize := uint64(DefaultMaxSegmentSize)

	if len(maxSegmentSize) > 1 {
		return nil, errors.New("at most one max segment size may be provided")
	}

	if len(maxSegmentSize) == 1 {
		if maxSegmentSize[0] == 0 {
			return nil, errors.New("max segment size must be greater than zero")
		}
		segmentSize = maxSegmentSize[0]
	}

	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	offsets := make([]uint64, 0)

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		offset, ok := segmentBaseOffset(entry.Name())
		if !ok {
			continue
		}

		indexPath := filepath.Join(dir, fmt.Sprintf("%d.index", offset))
		if _, err := os.Stat(indexPath); err != nil {
			if os.IsNotExist(err) {
				return nil, fmt.Errorf("missing index file for segment %d", offset)
			}
			return nil, err
		}

		offsets = append(offsets, offset)
	}

	sort.Slice(offsets, func(i, j int) bool {
		return offsets[i] < offsets[j]
	})

	log := &Log{
		dir:            dir,
		maxSegmentSize: segmentSize,
		segments:       make([]*Segment, 0, len(offsets)),
	}

	for _, offset := range offsets {
		segment, err := NewSegment(dir, offset)
		if err != nil {
			_ = log.Close()
			return nil, err
		}

		segment.SetCompressed(log.compressed)
		log.segments = append(log.segments, segment)
	}

	if len(log.segments) == 0 {
		segment, err := NewSegment(dir, 0)
		if err != nil {
			return nil, err
		}

		log.segments = append(log.segments, segment)
	}

	for i := 0; i < len(log.segments)-1; i++ {
		log.segments[i].nextOffset = log.segments[i+1].baseOffset
	}

	return log, nil
}

// SetCompressed enables or disables Snappy compression for this log.
func (l *Log) SetCompressed(compressed bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.compressed = compressed
	for _, segment := range l.segments {
		segment.SetCompressed(compressed)
	}
}

// SetCompressionSavedObserver installs a callback invoked after a compressed record is written.
func (l *Log) SetCompressionSavedObserver(observer func(uint64)) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.compressionSavedObserver = observer
}

// Append writes a single message to the active segment.
func (l *Log) Append(timestamp int64, key, value []byte) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.appendLocked(timestamp, key, value)
}

// AppendBatch appends all records in a single contiguous I/O write block.
func (l *Log) AppendBatch(records []Record) ([]uint64, error) {
	if len(records) == 0 {
		return nil, nil
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if len(l.segments) == 0 {
		return nil, errors.New("log has no segments")
	}

	active := l.segments[len(l.segments)-1]

	offsets, bytesSaved, err := active.AppendBatch(records)
	if err != nil {
		return nil, err
	}

	if bytesSaved > 0 && l.compressionSavedObserver != nil {
		l.compressionSavedObserver(uint64(bytesSaved))
	}

	if uint64(active.Size()) >= l.maxSegmentSize {
		nextSegment, err := NewSegment(l.dir, offsets[len(offsets)-1]+1)
		if err != nil {
			return offsets, err
		}
		nextSegment.SetCompressed(l.compressed)
		l.segments = append(l.segments, nextSegment)
	}

	return offsets, nil
}

func (l *Log) appendLocked(timestamp int64, key, value []byte) (uint64, error) {
	if len(l.segments) == 0 {
		return 0, errors.New("log has no segments")
	}

	active := l.segments[len(l.segments)-1]

	offset, bytesSaved, err := active.Append(timestamp, key, value)
	if err != nil {
		return 0, err
	}

	if bytesSaved > 0 && l.compressionSavedObserver != nil {
		l.compressionSavedObserver(uint64(bytesSaved))
	}

	if uint64(active.Size()) >= l.maxSegmentSize {
		nextSegment, err := NewSegment(l.dir, offset+1)
		if err != nil {
			return 0, err
		}
		nextSegment.SetCompressed(l.compressed)
		l.segments = append(l.segments, nextSegment)
	}

	return offset, nil
}

// LatestOffset returns the last written message offset in the log.
func (l *Log) LatestOffset() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if len(l.segments) == 0 {
		return 0
	}

	for i := len(l.segments) - 1; i >= 0; i-- {
		segment := l.segments[i]
		if segment.nextOffset > segment.baseOffset {
			return segment.nextOffset - 1
		}
	}

	return 0
}

// Read reads a single message by offset.
func (l *Log) Read(offset uint64) (*Message, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	segment, err := l.segmentForOffset(offset)
	if err != nil {
		return nil, err
	}

	return segment.Read(offset)
}

// ReadBatch reads up to maxCount messages sequentially starting at offset.
func (l *Log) ReadBatch(offset uint64, maxCount int32) ([]*Message, error) {
	if maxCount <= 0 {
		return []*Message{}, nil
	}

	l.mu.RLock()
	defer l.mu.RUnlock()

	messages := make([]*Message, 0, maxCount)
	currentOffset := offset

	for int32(len(messages)) < maxCount {
		segment, err := l.segmentForOffset(currentOffset)
		if err != nil {
			if len(messages) > 0 {
				break
			}
			return nil, err
		}

		// Read sequentially within this segment
		for int32(len(messages)) < maxCount && currentOffset < segment.nextOffset {
			msg, err := segment.Read(currentOffset)
			if err != nil {
				if len(messages) > 0 {
					return messages, nil
				}
				return nil, err
			}
			messages = append(messages, msg)
			currentOffset++
		}

		// Stop if reached the end of available messages across all segments
		if currentOffset >= segment.nextOffset && segment == l.segments[len(l.segments)-1] {
			break
		}
	}

	return messages, nil
}

// Close flushes and closes every segment managed by the log.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	var firstErr error
	for _, segment := range l.segments {
		if err := segment.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (l *Log) segmentForOffset(offset uint64) (*Segment, error) {
	if len(l.segments) == 0 {
		return nil, errors.New("log has no segments")
	}

	if offset < l.segments[0].BaseOffset() {
		return nil, fmt.Errorf("offset %d is before the beginning of the log", offset)
	}

	index := sort.Search(len(l.segments), func(i int) bool {
		return l.segments[i].BaseOffset() > offset
	})

	index--
	return l.segments[index], nil
}

// Compact, collectLatestOffsets, compactSegment, segmentIndex, and EnforceRetention remain unchanged
func (l *Log) Compact() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if len(l.segments) <= 1 {
		return nil
	}

	latestOffsets := make(map[string]int64)
	for i := len(l.segments) - 2; i >= 0; i-- {
		segment := l.segments[i]
		if err := l.collectLatestOffsets(segment, latestOffsets); err != nil {
			return fmt.Errorf("scan segment %d for compaction: %w", segment.baseOffset, err)
		}
	}

	for i := 0; i < len(l.segments)-1; i++ {
		segment := l.segments[i]
		if err := l.compactSegment(segment, latestOffsets); err != nil {
			return fmt.Errorf("compact segment %d: %w", segment.baseOffset, err)
		}
	}

	return nil
}

func (l *Log) collectLatestOffsets(segment *Segment, latestOffsets map[string]int64) error {
	info, err := segment.indexRead.Stat()
	if err != nil {
		return err
	}

	entries := info.Size() / indexEntrySize
	for i := entries - 1; i >= 0; i-- {
		offset, _, err := segment.readIndexEntry(i * indexEntrySize)
		if err != nil {
			return err
		}

		message, err := segment.Read(offset)
		if err != nil {
			return err
		}

		key := string(message.Key)
		if _, exists := latestOffsets[key]; !exists {
			latestOffsets[key] = int64(message.Offset)
		}
	}

	return nil
}

func (l *Log) compactSegment(segment *Segment, latestOffsets map[string]int64) error {
	if segment.nextOffset <= segment.baseOffset {
		return nil
	}

	base := fmt.Sprintf("%d", segment.baseOffset)
	logPath := filepath.Join(segment.dir, base+".log")
	indexPath := filepath.Join(segment.dir, base+".index")
	originalNextOffset := segment.nextOffset
	originalCompressed := segment.compressed

	indexInfo, err := segment.indexRead.Stat()
	if err != nil {
		return err
	}
	entries := indexInfo.Size() / indexEntrySize

	if err := segment.Close(); err != nil {
		return err
	}

	source, err := os.Open(logPath)
	if err != nil {
		return err
	}

	tempLog, err := os.CreateTemp(segment.dir, base+".compact-*.tmp")
	if err != nil {
		_ = source.Close()
		return err
	}
	tempLogPath := tempLog.Name()

	tempIndex, err := os.CreateTemp(segment.dir, base+".index-*.tmp")
	if err != nil {
		_ = tempLog.Close()
		_ = source.Close()
		_ = os.Remove(tempLogPath)
		return err
	}
	tempIndexPath := tempIndex.Name()

	cleanup := func() {
		_ = tempLog.Close()
		_ = tempIndex.Close()
		_ = source.Close()
		_ = os.Remove(tempLogPath)
		_ = os.Remove(tempIndexPath)
	}

	for i := int64(0); i < entries; i++ {
		offset, position, err := segment.readIndexEntry(i * indexEntrySize)
		if err != nil {
			cleanup()
			return err
		}

		var endPosition uint64
		if i+1 < entries {
			_, nextPos, err := segment.readIndexEntry((i + 1) * indexEntrySize)
			if err != nil {
				cleanup()
				return err
			}
			endPosition = nextPos
		} else {
			stat, err := source.Stat()
			if err != nil {
				cleanup()
				return err
			}
			endPosition = uint64(stat.Size())
		}

		if endPosition < position || endPosition-position < messageHeaderSize {
			cleanup()
			return fmt.Errorf("invalid record bounds at offset %d", offset)
		}

		record := make([]byte, endPosition-position)
		if _, err := source.ReadAt(record, int64(position)); err != nil {
			cleanup()
			return err
		}

		keySize := binary.BigEndian.Uint32(record[16:20])
		valueSize := binary.BigEndian.Uint32(record[20:24])
		payload := record[messageHeaderSize:]

		if originalCompressed {
			decoded, err := snappy.Decode(nil, payload)
			if err == nil {
				payload = decoded
			}
		}

		expectedSize := uint64(keySize) + uint64(valueSize)
		if uint64(len(payload)) != expectedSize {
			cleanup()
			return fmt.Errorf("invalid payload size %d for offset %d", len(payload), offset)
		}

		key := string(payload[:keySize])
		latestOffset, exists := latestOffsets[key]
		if !exists || latestOffset != int64(offset) {
			continue
		}

		newPosition := uint64(0)
		if info, err := tempLog.Stat(); err == nil {
			newPosition = uint64(info.Size())
		} else {
			cleanup()
			return err
		}

		if err := writeAll(tempLog, record); err != nil {
			cleanup()
			return err
		}

		var newIndexEntry [indexEntrySize]byte
		binary.BigEndian.PutUint64(newIndexEntry[0:8], offset)
		binary.BigEndian.PutUint64(newIndexEntry[8:16], newPosition)

		if err := writeAll(tempIndex, newIndexEntry[:]); err != nil {
			cleanup()
			return err
		}
	}

	if err := tempLog.Sync(); err != nil {
		cleanup()
		return err
	}
	if err := tempIndex.Sync(); err != nil {
		cleanup()
		return err
	}

	_ = tempLog.Close()
	_ = tempIndex.Close()
	_ = source.Close()

	if err := os.Rename(tempLogPath, logPath); err != nil {
		_ = os.Remove(tempLogPath)
		_ = os.Remove(tempIndexPath)
		return err
	}

	if err := os.Rename(tempIndexPath, indexPath); err != nil {
		_ = os.Remove(tempIndexPath)
		return err
	}

	rewrittenSegment, err := NewSegment(segment.dir, segment.baseOffset)
	if err != nil {
		return err
	}

	rewrittenSegment.SetCompressed(originalCompressed)
	rewrittenSegment.nextOffset = originalNextOffset

	index := l.segmentIndex(segment.baseOffset)
	if index < 0 {
		_ = rewrittenSegment.Close()
		return fmt.Errorf("segment %d no longer exists in log", segment.baseOffset)
	}

	l.segments[index] = rewrittenSegment
	return nil
}

func (l *Log) segmentIndex(baseOffset uint64) int {
	index := sort.Search(len(l.segments), func(i int) bool {
		return l.segments[i].baseOffset >= baseOffset
	})

	if index < len(l.segments) && l.segments[index].baseOffset == baseOffset {
		return index
	}

	return -1
}

func (l *Log) EnforceRetention(retentionBytes int64, retentionMs int64, now time.Time) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if len(l.segments) <= 1 {
		return nil
	}

	if retentionBytes <= 0 && retentionMs <= 0 {
		return nil
	}

	totalSize := int64(0)
	for _, segment := range l.segments {
		totalSize += segment.Size()
	}

	var cutoff time.Time
	if retentionMs > 0 {
		cutoff = now.Add(-time.Duration(retentionMs) * time.Millisecond)
	}

	for len(l.segments) > 1 {
		segment := l.segments[0]
		removeForBytes := retentionBytes > 0 && totalSize > retentionBytes
		removeForTime := false

		if retentionMs > 0 && segment.Size() > 0 {
			newestTimestamp, err := segment.NewestTimestamp()
			if err != nil {
				return fmt.Errorf("read newest timestamp for segment %d: %w", segment.BaseOffset(), err)
			}
			newestTime := time.Unix(0, newestTimestamp)
			removeForTime = newestTime.Before(cutoff)
		}

		if !removeForBytes && !removeForTime {
			break
		}

		segmentSize := segment.Size()
		if err := segment.Delete(); err != nil {
			return fmt.Errorf("delete segment %d: %w", segment.BaseOffset(), err)
		}

		l.segments = l.segments[1:]
		totalSize -= segmentSize
	}

	return nil
}
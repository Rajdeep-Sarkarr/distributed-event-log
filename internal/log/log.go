package log

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

const (
	// DefaultMaxSegmentSize is the default maximum segment size of one gigabyte.
	DefaultMaxSegmentSize uint64 = 1024 * 1024 * 1024
)

// Log manages the ordered collection of commit log segments.
type Log struct {
	dir            string
	maxSegmentSize uint64
	segments       []*Segment
}

// NewLog opens an existing log directory and loads its segments in offset order.
// If maxSegmentSize is omitted, DefaultMaxSegmentSize is used.
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

		log.segments = append(log.segments, segment)
	}

	if len(log.segments) == 0 {
		segment, err := NewSegment(dir, 0)
		if err != nil {
			return nil, err
		}

		log.segments = append(log.segments, segment)
	}

	return log, nil
}

// Append writes a message to the active segment and rolls to a new segment when needed.
func (l *Log) Append(timestamp int64, key, value []byte) (uint64, error) {
	if len(l.segments) == 0 {
		return 0, errors.New("log has no segments")
	}

	active := l.segments[len(l.segments)-1]

	offset, err := active.Append(timestamp, key, value)
	if err != nil {
		return 0, err
	}

	if uint64(active.Size()) >= l.maxSegmentSize {
		nextSegment, err := NewSegment(l.dir, offset+1)
		if err != nil {
			return 0, err
		}

		l.segments = append(l.segments, nextSegment)
	}

	return offset, nil
}

// Read finds the segment containing the requested offset and reads the message.
func (l *Log) Read(offset uint64) (*Message, error) {
	segment, err := l.segmentForOffset(offset)
	if err != nil {
		return nil, err
	}

	return segment.Read(offset)
}

// Close flushes and closes every segment managed by the log.
func (l *Log) Close() error {
	var firstErr error

	for _, segment := range l.segments {
		if err := segment.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

// segmentForOffset finds the segment whose base offset is the greatest base
// offset less than or equal to the requested offset.
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
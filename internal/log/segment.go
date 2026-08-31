package log

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/golang/snappy"
)

const (
	// messageHeaderSize is the fixed-size portion of every log message.
	messageHeaderSize = 24

	// indexEntrySize is the fixed size of every index entry.
	indexEntrySize = 16
)

var headerPool = sync.Pool{
	New: func() any {
		b := make([]byte, messageHeaderSize)
		return &b
	},
}

// Message represents one record stored in the commit log.
type Message struct {
	Offset    uint64
	Timestamp int64
	Key       []byte
	Value     []byte
}

// Segment represents one .log and .index file pair.
type Segment struct {
	dir          string
	baseOffset   uint64
	nextOffset   uint64
	size         int64
	indexEntries int64 // Cached count to avoid os.Stat() syscalls on every read
	compressed   bool
	logFile      *os.File
	logRead      *os.File
	indexWrite   *os.File
	indexRead    *os.File
}

// NewSegment creates or opens a segment with the specified base offset.
func NewSegment(
	dir string,
	baseOffset uint64,
) (*Segment, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	base := strconv.FormatUint(
		baseOffset,
		10,
	)

	logPath := filepath.Join(
		dir,
		base+".log",
	)

	indexPath := filepath.Join(
		dir,
		base+".index",
	)

	logFile, err := os.OpenFile(
		logPath,
		os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		0644,
	)
	if err != nil {
		return nil, err
	}

	logRead, err := os.Open(logPath)
	if err != nil {
		_ = logFile.Close()
		return nil, err
	}

	indexWrite, err := os.OpenFile(
		indexPath,
		os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		0644,
	)
	if err != nil {
		_ = logRead.Close()
		_ = logFile.Close()
		return nil, err
	}

	indexRead, err := os.Open(indexPath)
	if err != nil {
		_ = indexWrite.Close()
		_ = logRead.Close()
		_ = logFile.Close()
		return nil, err
	}

	logInfo, err := logFile.Stat()
	if err != nil {
		_ = indexRead.Close()
		_ = indexWrite.Close()
		_ = logRead.Close()
		_ = logFile.Close()
		return nil, err
	}

	indexInfo, err := indexRead.Stat()
	if err != nil {
		_ = indexRead.Close()
		_ = indexWrite.Close()
		_ = logRead.Close()
		_ = logFile.Close()
		return nil, err
	}

	if indexInfo.Size()%indexEntrySize != 0 {
		_ = indexRead.Close()
		_ = indexWrite.Close()
		_ = logRead.Close()
		_ = logFile.Close()

		return nil, fmt.Errorf(
			"invalid index file size %d",
			indexInfo.Size(),
		)
	}

	numEntries := indexInfo.Size() / indexEntrySize

	segment := &Segment{
		dir:          dir,
		baseOffset:   baseOffset,
		nextOffset:   baseOffset,
		size:         logInfo.Size(),
		indexEntries: numEntries,
		logFile:      logFile,
		logRead:      logRead,
		indexWrite:   indexWrite,
		indexRead:    indexRead,
	}

	if numEntries > 0 {
		lastOffset, err := segment.readIndexOffset(
			(numEntries - 1) * indexEntrySize,
		)
		if err != nil {
			_ = segment.Close()
			return nil, err
		}

		segment.nextOffset = lastOffset + 1
	}

	if logInfo.Size() == 0 && indexInfo.Size() != 0 {
		_ = segment.Close()
		return nil, errors.New("index contains entries but log file is empty")
	}

	if logInfo.Size() != 0 && indexInfo.Size() == 0 {
		_ = segment.Close()
		return nil, errors.New("log file contains data but index is empty")
	}

	return segment, nil
}

// SetCompressed enables or disables Snappy compression for records written to
// and read from this segment.
func (s *Segment) SetCompressed(compressed bool) {
	s.compressed = compressed
}

// Append writes a message to the segment and returns its assigned offset.
func (s *Segment) Append(
	timestamp int64,
	key,
	value []byte,
) (uint64, int64, error) {
	if uint64(len(key)) > uint64(^uint32(0)) {
		return 0, 0, errors.New("key is too large")
	}

	if uint64(len(value)) > uint64(^uint32(0)) {
		return 0, 0, errors.New("value is too large")
	}

	payload := make([]byte, 0, len(key)+len(value))
	payload = append(payload, key...)
	payload = append(payload, value...)

	bytesSaved := int64(0)

	if s.compressed {
		originalSize := len(payload)
		payload = snappy.Encode(nil, payload)
		bytesSaved = int64(originalSize - len(payload))
	}

	offset := s.nextOffset
	position := uint64(s.size)

	var header [messageHeaderSize]byte
	binary.BigEndian.PutUint64(header[0:8], offset)
	binary.BigEndian.PutUint64(header[8:16], uint64(timestamp))
	binary.BigEndian.PutUint32(header[16:20], uint32(len(key)))
	binary.BigEndian.PutUint32(header[20:24], uint32(len(value)))

	if err := writeAll(s.logFile, header[:]); err != nil {
		return 0, 0, err
	}

	if err := writeAll(s.logFile, payload); err != nil {
		return 0, 0, err
	}

	var indexEntry [indexEntrySize]byte
	binary.BigEndian.PutUint64(indexEntry[0:8], offset)
	binary.BigEndian.PutUint64(indexEntry[8:16], position)

	if err := writeAll(s.indexWrite, indexEntry[:]); err != nil {
		return 0, 0, err
	}

	s.size += int64(messageHeaderSize + len(payload))
	s.nextOffset++
	atomic.AddInt64(&s.indexEntries, 1)

	if bytesSaved < 0 {
		bytesSaved = 0
	}

	return offset, bytesSaved, nil
}

// Read reads a message from the segment by its offset.
func (s *Segment) Read(
	offset uint64,
) (*Message, error) {
	if offset < s.baseOffset || offset >= s.nextOffset {
		return nil, fmt.Errorf("offset %d is outside segment", offset)
	}

	position, nextPosition, err := s.findMessageBounds(offset)
	if err != nil {
		return nil, err
	}

	headerBuf := headerPool.Get().(*[]byte)
	defer headerPool.Put(headerBuf)
	header := *headerBuf

	if _, err := s.logRead.ReadAt(header, int64(position)); err != nil {
		return nil, err
	}

	messageOffset := binary.BigEndian.Uint64(header[0:8])
	if messageOffset != offset {
		return nil, fmt.Errorf(
			"index offset %d points to message offset %d",
			offset,
			messageOffset,
		)
	}

	timestamp := int64(binary.BigEndian.Uint64(header[8:16]))
	keySize := binary.BigEndian.Uint32(header[16:20])
	valueSize := binary.BigEndian.Uint32(header[20:24])

	payloadSize := nextPosition - position - messageHeaderSize
	payload := make([]byte, payloadSize)

	if _, err := s.logRead.ReadAt(payload, int64(position)+messageHeaderSize); err != nil {
		return nil, err
	}

	expectedSize := uint64(keySize) + uint64(valueSize)

	if s.compressed {
		decoded, err := snappy.Decode(nil, payload)
		if err != nil {
			return nil, fmt.Errorf(
				"decode compressed payload at offset %d: %w",
				offset,
				err,
			)
		}
		payload = decoded
	}

	if uint64(len(payload)) != expectedSize {
		return nil, fmt.Errorf(
			"payload size %d does not match key/value sizes %d",
			len(payload),
			expectedSize,
		)
	}

	key := make([]byte, keySize)
	copy(key, payload[:keySize])

	value := make([]byte, valueSize)
	copy(value, payload[keySize:])

	return &Message{
		Offset:    messageOffset,
		Timestamp: timestamp,
		Key:       key,
		Value:     value,
	}, nil
}

// findMessageBounds returns the start position and next message start position (or segment end).
// Performs O(1) direct indexing for contiguous offsets with automatic fallback to binary search.
func (s *Segment) findMessageBounds(offset uint64) (uint64, uint64, error) {
	entries := atomic.LoadInt64(&s.indexEntries)
	if entries == 0 {
		return 0, 0, errors.New("segment index is empty")
	}

	// O(1) fast path: Check if entry offset matches direct index arithmetic
	directIdx := int64(offset - s.baseOffset)
	if directIdx >= 0 && directIdx < entries {
		entryOffset, position, err := s.readIndexEntry(directIdx * indexEntrySize)
		if err == nil && entryOffset == offset {
			var nextPos uint64
			if directIdx+1 < entries {
				_, nextPos, err = s.readIndexEntry((directIdx + 1) * indexEntrySize)
				if err != nil {
					return 0, 0, err
				}
			} else {
				nextPos = uint64(s.size)
			}
			return position, nextPos, nil
		}
	}

	// O(log N) binary search fallback
	low := int64(0)
	high := entries - 1

	for low <= high {
		mid := low + (high-low)/2
		entryOffset, position, err := s.readIndexEntry(mid * indexEntrySize)
		if err != nil {
			return 0, 0, err
		}

		if entryOffset == offset {
			var nextPos uint64
			if mid+1 < entries {
				_, nextPos, err = s.readIndexEntry((mid + 1) * indexEntrySize)
				if err != nil {
					return 0, 0, err
				}
			} else {
				nextPos = uint64(s.size)
			}
			return position, nextPos, nil
		}

		if entryOffset < offset {
			low = mid + 1
		} else {
			high = mid - 1
		}
	}

	return 0, 0, fmt.Errorf("offset %d not found in segment index", offset)
}

// findPosition looks up an offset in the index and returns its log position.
func (s *Segment) findPosition(offset uint64) (uint64, error) {
	pos, _, err := s.findMessageBounds(offset)
	return pos, err
}

// findEndPosition returns the byte immediately after the record beginning at position.
func (s *Segment) findEndPosition(position uint64) (uint64, error) {
	entries := atomic.LoadInt64(&s.indexEntries)
	low := int64(0)
	high := entries - 1

	for low <= high {
		mid := low + (high-low)/2
		_, entryPosition, err := s.readIndexEntry(mid * indexEntrySize)
		if err != nil {
			return 0, err
		}

		if entryPosition == position {
			if mid+1 < entries {
				_, nextPosition, err := s.readIndexEntry((mid + 1) * indexEntrySize)
				if err != nil {
					return 0, err
				}
				return nextPosition, nil
			}
			return uint64(s.size), nil
		}

		if entryPosition < position {
			low = mid + 1
		} else {
			high = mid - 1
		}
	}

	return 0, fmt.Errorf("log position %d not found in segment index", position)
}

// NewestTimestamp returns the timestamp of the newest message in the segment.
func (s *Segment) NewestTimestamp() (int64, error) {
	if s.nextOffset <= s.baseOffset {
		return 0, errors.New("segment is empty")
	}

	entries := atomic.LoadInt64(&s.indexEntries)
	if entries == 0 {
		return 0, errors.New("segment index is empty")
	}

	position, err := s.readIndexPosition((entries - 1) * indexEntrySize)
	if err != nil {
		return 0, err
	}

	var header [messageHeaderSize]byte
	if _, err := s.logRead.ReadAt(header[:], int64(position)); err != nil {
		return 0, err
	}

	return int64(binary.BigEndian.Uint64(header[8:16])), nil
}

// Size reports the number of bytes currently stored in the segment's .log file.
func (s *Segment) Size() int64 {
	return s.size
}

// BaseOffset reports the offset of the first message assigned to the segment.
func (s *Segment) BaseOffset() uint64 {
	return s.baseOffset
}

// Close flushes and closes all files belonging to the segment.
func (s *Segment) Close() error {
	var errs []error

	if s.logFile != nil {
		errs = append(errs, s.logFile.Sync(), s.logFile.Close())
		s.logFile = nil
	}

	if s.indexWrite != nil {
		errs = append(errs, s.indexWrite.Sync(), s.indexWrite.Close())
		s.indexWrite = nil
	}

	if s.logRead != nil {
		errs = append(errs, s.logRead.Close())
		s.logRead = nil
	}

	if s.indexRead != nil {
		errs = append(errs, s.indexRead.Close())
		s.indexRead = nil
	}

	for _, err := range errs {
		if err != nil {
			return err
		}
	}

	return nil
}

// Delete closes the segment and removes both the .log and .index files.
func (s *Segment) Delete() error {
	if err := s.Close(); err != nil {
		return err
	}

	base := strconv.FormatUint(s.baseOffset, 10)
	logPath := filepath.Join(s.dir, base+".log")
	indexPath := filepath.Join(s.dir, base+".index")

	if err := os.Remove(logPath); err != nil && !os.IsNotExist(err) {
		return err
	}

	if err := os.Remove(indexPath); err != nil && !os.IsNotExist(err) {
		return err
	}

	return nil
}

// readIndexEntry reads one fixed-size index entry at the specified byte position using a stack buffer.
func (s *Segment) readIndexEntry(position int64) (uint64, uint64, error) {
	var entry [indexEntrySize]byte
	if _, err := s.indexRead.ReadAt(entry[:], position); err != nil {
		return 0, 0, err
	}

	return binary.BigEndian.Uint64(entry[0:8]), binary.BigEndian.Uint64(entry[8:16]), nil
}

// readIndexPosition reads the log position from one index entry.
func (s *Segment) readIndexPosition(position int64) (uint64, error) {
	_, logPosition, err := s.readIndexEntry(position)
	return logPosition, err
}

// readIndexOffset reads the offset from one index entry at the specified position.
func (s *Segment) readIndexOffset(position int64) (uint64, error) {
	offset, _, err := s.readIndexEntry(position)
	return offset, err
}

// writeAll writes the complete byte slice to the supplied file.
func writeAll(file *os.File, data []byte) error {
	for len(data) > 0 {
		n, err := file.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}

// segmentBaseOffset extracts a segment base offset from a .log filename.
func segmentBaseOffset(name string) (uint64, bool) {
	if !strings.HasSuffix(name, ".log") {
		return 0, false
	}

	base := strings.TrimSuffix(name, ".log")
	offset, err := strconv.ParseUint(base, 10, 64)
	if err != nil {
		return 0, false
	}

	return offset, true
}

// AppendBatch writes a slice of records to the segment in a single contiguous write.
func (s *Segment) AppendBatch(
	records []Record,
) ([]uint64, int64, error) {
	if len(records) == 0 {
		return nil, 0, nil
	}

	offsets := make([]uint64, len(records))
	totalSaved := int64(0)

	// Pre-calculate total sizes to allocate contiguous buffers once
	type preparedPayload struct {
		payload    []byte
		keyLen     int
		valLen     int
		timestamp  int64
		bytesSaved int64
	}

	prepared := make([]preparedPayload, len(records))
	totalLogBytes := 0
	totalIndexBytes := len(records) * indexEntrySize

	for i, r := range records {
		if uint64(len(r.Key)) > uint64(^uint32(0)) {
			return nil, 0, errors.New("key is too large")
		}
		if uint64(len(r.Value)) > uint64(^uint32(0)) {
			return nil, 0, errors.New("value is too large")
		}

		p := make([]byte, 0, len(r.Key)+len(r.Value))
		p = append(p, r.Key...)
		p = append(p, r.Value...)

		saved := int64(0)
		if s.compressed {
			orig := len(p)
			p = snappy.Encode(nil, p)
			saved = int64(orig - len(p))
			if saved < 0 {
				saved = 0
			}
		}

		prepared[i] = preparedPayload{
			payload:    p,
			keyLen:     len(r.Key),
			valLen:     len(r.Value),
			timestamp:  r.Timestamp,
			bytesSaved: saved,
		}
		totalSaved += saved
		totalLogBytes += messageHeaderSize + len(p)
	}

	logBuf := make([]byte, totalLogBytes)
	indexBuf := make([]byte, totalIndexBytes)

	logOffset := 0
	indexOffset := 0
	currentPos := uint64(s.size)
	currOffset := s.nextOffset

	for i, p := range prepared {
		offsets[i] = currOffset

		// 1. Write Header
		binary.BigEndian.PutUint64(logBuf[logOffset:logOffset+8], currOffset)
		binary.BigEndian.PutUint64(logBuf[logOffset+8:logOffset+16], uint64(p.timestamp))
		binary.BigEndian.PutUint32(logBuf[logOffset+16:logOffset+20], uint32(p.keyLen))
		binary.BigEndian.PutUint32(logBuf[logOffset+20:logOffset+24], uint32(p.valLen))
		logOffset += messageHeaderSize

		// 2. Write Payload
		copy(logBuf[logOffset:logOffset+len(p.payload)], p.payload)
		logOffset += len(p.payload)

		// 3. Write Index Entry
		binary.BigEndian.PutUint64(indexBuf[indexOffset:indexOffset+8], currOffset)
		binary.BigEndian.PutUint64(indexBuf[indexOffset+8:indexOffset+16], currentPos)
		indexOffset += indexEntrySize

		currentPos += uint64(messageHeaderSize + len(p.payload))
		currOffset++
	}

	// Two atomic OS writes for the entire batch
	if err := writeAll(s.logFile, logBuf); err != nil {
		return nil, 0, err
	}
	if err := writeAll(s.indexWrite, indexBuf); err != nil {
		return nil, 0, err
	}

	s.size += int64(totalLogBytes)
	s.nextOffset = currOffset
	atomic.AddInt64(&s.indexEntries, int64(len(records)))

	return offsets, totalSaved, nil
}
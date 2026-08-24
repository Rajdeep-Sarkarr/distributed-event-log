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
)

const (
	// messageHeaderSize is the fixed-size portion of every log message.
	messageHeaderSize = 24

	// indexEntrySize is the fixed size of every index entry.
	indexEntrySize = 16
)

// Message represents one record stored in the commit log.
type Message struct {
	Offset    uint64
	Timestamp int64
	Key       []byte
	Value     []byte
}

// Segment represents one .log and .index file pair.
type Segment struct {
	dir        string
	baseOffset uint64
	nextOffset uint64
	size       int64
	logFile    *os.File
	logRead    *os.File
	indexWrite *os.File
	indexRead  *os.File
}

// NewSegment creates or opens a segment with the specified base offset.
func NewSegment(dir string, baseOffset uint64) (*Segment, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	base := strconv.FormatUint(baseOffset, 10)
	logPath := filepath.Join(dir, base+".log")
	indexPath := filepath.Join(dir, base+".index")

	logFile, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	logRead, err := os.Open(logPath)
	if err != nil {
		_ = logFile.Close()
		return nil, err
	}

	indexWrite, err := os.OpenFile(indexPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
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
		return nil, fmt.Errorf("invalid index file size %d", indexInfo.Size())
	}

	segment := &Segment{
		dir:        dir,
		baseOffset: baseOffset,
		nextOffset: baseOffset,
		size:       logInfo.Size(),
		logFile:    logFile,
		logRead:    logRead,
		indexWrite: indexWrite,
		indexRead:  indexRead,
	}

	if indexInfo.Size() > 0 {
		lastOffset, err := segment.readIndexOffset(indexInfo.Size() - indexEntrySize)
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

// Append writes a message to the segment and returns its assigned offset.
func (s *Segment) Append(timestamp int64, key, value []byte) (uint64, error) {
	if uint64(len(key)) > uint64(^uint32(0)) {
		return 0, errors.New("key is too large")
	}

	if uint64(len(value)) > uint64(^uint32(0)) {
		return 0, errors.New("value is too large")
	}

	offset := s.nextOffset
	position := uint64(s.size)

	header := make([]byte, messageHeaderSize)
	binary.BigEndian.PutUint64(header[0:8], offset)
	binary.BigEndian.PutUint64(header[8:16], uint64(timestamp))
	binary.BigEndian.PutUint32(header[16:20], uint32(len(key)))
	binary.BigEndian.PutUint32(header[20:24], uint32(len(value)))

	if err := writeAll(s.logFile, header); err != nil {
		return 0, err
	}

	if err := writeAll(s.logFile, key); err != nil {
		return 0, err
	}

	if err := writeAll(s.logFile, value); err != nil {
		return 0, err
	}

	indexEntry := make([]byte, indexEntrySize)
	binary.BigEndian.PutUint64(indexEntry[0:8], offset)
	binary.BigEndian.PutUint64(indexEntry[8:16], position)

	if err := writeAll(s.indexWrite, indexEntry); err != nil {
		return 0, err
	}

	s.size += int64(messageHeaderSize + len(key) + len(value))
	s.nextOffset++

	return offset, nil
}

// Read reads a message from the segment by its offset.
func (s *Segment) Read(offset uint64) (*Message, error) {
	if offset < s.baseOffset || offset >= s.nextOffset {
		return nil, fmt.Errorf("offset %d is outside segment", offset)
	}

	position, err := s.findPosition(offset)
	if err != nil {
		return nil, err
	}

	header := make([]byte, messageHeaderSize)
	if _, err := s.logRead.ReadAt(header, int64(position)); err != nil {
		return nil, err
	}

	messageOffset := binary.BigEndian.Uint64(header[0:8])
	if messageOffset != offset {
		return nil, fmt.Errorf("index offset %d points to message offset %d", offset, messageOffset)
	}

	timestamp := int64(binary.BigEndian.Uint64(header[8:16]))
	keySize := binary.BigEndian.Uint32(header[16:20])
	valueSize := binary.BigEndian.Uint32(header[20:24])

	payloadSize := uint64(keySize) + uint64(valueSize)
	payload := make([]byte, payloadSize)

	if _, err := s.logRead.ReadAt(payload, int64(position)+messageHeaderSize); err != nil {
		return nil, err
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

    errs = append(errs, s.logFile.Sync())
    errs = append(errs, s.indexWrite.Sync())
    errs = append(errs, s.logFile.Close())
    errs = append(errs, s.logRead.Close())
    errs = append(errs, s.indexWrite.Close())
    errs = append(errs, s.indexRead.Close())

    for _, err := range errs {
        if err != nil {
            return err
        }
    }

    return nil
}

// findPosition looks up an offset in the index and returns its log position.
func (s *Segment) findPosition(offset uint64) (uint64, error) {
	info, err := s.indexRead.Stat()
	if err != nil {
		return 0, err
	}

	entries := info.Size() / indexEntrySize
	low := int64(0)
	high := entries - 1

	for low <= high {
		mid := low + (high-low)/2
		entryOffset, position, err := s.readIndexEntry(mid * indexEntrySize)
		if err != nil {
			return 0, err
		}

		if entryOffset == offset {
			return position, nil
		}

		if entryOffset < offset {
			low = mid + 1
		} else {
			high = mid - 1
		}
	}

	return 0, fmt.Errorf("offset %d not found in segment", offset)
}

// readIndexEntry reads one fixed-size index entry at the specified byte position.
func (s *Segment) readIndexEntry(position int64) (uint64, uint64, error) {
	entry := make([]byte, indexEntrySize)

	if _, err := s.indexRead.ReadAt(entry, position); err != nil {
		return 0, 0, err
	}

	return binary.BigEndian.Uint64(entry[0:8]),
		binary.BigEndian.Uint64(entry[8:16]),
		nil
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
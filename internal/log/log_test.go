package log

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestLogAppendAndRead verifies that appended messages can be read back by offset.
func TestLogAppendAndRead(t *testing.T) {
	dir := t.TempDir()

	log, err := NewLog(dir, 1024)
	if err != nil {
		t.Fatalf("create log: %v", err)
	}
	defer func() {
		if err := log.Close(); err != nil {
			t.Fatalf("close log: %v", err)
		}
	}()

	messages := []struct {
		key   []byte
		value []byte
	}{
		{key: []byte("key-1"), value: []byte("value-1")},
		{key: []byte("key-2"), value: []byte("value-2")},
		{key: []byte("key-3"), value: []byte("value-3")},
	}

	offsets := make([]uint64, len(messages))

	for i, message := range messages {
		offset, err := log.Append(time.Now().UnixNano(), message.key, message.value)
		if err != nil {
			t.Fatalf("append message %d: %v", i, err)
		}

		offsets[i] = offset
	}

	for i, expected := range messages {
		message, err := log.Read(offsets[i])
		if err != nil {
			t.Fatalf("read message %d: %v", i, err)
		}

		if !bytes.Equal(message.Key, expected.key) {
			t.Fatalf("message %d key = %q, want %q", i, message.Key, expected.key)
		}

		if !bytes.Equal(message.Value, expected.value) {
			t.Fatalf("message %d value = %q, want %q", i, message.Value, expected.value)
		}
	}
}

// TestLogSegmentRollover verifies that an oversized active segment creates a new segment.
func TestLogSegmentRollover(t *testing.T) {
	dir := t.TempDir()

	log, err := NewLog(dir, 1024)
	if err != nil {
		t.Fatalf("create log: %v", err)
	}
	defer func() {
		if err := log.Close(); err != nil {
			t.Fatalf("close log: %v", err)
		}
	}()

	value := make([]byte, 1024)

	_, err = log.Append(time.Now().UnixNano(), []byte("key"), value)
	if err != nil {
		t.Fatalf("append message: %v", err)
	}

	if len(log.segments) != 2 {
		t.Fatalf("segment count = %d, want 2", len(log.segments))
	}

	if _, err := os.Stat(filepath.Join(dir, "0.log")); err != nil {
		t.Fatalf("first segment log file was not created: %v", err)
	}

	if _, err := os.Stat(filepath.Join(dir, "0.index")); err != nil {
		t.Fatalf("first segment index file was not created: %v", err)
	}

	if _, err := os.Stat(filepath.Join(dir, "1.log")); err != nil {
		t.Fatalf("new segment log file was not created: %v", err)
	}

	if _, err := os.Stat(filepath.Join(dir, "1.index")); err != nil {
		t.Fatalf("new segment index file was not created: %v", err)
	}
}

// TestLogReload verifies that existing segments are loaded in base-offset order.
func TestLogReload(t *testing.T) {
	dir := t.TempDir()

	log, err := NewLog(dir, 1024)
	if err != nil {
		t.Fatalf("create log: %v", err)
	}

	offset, err := log.Append(time.Now().UnixNano(), []byte("key"), []byte("value"))
	if err != nil {
		t.Fatalf("append message: %v", err)
	}

	if err := log.Close(); err != nil {
		t.Fatalf("close log: %v", err)
	}

	reloaded, err := NewLog(dir, 1024)
	if err != nil {
		t.Fatalf("reload log: %v", err)
	}
	defer func() {
		if err := reloaded.Close(); err != nil {
			t.Fatalf("close reloaded log: %v", err)
		}
	}()

	message, err := reloaded.Read(offset)
	if err != nil {
		t.Fatalf("read reloaded message: %v", err)
	}

	if !bytes.Equal(message.Key, []byte("key")) {
		t.Fatalf("reloaded key = %q, want %q", message.Key, "key")
	}

	if !bytes.Equal(message.Value, []byte("value")) {
		t.Fatalf("reloaded value = %q, want %q", message.Value, "value")
	}
}
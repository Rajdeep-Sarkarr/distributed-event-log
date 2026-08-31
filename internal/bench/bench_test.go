package bench

import (
	"context"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"testing"
	"time"

	"distributed-event-log/internal/broker"
	pb "distributed-event-log/internal/proto"
)

func init() {
	// Silence standard library log output (bbolt / raft-boltdb tx warnings)
	log.SetOutput(io.Discard)
}

func getFreePort(b *testing.B) int {
	b.Helper()
	for attempts := 0; attempts < 10; attempts++ {
		port := 10000 + rand.Intn(39000)
		l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err != nil {
			continue
		}
		_ = l.Close()
		return port
	}
	b.Fatal("could not find a free port after 10 attempts")
	return 0
}

func setupTestBroker(b *testing.B) (*broker.Broker, string) {
	b.Helper()

	tempDir, err := os.MkdirTemp("", "bench-broker-*")
	if err != nil {
		b.Fatalf("failed to create temp dir: %v", err)
	}

	b.Cleanup(func() {
		_ = os.RemoveAll(tempDir)
	})

	httpPort := getFreePort(b)
	raftPort := getFreePort(b)

	httpAddr := fmt.Sprintf("127.0.0.1:%d", httpPort)
	raftAddr := fmt.Sprintf("127.0.0.1:%d", raftPort)

	origStderr := os.Stderr
	devNull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0666)
	if err == nil {
		os.Stderr = devNull
		b.Cleanup(func() {
			os.Stderr = origStderr
			_ = devNull.Close()
		})
	}

	bkr, err := broker.NewBroker(
		"bench-node-1",
		httpAddr,
		raftAddr,
		tempDir,
		"",
		"",
		"",
		"",
	)
	if err != nil {
		b.Fatalf("failed to initialize broker: %v", err)
	}

	b.Cleanup(func() {
		_ = bkr.Close()
	})

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if bkr.IsLeader() {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}

	if !bkr.IsLeader() {
		b.Fatal("broker did not become leader within 5s")
	}

	return bkr, tempDir
}

func BenchmarkProduceSingle(b *testing.B) {
	bkr, _ := setupTestBroker(b)
	ctx := context.Background()

	p := int32(0)
	req := &pb.ProduceRequest{
		Topic:     "bench-produce-single",
		Key:       []byte("bench-key"),
		Value:     []byte("bench-value-payload-data-bytes"),
		Partition: &p,
	}

	b.SetBytes(int64(len(req.Key) + len(req.Value)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := bkr.Produce(ctx, req); err != nil {
			b.Fatalf("Produce failed on iteration %d: %v", i, err)
		}
	}
}

func BenchmarkProduceBatch100(b *testing.B) {
	bkr, _ := setupTestBroker(b)
	ctx := context.Background()

	records := make([]*pb.Record, 100)
	for i := 0; i < 100; i++ {
		records[i] = &pb.Record{
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte("bench-batch-payload-100-bytes-of-data"),
		}
	}

	req := &pb.ProduceBatchRequest{
		Topic:     "bench-produce-batch",
		Partition: 0,
		Records:   records,
	}

	var totalBytes int64
	for _, r := range records {
		totalBytes += int64(len(r.Key) + len(r.Value))
	}
	b.SetBytes(totalBytes)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := bkr.ProduceBatch(ctx, req)
		if err != nil {
			b.Fatalf("ProduceBatch failed: %v", err)
		}
		if resp.Error != "" {
			b.Fatalf("ProduceBatch response error: %s", resp.Error)
		}
	}
}

func BenchmarkProduceBatch100Parallel(b *testing.B) {
	bkr, _ := setupTestBroker(b)
	ctx := context.Background()

	records := make([]*pb.Record, 100)
	for i := 0; i < 100; i++ {
		records[i] = &pb.Record{
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte("bench-batch-payload-100-bytes-of-data"),
		}
	}

	var totalBytes int64
	for _, r := range records {
		totalBytes += int64(len(r.Key) + len(r.Value))
	}
	b.SetBytes(totalBytes)
	b.ResetTimer()

	b.RunParallel(func(pbIter *testing.PB) {
		req := &pb.ProduceBatchRequest{
			Topic:     "bench-produce-batch-parallel",
			Partition: 0,
			Records:   records,
		}
		for pbIter.Next() {
			resp, err := bkr.ProduceBatch(ctx, req)
			if err != nil {
				b.Errorf("ProduceBatch (parallel) failed: %v", err)
				return
			}
			if resp.Error != "" {
				b.Errorf("ProduceBatch (parallel) response error: %s", resp.Error)
				return
			}
		}
	})
}

func BenchmarkConsumeSingle(b *testing.B) {
	bkr, _ := setupTestBroker(b)
	ctx := context.Background()
	topic := "bench-consume-single"

	batchSize := 100
	batches := (b.N + batchSize - 1) / batchSize
	if batches == 0 {
		batches = 1
	}

	records := make([]*pb.Record, batchSize)
	for j := 0; j < batchSize; j++ {
		records[j] = &pb.Record{
			Key:   []byte("seed-key"),
			Value: []byte("seed-value-payload"),
		}
	}

	var firstOffset uint64
	for i := 0; i < batches; i++ {
		batchReq := &pb.ProduceBatchRequest{
			Topic:     topic,
			Partition: 0,
			Records:   records,
		}
		resp, err := bkr.ProduceBatch(ctx, batchReq)
		if err != nil || resp.Error != "" {
			b.Fatalf("failed to seed log batch %d: %v, %s", i, err, resp.Error)
		}
		if i == 0 && len(resp.Offsets) > 0 {
			firstOffset = uint64(resp.Offsets[0])
		}
	}

	partition := int32(0)
	consumeReq := &pb.ConsumeRequest{
		Topic:     topic,
		Partition: &partition,
	}

	b.SetBytes(int64(len(records[0].Key) + len(records[0].Value)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		consumeReq.Offset = firstOffset + uint64(i)
		if _, err := bkr.Consume(ctx, consumeReq); err != nil {
			b.Fatalf("Consume failed at offset %d: %v", consumeReq.Offset, err)
		}
	}
}

func BenchmarkConsumeSequential1000(b *testing.B) {
	bkr, _ := setupTestBroker(b)
	ctx := context.Background()
	topic := "bench-consume-sequential"

	records := make([]*pb.Record, 100)
	for i := 0; i < 100; i++ {
		records[i] = &pb.Record{
			Key:   []byte(fmt.Sprintf("seed-key-%d", i)),
			Value: []byte("seed-batch-payload-data-value"),
		}
	}

	var seededFirstOffset uint64
	for i := 0; i < 10; i++ {
		batchReq := &pb.ProduceBatchRequest{
			Topic:     topic,
			Partition: 0,
			Records:   records,
		}
		resp, err := bkr.ProduceBatch(ctx, batchReq)
		if err != nil || resp.Error != "" {
			b.Fatalf("failed to seed 1000 messages: %v, %s", err, resp.Error)
		}
		if i == 0 && len(resp.Offsets) > 0 {
			seededFirstOffset = uint64(resp.Offsets[0])
		}
	}

	var totalBytes int64
	for _, r := range records {
		totalBytes += int64(len(r.Key) + len(r.Value))
	}
	b.SetBytes(totalBytes * 10)
	b.ResetTimer()

	consumeBatchReq := &pb.ConsumeBatchRequest{
		Topic:     topic,
		Partition: 0,
		Offset:    int64(seededFirstOffset),
		MaxCount:  1000,
	}

	for i := 0; i < b.N; i++ {
		resp, err := bkr.ConsumeBatch(ctx, consumeBatchReq)
		if err != nil {
			b.Fatalf("ConsumeBatch failed: %v", err)
		}
		if len(resp.Records) == 0 {
			b.Fatalf("expected records from ConsumeBatch, got 0")
		}
	}
}
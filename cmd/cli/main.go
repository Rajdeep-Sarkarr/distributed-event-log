package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "distributed-event-log/internal/proto"
)

// main parses and executes the requested CLI subcommand.
func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: elog produce|consume")
		os.Exit(1)
	}

	var err error

	switch os.Args[1] {
	case "produce":
		err = produce(os.Args[2:])
	case "consume":
		err = consume(os.Args[2:])
	default:
		err = fmt.Errorf("unknown command: %s", os.Args[1])
	}

	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// produce parses produce flags, connects to the broker, and publishes a message.
func produce(args []string) error {
	flags := flag.NewFlagSet("produce", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)

	addr := flags.String("addr", "localhost:9080", "broker gRPC address")
	topic := flags.String("topic", "default", "topic")
	msg := flags.String("msg", "", "message")

	if err := flags.Parse(args); err != nil {
		return err
	}

	if *msg == "" {
		return fmt.Errorf("--msg is required")
	}

	conn, err := grpc.Dial(
		*addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return err
	}
	defer conn.Close()

	client := pb.NewBrokerServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	response, err := client.Produce(ctx, &pb.ProduceRequest{
		Topic: *topic,
		Key:   []byte("msg"),
		Value: []byte(*msg),
	})
	if err != nil {
		return err
	}

	fmt.Println(response.Offset)

	return nil
}

// consume parses consume flags, connects to the broker, and reads a message.
func consume(args []string) error {
	flags := flag.NewFlagSet("consume", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)

	addr := flags.String("addr", "localhost:9080", "broker gRPC address")
	offset := flags.Uint64("offset", 0, "message offset")

	if err := flags.Parse(args); err != nil {
		return err
	}

	conn, err := grpc.Dial(
		*addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return err
	}
	defer conn.Close()

	client := pb.NewBrokerServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	response, err := client.Consume(ctx, &pb.ConsumeRequest{
		Offset: *offset,
	})
	if err != nil {
		return err
	}

	fmt.Printf("key: %s\n", response.Key)
	fmt.Printf("value: %s\n", response.Value)

	return nil
}
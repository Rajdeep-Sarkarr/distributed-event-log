package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	pb "distributed-event-log/internal/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// main parses and executes the requested CLI subcommand.
func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(
			os.Stderr,
			"usage: elog produce|consume|consume-group",
		)
		os.Exit(1)
	}

	var err error

	switch os.Args[1] {
	case "produce":
		err = produce(os.Args[2:])
	case "consume":
		err = consume(os.Args[2:])
	case "consume-group":
		err = consumeGroup(os.Args[2:])
	default:
		err = fmt.Errorf("unknown command: %s", os.Args[1])
	}

	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// produce parses produce flags and publishes a message.
func produce(args []string) error {
	flags := flag.NewFlagSet("produce", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)

	addr := flags.String(
		"addr",
		"localhost:9080",
		"broker gRPC address",
	)
	topic := flags.String(
		"topic",
		"default",
		"topic",
	)
	msg := flags.String(
		"msg",
		"",
		"message",
	)
	partition := flags.Int(
		"partition",
		-1,
		"partition, or -1 for automatic selection",
	)

	if err := flags.Parse(args); err != nil {
		return err
	}

	if *msg == "" {
		return fmt.Errorf("--msg is required")
	}

	conn, err := grpc.Dial(
		*addr,
		grpc.WithTransportCredentials(
			insecure.NewCredentials(),
		),
	)
	if err != nil {
		return err
	}
	defer conn.Close()

	client := pb.NewBrokerServiceClient(conn)

	var selectedPartition *int32

	if *partition >= 0 {
		value := int32(*partition)
		selectedPartition = &value
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	response, err := client.Produce(
		ctx,
		&pb.ProduceRequest{
			Topic:     *topic,
			Key:       []byte("msg"),
			Value:     []byte(*msg),
			Partition: selectedPartition,
		},
	)
	if err != nil {
		return err
	}

	fmt.Println(response.Offset)

	return nil
}

// consume parses consume flags and reads a message.
func consume(args []string) error {
	flags := flag.NewFlagSet("consume", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)

	addr := flags.String(
		"addr",
		"localhost:9080",
		"broker gRPC address",
	)
	topic := flags.String(
		"topic",
		"default",
		"topic",
	)
	offset := flags.Uint64(
		"offset",
		0,
		"message offset",
	)
	partition := flags.Int(
		"partition",
		0,
		"partition",
	)

	if err := flags.Parse(args); err != nil {
		return err
	}

	conn, err := grpc.Dial(
		*addr,
		grpc.WithTransportCredentials(
			insecure.NewCredentials(),
		),
	)
	if err != nil {
		return err
	}
	defer conn.Close()

	client := pb.NewBrokerServiceClient(conn)

	selectedPartition := int32(*partition)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	response, err := client.Consume(
		ctx,
		&pb.ConsumeRequest{
			Offset:    *offset,
			Topic:     *topic,
			Partition: &selectedPartition,
		},
	)
	if err != nil {
		return err
	}

	fmt.Printf("key: %s\n", response.Key)
	fmt.Printf("value: %s\n", response.Value)

	return nil
}

// consumeGroup consumes the next message for a consumer group,
// commits the resulting offset, and optionally follows continuously.
func consumeGroup(args []string) error {
	flags := flag.NewFlagSet(
		"consume-group",
		flag.ContinueOnError,
	)
	flags.SetOutput(os.Stderr)

	addr := flags.String(
		"addr",
		"localhost:9080",
		"broker gRPC address",
	)
	group := flags.String(
		"group",
		"",
		"consumer group ID",
	)
	topic := flags.String(
		"topic",
		"default",
		"topic",
	)
	partition := flags.Int(
		"partition",
		0,
		"partition",
	)
	follow := flags.Bool(
		"follow",
		false,
		"continue polling for new messages",
	)

	if err := flags.Parse(args); err != nil {
		return err
	}

	if *group == "" {
		return fmt.Errorf("--group is required")
	}

	if *partition < 0 || *partition >= 3 {
		return fmt.Errorf("partition must be between 0 and 2")
	}

	conn, err := grpc.Dial(
		*addr,
		grpc.WithTransportCredentials(
			insecure.NewCredentials(),
		),
	)
	if err != nil {
		return err
	}
	defer conn.Close()

	client := pb.NewBrokerServiceClient(conn)

	for {
		ctx, cancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)

		offsetResponse, err := client.FetchOffset(
			ctx,
			&pb.FetchOffsetRequest{
				GroupId:   *group,
				Topic:     *topic,
				Partition: int32(*partition),
			},
		)

		cancel()

		if err != nil {
			return err
		}

		nextOffset := offsetResponse.Offset

		ctx, cancel = context.WithTimeout(
			context.Background(),
			5*time.Second,
		)

		message, err := client.Consume(
			ctx,
			&pb.ConsumeRequest{
				Offset: nextOffset,
				Topic:  *topic,
				Partition: func() *int32 {
					value := int32(*partition)
					return &value
				}(),
			},
		)

		cancel()

		if err != nil {
			if *follow {
				time.Sleep(1 * time.Second)
				continue
			}

			return err
		}

		fmt.Printf("key: %s\n", message.Key)
		fmt.Printf("value: %s\n", message.Value)

		newOffset := message.Offset + 1

		ctx, cancel = context.WithTimeout(
			context.Background(),
			5*time.Second,
		)

		_, err = client.CommitOffset(
			ctx,
			&pb.CommitOffsetRequest{
				GroupId:   *group,
				Topic:     *topic,
				Partition: int32(*partition),
				Offset:    newOffset,
			},
		)

		cancel()

		if err != nil {
			return err
		}

		if !*follow {
			return nil
		}

		time.Sleep(1 * time.Second)
	}
}
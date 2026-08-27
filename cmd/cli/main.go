package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"distributed-event-log/internal/proto"
	deltatls "distributed-event-log/internal/tls"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// main parses the CLI subcommand and executes it.
func main() {
	if len(os.Args) < 2 {
		printUsage()
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
		printUsage()
		os.Exit(1)
	}

	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// produce publishes one message through the gRPC API.
func produce(args []string) error {
	flags := flag.NewFlagSet(
		"produce",
		flag.ContinueOnError,
	)

	addr := flags.String(
		"addr",
		"localhost:9080",
		"broker gRPC address",
	)

	topic := flags.String(
		"topic",
		"default",
		"topic name",
	)

	partition := flags.Int(
		"partition",
		-1,
		"partition, or -1 for automatic selection",
	)

	msg := flags.String(
		"msg",
		"",
		"message value",
	)

	certFile := flags.String(
		"cert",
		"",
		"client TLS certificate",
	)

	keyFile := flags.String(
		"key",
		"",
		"client TLS private key",
	)

	caFile := flags.String(
		"ca",
		"",
		"CA certificate",
	)

	if err := flags.Parse(args); err != nil {
		return err
	}

	if *msg == "" {
		return fmt.Errorf("--msg is required")
	}

	var partitionPtr *int32

	if *partition >= 0 {
		if *partition >= 3 {
			return fmt.Errorf(
				"partition must be between 0 and 2",
			)
		}

		p := int32(*partition)
		partitionPtr = &p
	}

	dialOptions, err := grpcDialOptions(
		*certFile,
		*keyFile,
		*caFile,
	)
	if err != nil {
		return err
	}

	conn, err := grpc.Dial(
		*addr,
		dialOptions...,
	)
	if err != nil {
		return err
	}
	defer conn.Close()

	client := proto.NewBrokerServiceClient(conn)

	response, err := client.Produce(
		context.Background(),
		&proto.ProduceRequest{
			Topic:     *topic,
			Partition: partitionPtr,
			Key:       []byte("msg"),
			Value:     []byte(*msg),
		},
	)
	if err != nil {
		return err
	}

	fmt.Println(response.Offset)

	return nil
}

// consume reads one message from the gRPC API.
func consume(args []string) error {
	flags := flag.NewFlagSet(
		"consume",
		flag.ContinueOnError,
	)

	addr := flags.String(
		"addr",
		"localhost:9080",
		"broker gRPC address",
	)

	topic := flags.String(
		"topic",
		"default",
		"topic name",
	)

	partition := flags.Int(
		"partition",
		0,
		"partition",
	)

	offset := flags.Uint64(
		"offset",
		0,
		"message offset",
	)

	certFile := flags.String(
		"cert",
		"",
		"client TLS certificate",
	)

	keyFile := flags.String(
		"key",
		"",
		"client TLS private key",
	)

	caFile := flags.String(
		"ca",
		"",
		"CA certificate",
	)

	if err := flags.Parse(args); err != nil {
		return err
	}

	if *partition < 0 || *partition >= 3 {
		return fmt.Errorf(
			"partition must be between 0 and 2",
		)
	}

	dialOptions, err := grpcDialOptions(
		*certFile,
		*keyFile,
		*caFile,
	)
	if err != nil {
		return err
	}

	conn, err := grpc.Dial(
		*addr,
		dialOptions...,
	)
	if err != nil {
		return err
	}
	defer conn.Close()

	client := proto.NewBrokerServiceClient(conn)

	p := int32(*partition)

	response, err := client.Consume(
		context.Background(),
		&proto.ConsumeRequest{
			Topic:     *topic,
			Partition: &p,
			Offset:    *offset,
		},
	)
	if err != nil {
		return err
	}

	fmt.Printf(
		"key: %s\n",
		string(response.Key),
	)

	fmt.Printf(
		"value: %s\n",
		string(response.Value),
	)

	return nil
}

// consumeGroup consumes messages using a committed consumer-group offset.
func consumeGroup(args []string) error {
	flags := flag.NewFlagSet(
		"consume-group",
		flag.ContinueOnError,
	)

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
		"topic name",
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

	certFile := flags.String(
		"cert",
		"",
		"client TLS certificate",
	)

	keyFile := flags.String(
		"key",
		"",
		"client TLS private key",
	)

	caFile := flags.String(
		"ca",
		"",
		"CA certificate",
	)

	if err := flags.Parse(args); err != nil {
		return err
	}

	if *group == "" {
		return fmt.Errorf("--group is required")
	}

	if *partition < 0 || *partition >= 3 {
		return fmt.Errorf(
			"partition must be between 0 and 2",
		)
	}

	dialOptions, err := grpcDialOptions(
		*certFile,
		*keyFile,
		*caFile,
	)
	if err != nil {
		return err
	}

	conn, err := grpc.Dial(
		*addr,
		dialOptions...,
	)
	if err != nil {
		return err
	}
	defer conn.Close()

	client := proto.NewBrokerServiceClient(conn)

	p := int32(*partition)

	for {
		fetchResponse, err := client.FetchOffset(
			context.Background(),
			&proto.FetchOffsetRequest{
				GroupId:   *group,
				Topic:     *topic,
				Partition: p,
			},
		)
		if err != nil {
			if !*follow {
				return err
			}

			time.Sleep(time.Second)
			continue
		}

		offset := fetchResponse.Offset

		consumeResponse, err := client.Consume(
			context.Background(),
			&proto.ConsumeRequest{
				Topic:     *topic,
				Partition: &p,
				Offset:    offset,
			},
		)
		if err != nil {
			if !*follow {
				return err
			}

			time.Sleep(time.Second)
			continue
		}

		fmt.Printf(
			"key: %s\n",
			string(consumeResponse.Key),
		)

		fmt.Printf(
			"value: %s\n",
			string(consumeResponse.Value),
		)

		_, err = client.CommitOffset(
			context.Background(),
			&proto.CommitOffsetRequest{
				GroupId:   *group,
				Topic:     *topic,
				Partition: p,
				Offset:    consumeResponse.Offset + 1,
			},
		)
		if err != nil {
			if !*follow {
				return err
			}

			time.Sleep(time.Second)
			continue
		}

		if !*follow {
			return nil
		}

		time.Sleep(time.Second)
	}
}

// grpcDialOptions returns mTLS credentials when all certificate paths are
// supplied, otherwise it returns insecure credentials.
func grpcDialOptions(
	certFile string,
	keyFile string,
	caFile string,
) ([]grpc.DialOption, error) {
	if certFile == "" &&
		keyFile == "" &&
		caFile == "" {
		return []grpc.DialOption{
			grpc.WithTransportCredentials(
				insecure.NewCredentials(),
			),
		}, nil
	}

	if certFile == "" ||
		keyFile == "" ||
		caFile == "" {
		return nil, fmt.Errorf(
			"cert, key, and ca must either all be provided or all be omitted",
		)
	}

	config, err := deltatls.LoadClientTLSConfig(
		certFile,
		keyFile,
		caFile,
	)
	if err != nil {
		return nil, err
	}

	return []grpc.DialOption{
		grpc.WithTransportCredentials(
			credentials.NewTLS(config),
		),
	}, nil
}

// printUsage prints the available CLI commands.
func printUsage() {
	fmt.Println(`Usage:

  elog produce --addr localhost:9080 --topic default --partition 0 --msg "hello"
  elog consume --addr localhost:9080 --topic default --partition 0 --offset 0
  elog consume-group --addr localhost:9080 --group group1 --topic default --partition 0 --follow

TLS options:

  --cert <client certificate>
  --key  <client private key>
  --ca   <CA certificate>`)
}
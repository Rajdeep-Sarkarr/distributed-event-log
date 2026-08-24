package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
)

type publishResponse struct {
	Offset uint64 `json:"offset"`
}

type readResponse struct {
	Offset    uint64 `json:"offset"`
	Timestamp int64  `json:"timestamp"`
	Key       string `json:"key"`
	Value     string `json:"value"`
}

// main parses the CLI subcommand and executes the requested operation.
func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// run executes the CLI command represented by the supplied arguments.
func run(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: elog produce|consume")
	}

	switch args[0] {
	case "produce":
		return produce(args[1:])
	case "consume":
		return consume(args[1:])
	default:
		return fmt.Errorf("unknown command: %s", args[0])
	}
}

// produce parses the produce arguments and publishes the message.
func produce(args []string) error {
	var message string

	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--topic":
			if i+1 >= len(args) {
				return fmt.Errorf("missing value for --topic")
			}
			i++

		case "--msg":
			if i+1 >= len(args) {
				return fmt.Errorf("missing value for --msg")
			}
			message = args[i+1]
			i++

		default:
			return fmt.Errorf("unknown produce option: %s", args[i])
		}
	}

	if message == "" {
		return fmt.Errorf("missing --msg")
	}

	body := fmt.Sprintf(`{"key":"msg","value":%s}`, strconv.Quote(message))

	response, err := http.Post(
	"http://localhost:8080/publish",
	"application/json",
	strings.NewReader(body),
	)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	if response.StatusCode < 200 || response.StatusCode >= 300 {
		data, _ := io.ReadAll(response.Body)
		return fmt.Errorf("publish failed: %s", string(data))
	}

	var result publishResponse
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return err
	}

	fmt.Println(result.Offset)

	return nil
}

// consume parses the consume arguments and reads the requested message.
func consume(args []string) error {
	var offsetString string

	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--offset":
			if i+1 >= len(args) {
				return fmt.Errorf("missing value for --offset")
			}
			offsetString = args[i+1]
			i++

		default:
			return fmt.Errorf("unknown consume option: %s", args[i])
		}
	}

	if offsetString == "" {
		return fmt.Errorf("missing --offset")
	}

	offset, err := strconv.ParseUint(offsetString, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid offset: %w", err)
	}

	requestURL := "http://localhost:8080/read?offset=" +
		url.QueryEscape(strconv.FormatUint(offset, 10))

	response, err := http.Get(requestURL)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	if response.StatusCode < 200 || response.StatusCode >= 300 {
		data, _ := io.ReadAll(response.Body)
		return fmt.Errorf("consume failed: %s", string(data))
	}

	var result readResponse
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return err
	}

	fmt.Printf("key: %s\n", result.Key)
	fmt.Printf("value: %s\n", result.Value)

	return nil
}

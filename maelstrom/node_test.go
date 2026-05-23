package maelstrom

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
)

func newTestNode() (*Node, *bytes.Buffer) {
	out := &bytes.Buffer{}
	n := newNode(out)

	return n, out
}

func encodeMessage[T any](msg Message[T]) []byte {
	data, _ := json.Marshal(msg)
	return append(data, '\n')
}

func readOutput[T any](out *bytes.Buffer) ([]Message[T], error) {
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	msgs := make([]Message[T], 0, len(lines))

	for _, line := range lines {
		var msg Message[T]
		if err := json.Unmarshal([]byte(line), &msg); err != nil {
			return nil, fmt.Errorf("failed to unmarshal message: %w", err)
		}

		msgs = append(msgs, msg)
	}

	return msgs, nil
}

func TestInitMessage(t *testing.T) {
	n, out := newTestNode()
	initMsg := Message[initRequest]{
		Src: "c1",
		Dst: "n3",
		Body: MessageBody[initRequest]{
			Type:  "init",
			MsgID: 1,
			Payload: initRequest{
				NodeID:  "n3",
				NodeIDs: []string{"n1", "n2", "n3"},
			},
		},
	}

	input := bytes.NewBuffer(nil)
	input.Write(encodeMessage(initMsg))

	if err := n.run(context.Background(), input); err != nil {
		t.Fatal(err)
	}

	msgs, err := readOutput[EmptyPayload](out)
	if err != nil {
		t.Fatal(err)
	}

	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}

	res := msgs[0]
	if res.Body.Type != "init_ok" {
		t.Errorf("expected response type \"init_ok\", got %q", res.Body.Type)
	}

	if res.Body.InReplyTo != initMsg.Body.MsgID {
		t.Errorf("expected \"in_reply_to\" value %d, got %d", initMsg.Body.MsgID, res.Body.InReplyTo)
	}

	if n.NodeID != "n3" {
		t.Errorf("expected node ID \"n3\", got %q", n.NodeID)
	}

	expectedIDs := []string{"n1", "n2", "n3"}
	if !slices.Equal(n.NodeIDs, expectedIDs) {
		t.Errorf("expected node IDs %v, got %v", expectedIDs, n.NodeIDs)
	}
}

func TestUnregisteredMessageType(t *testing.T) {
	type DummyRequest struct {
		Data string `json:"data"`
	}

	n, _ := newTestNode()
	msg := Message[DummyRequest]{
		Src: "c1",
		Dst: "n3",
		Body: MessageBody[DummyRequest]{
			Type:  "unknown",
			MsgID: 1,
			Payload: DummyRequest{
				Data: "hello",
			},
		},
	}

	input := bytes.NewBuffer(encodeMessage(msg))

	if err := n.run(context.Background(), input); err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestConcurrentMessageHandling(t *testing.T) {
	type CountRequest struct {
		Value int `json:"value"`
	}

	var mu sync.Mutex
	const numMessages = 100

	n, out := newTestNode()
	counter := 0

	Handle(n, "count", func(incoming Message[CountRequest]) error {
		mu.Lock()
		counter += incoming.Body.Payload.Value
		mu.Unlock()

		return Reply(n, incoming, "count_ok", EmptyPayload{})
	})

	input := &bytes.Buffer{}
	for i := 1; i <= numMessages; i++ {
		msg := Message[CountRequest]{
			Src: "c1",
			Dst: "n3",
			Body: MessageBody[CountRequest]{
				Type:  "count",
				MsgID: uint(i),
				Payload: CountRequest{
					Value: 1,
				},
			},
		}

		input.Write(encodeMessage(msg))
	}

	if err := n.run(context.Background(), input); err != nil {
		t.Fatal(err)
	}

	if counter != numMessages {
		t.Errorf("expected counter value %d, got %d", numMessages, counter)
	}

	msgs, err := readOutput[CountRequest](out)
	if err != nil {
		t.Fatal(err)
	}

	if len(msgs) != numMessages {
		t.Errorf("expected %d replies, got %d", numMessages, len(msgs))
	}
}

func TestMalformedMessages(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			"missing type field",
			`{"src":"c1","dest":"n1","body":{"msg_id":1}}`,
		},
		{
			"missing body field",
			`{"src":"c1","dest":"n1"}`,
		},
		{
			"malformed JSON",
			`{"src":"c1" "dest":"n1","body":{"type""echo","msg_id":1}}`,
		},
		{
			"invalid body field type",
			`{"src":"c1","dest":"n1","body":"just a string"}`,
		},
		{
			"empty line",
			"",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// t.Parallel()

			node, out := newTestNode()
			input := bytes.NewBufferString(tt.input + "\n")

			if err := node.run(context.Background(), input); err != nil {
				t.Fatalf("expected nil, got error: %v", err)
			}

			if out.Len() != 0 {
				t.Fatalf("expected no response, got %q", out.String())
			}
		})
	}
}

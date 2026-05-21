package maelstrom

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
)

func newTestNode() (*Node, *bytes.Buffer) {
	out := &bytes.Buffer{}
	node := newNode(out)

	return node, out
}

func encodeMessage[In hasMetadata](msg Message[In]) []byte {
	data, _ := json.Marshal(msg)
	return append(data, '\n')
}

func readOutput[T hasMetadata](out *bytes.Buffer) ([]Message[T], error) {
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
	node, out := newTestNode()
	initMsg := Message[initRequest]{
		Src: "c1",
		Dst: "n3",
		Body: initRequest{
			RPCMetadata: RPCMetadata{
				Type:  "init",
				MsgID: 1,
			},
			NodeID:  "n3",
			NodeIDs: []string{"n1", "n2", "n3"},
		},
	}

	input := bytes.NewBuffer(nil)
	input.Write(encodeMessage(initMsg))

	if err := node.run(input); err != nil {
		t.Fatal(err)
	}

	msgs, err := readOutput[initResponse](out)
	if err != nil {
		t.Fatal(err)
	}

	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}

	resp := msgs[0]
	if resp.Body.Type != "init_ok" {
		t.Errorf("expected response type \"init_ok\", got %q", resp.Body.Type)
	}

	if resp.Body.InReplyTo != initMsg.Body.MsgID {
		t.Errorf("expected \"in_reply_to\" value %d, got %d", initMsg.Body.MsgID, resp.Body.InReplyTo)
	}

	if node.NodeID != "n3" {
		t.Errorf("expected node ID \"n3\", got %q", node.NodeID)
	}

	expectedIDs := []string{"n1", "n2", "n3"}
	if !slices.Equal(node.NodeIDs, expectedIDs) {
		t.Errorf("expected node IDs %v, got %v", expectedIDs, node.NodeIDs)
	}
}

func TestUnregisteredMessageType(t *testing.T) {
	type DummyMessage struct {
		RPCMetadata
		Data string `json:"data"`
	}

	node, _ := newTestNode()
	msg := Message[DummyMessage]{
		Src: "c1",
		Dst: "n3",
		Body: DummyMessage{
			RPCMetadata: RPCMetadata{
				Type:  "unknown",
				MsgID: 1,
			},
			Data: "hello",
		},
	}

	input := bytes.NewBuffer(encodeMessage(msg))

	if err := node.run(input); err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestConcurrentMessageHandling(t *testing.T) {
	const numMessages = 100
	var mu sync.Mutex

	node, out := newTestNode()
	counter := 0

	type CountMessage struct {
		RPCMetadata
		Value int `json:"value"`
	}

	Handle(node, "count", func(incoming Message[CountMessage]) error {
		mu.Lock()
		counter += incoming.Body.Value
		mu.Unlock()

		outgoing := incoming.Body
		outgoing.Type = "count_ok"
		outgoing.MsgID = node.NextMsgID()
		outgoing.InReplyTo = incoming.Body.MsgID

		return Reply(node, incoming, outgoing)
	})

	input := &bytes.Buffer{}
	for i := 1; i <= numMessages; i++ {
		msg := Message[CountMessage]{
			Src: "c1",
			Dst: "n3",
			Body: CountMessage{
				RPCMetadata: RPCMetadata{
					Type:  "count",
					MsgID: uint64(i),
				},
				Value: 1,
			},
		}
		input.Write(encodeMessage(msg))
	}

	if err := node.run(input); err != nil {
		t.Fatal(err)
	}

	if counter != numMessages {
		t.Errorf("expected counter value %d, got %d", numMessages, counter)
	}

	msgs, err := readOutput[CountMessage](out)
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
			t.Parallel()

			node, out := newTestNode()
			input := bytes.NewBufferString(tt.input + "\n")

			if err := node.run(input); err != nil {
				t.Fatalf("expected nil, got error: %v", err)
			}

			if out.Len() != 0 {
				t.Fatalf("expected no response, got %q", out.String())
			}
		})
	}
}

// Message-Based Communication
//
// Distributed systems communicate by exchanging messages. Unlike threads in a
// single process, distributed nodes cannot directly share memory, and this
// constraint fundamentally shapes how distributed algorithms are designed.
//
// In a traditional single-machine program, threads operate within the same
// virtual address space and can communicate through shared memory. Separate
// processes can also communicate locally using mechanisms such as sockets,
// pipes, or memory-mapped pages (`mmap`).
//
// In a distributed system, however, each program runs on a different machine
// with its own memory and execution environment. Communication must happen
// over a network, which introduces additional challenges:
//
//   - messages may be delayed
//   - messages may be duplicated
//   - messages may be lost
//   - machines may fail independently
//
// Because nodes cannot directly inspect each other's state, every message
// must contain enough information for the receiving node to process it
// independently.
package messenger

import (
	"encoding/json"
	"fmt"
)

// Message represents a JSON message exchanged between nodes in a distributed
// system.
//
// In the `Maelstrom` framework, messages are sent over `stdin/stdout` to keep
// the system simple and language-agnostic.
type Message struct {
	Src  string         `json:"src"`
	Dest string         `json:"dest"`
	Body map[string]any `json:"body"`
}

// ParseMessage decodes a JSON-encoded message from the given byte slice,
// returning a Message struct or an error if the input is not valid JSON.
//
// Example message format:
//
//	{
//	  "src": "c1",
//	  "dest": "n1",
//	  "body": {
//	    "type": "ping",
//	    "msg_id": 1
//	  }
//	}
func ParseMessage(buf []byte) (Message, error) {
	var msg Message

	err := json.Unmarshal(buf, &msg)
	if err != nil {
		return Message{}, err
	}

	return msg, nil
}

// FormatMessage returns a formatted string of the form:
//
//	"PARSED: src|dest|body_type"
func (msg *Message) FormatMessage() string {
	bodyType := "unknown"
	if value, ok := msg.Body["type"].(string); ok {
		bodyType = value
	}

	return fmt.Sprintf("PARSED: %s|%s|%s", msg.Src, msg.Dest, bodyType)
}

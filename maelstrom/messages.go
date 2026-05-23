package maelstrom

import (
	"encoding/json"
	"fmt"
)

type initRequest struct {
	NodeID  string   `json:"node_id"`
	NodeIDs []string `json:"node_ids"`
}

// EmptyPayload represents a [Message] with no expected payload.
type EmptyPayload struct{}

// Message represents a single `Maelstrom` message. Each instantiated
// [MessageBody] must marshal to a valid JSON object.
type Message[T any] struct {
	Src  string         `json:"src"`
	Dst  string         `json:"dest"`
	Body MessageBody[T] `json:"body"`
}

// MessageBody represents fields common to each `Maelstrom` message, with a
// generic payload.
type MessageBody[T any] struct {
	Type      string    `json:"type"`
	MsgID     uint      `json:"msg_id,omitempty"`
	InReplyTo uint      `json:"in_reply_to,omitempty"`
	Code      ErrorCode `json:"code,omitempty"`
	Text      string    `json:"text,omitempty"`
	Payload   T         `json:"-"`
}

func (body MessageBody[T]) MarshalJSON() ([]byte, error) {
	payloadBytes, err := json.Marshal(body.Payload)
	if err != nil {
		return nil, err
	}

	payloadMap := map[string]json.RawMessage{}
	if len(payloadBytes) > 0 && string(payloadBytes) != "null" {
		if err := json.Unmarshal(payloadBytes, &payloadMap); err != nil {
			return nil, err
		}
	}

	out := map[string]any{
		"type": body.Type,
	}

	if body.MsgID != 0 {
		out["msg_id"] = body.MsgID
	}

	if body.InReplyTo != 0 {
		out["in_reply_to"] = body.InReplyTo
	}

	if body.Code != 0 {
		out["code"] = body.Code
	}

	if body.Text != "" {
		out["text"] = body.Text
	}

	for k, v := range payloadMap {
		if _, exists := out[k]; exists {
			return nil, fmt.Errorf("payload field %q conflicts with reserved field", k)
		}

		out[k] = v
	}

	return json.Marshal(out)
}

func (body *MessageBody[T]) UnmarshalJSON(data []byte) error {
	type meta struct {
		Type      string    `json:"type"`
		MsgID     uint      `json:"msg_id"`
		InReplyTo uint      `json:"in_reply_to"`
		Code      ErrorCode `json:"code"`
		Text      string    `json:"text"`
	}

	var m meta
	if err := json.Unmarshal(data, &m); err != nil {
		return err
	}

	body.Type = m.Type
	body.MsgID = m.MsgID
	body.InReplyTo = m.InReplyTo
	body.Code = m.Code
	body.Text = m.Text

	raw := map[string]json.RawMessage{}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	delete(raw, "type")
	delete(raw, "msg_id")
	delete(raw, "in_reply_to")
	delete(raw, "code")
	delete(raw, "text")

	remaining, err := json.Marshal(raw)
	if err != nil {
		return err
	}

	if len(raw) == 0 {
		var zero T
		body.Payload = zero

		return nil
	}

	return json.Unmarshal(remaining, &body.Payload)
}

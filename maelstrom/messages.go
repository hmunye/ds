package maelstrom

// Message represents a `Maelstrom` message. Each instantiated Message must
// embed [RPCMetadata].
type Message[T hasMetadata] struct {
	Src  string `json:"src"`
	Dst  string `json:"dest"`
	Body T      `json:"body"`
}

// RPCMetadata contains metadata fields shared by `Maelstrom` message payloads.
type RPCMetadata struct {
	Type      string `json:"type"`
	MsgID     uint   `json:"msg_id,omitempty"`
	InReplyTo uint   `json:"in_reply_to,omitempty"`
}

func (meta RPCMetadata) MetaType() string    { return meta.Type }
func (meta RPCMetadata) MetaMsgID() uint     { return meta.MsgID }
func (meta RPCMetadata) MetaInReplyTo() uint { return meta.InReplyTo }

// ErrorMessage represents an "error" message exchanged between nodes.
type ErrorMessage struct {
	RPCMetadata
	Code ErrorCode `json:"code"`
	Text string    `json:"text,omitempty"`
}

// hasMetadata is used as a compile-time constraint ensuring each instantiated
// [Message.Body] embeds [RPCMetadata].
type hasMetadata interface {
	MetaType() string
	MetaMsgID() uint
	MetaInReplyTo() uint
}

type initRequest struct {
	RPCMetadata
	NodeID  string   `json:"node_id"`
	NodeIDs []string `json:"node_ids"`
}

type initResponse struct {
	RPCMetadata
}

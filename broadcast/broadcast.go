// Package broadcast implements a gossip-based protocol for disseminating
// messages across a cluster of `Maelstrom` nodes.
//
// In a distributed system, nodes cannot share memory, so information must be
// explicitly propagated across the network. One straightforward approach is for
// each node to broadcast directly to every other node. While this works in
// small clusters, it does not scale well. With N nodes, each broadcast requires
// N-1 outbound messages from the sender, and each node must independently
// transmit and receive a full fan-out of traffic for every broadcast it
// originates. This leads to O(N^2) total message traffic across the cluster as
// the system grows. Under network partitions or node failures, messages may be
// dropped without any mechanism for recovery.
//
// Gossip protocols solve this by having each node exchange information with a
// small random subset of its peers rather than the entire cluster. Each
// recipient repeats the process, causing information to propagate rapidly
// across the network over multiple rounds.
//
// This approach has three main properties:
//
//   - Partition Tolerance: Information continues to spread along any available
//     network paths, so partitions delay convergence rather than preventing it.
//
//   - Eventual Consistency: Given sufficient time and a connected network, all
//     nodes will converge on the same set of messages.
//
//   - Failure Resilience: Since dissemination is decentralized, the failure of
//     individual nodes does not prevent the remaining cluster from converging.
//
// The tradeoff is increased latency: information takes multiple rounds to
// propagate throughout the system rather than being delivered immediately. The
// latency depends on the fan-out (i.e., the number of peers each node exchanges
// messages with per round) and the gossip interval (i.e., how often gossip
// rounds are executed). Higher fan-out converges faster but generates more
// traffic, while lower fan-out reduces communication cost at the expense of
// slower convergence and reduced resilience to message loss.
package broadcast

import (
	"github.com/hmunye/ds/maelstrom"
)

type broadcastRequest struct {
	Message int `json:"message"`
}

type topologyRequest struct {
	Topology map[string][]string `json:"topology"`
}

type readResponse struct {
	Messages []int `json:"messages"`
}

// Broadcaster maintains the state of the gossip protocol for a given
// [maelstrom.Node].
type Broadcaster struct {
	n        *maelstrom.Node
	messages []int
}

// New returns a Broadcaster using the given [maelstrom.Node]. The Broadcaster
// is not active until [Register] is called.
func New(n *maelstrom.Node) *Broadcaster {
	return &Broadcaster{
		n:        n,
		messages: make([]int, 0),
	}
}

// Register installs the "broadcast", "topology", and "read" handlers on the
// [maelstrom.Node].
func (b *Broadcaster) Register() {
	maelstrom.Handle(b.n, "broadcast",
		func(incoming maelstrom.Message[broadcastRequest]) error {
			b.messages = append(b.messages, incoming.Body.Payload.Message)

			return maelstrom.Reply(b.n, incoming, "broadcast_ok", maelstrom.EmptyPayload{})
		})

	maelstrom.Handle(b.n, "read",
		func(incoming maelstrom.Message[maelstrom.EmptyPayload]) error {
			payload := readResponse{
				Messages: b.messages,
			}

			return maelstrom.Reply(b.n, incoming, "read_ok", payload)
		})

	maelstrom.Handle(b.n, "topology",
		func(incoming maelstrom.Message[topologyRequest]) error {
			// Ignoring the provided topology.
			return maelstrom.Reply(b.n, incoming, "topology_ok", maelstrom.EmptyPayload{})
		})
}

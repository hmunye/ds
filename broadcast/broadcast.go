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
	"log/slog"
	"math/rand"
	"slices"
	"sync"
	"time"

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

type gossipRequest struct {
	Messages []int `json:"messages"`
}

// Broadcaster maintains the state of the gossip protocol for a given
// [maelstrom.Node].
type Broadcaster struct {
	n                 *maelstrom.Node
	fanout            int
	interval, timeout time.Duration
	messages          map[int]struct{} // set for message de-duplication
	msgMu             sync.Mutex
	known             map[string]map[int]struct{}
	knownMu           sync.Mutex
}

// New returns a Broadcaster using the given [maelstrom.Node].
func New(n *maelstrom.Node) *Broadcaster {
	return &Broadcaster{
		n:        n,
		fanout:   3,
		interval: 100 * time.Millisecond,
		timeout:  time.Second,
		messages: make(map[int]struct{}),
		known:    make(map[string]map[int]struct{}),
	}
}

// WithFanout sets the number of peers the [maelstrom.Node] gossips with on each
// interval. The effective value is capped at the number of available peers.
// Defaults to 3.
//
// Increasing fan-out improves propagation speed and lowers time to convergence
// (latency), but increases network traffic and message overhead. Decreasing
// fan-out reduces network cost but increases time to convergence.
func (b *Broadcaster) WithFanout(f int) *Broadcaster {
	b.fanout = f
	return b
}

// WithInterval sets how frequently the [maelstrom.Node] gossips with its peers.
// Defaults to 100ms.
//
// A lower interval increases gossip frequency, improving time to convergence
// but increasing network traffic. A higher interval reduces traffic but slows
// down convergence.
func (b *Broadcaster) WithInterval(d time.Duration) *Broadcaster {
	b.interval = d
	return b
}

// WithTimeout sets how long to wait for a "gossip" response from a peer before
// giving up. Defaults to 1s.
func (b *Broadcaster) WithTimeout(d time.Duration) *Broadcaster {
	b.timeout = d
	return b
}

// Start registers the "broadcast", "topology", "read", and "gossip" handlers on
// the [maelstrom.Node] and periodically sends messages to its peers in the
// background. Must be called before [maelstrom.Node.Run].
func (b *Broadcaster) Start() {
	maelstrom.Handle(b.n, "broadcast",
		func(incoming maelstrom.Message[broadcastRequest]) error {
			b.msgMu.Lock()

			b.messages[incoming.Body.Payload.Message] = struct{}{}

			b.msgMu.Unlock()

			return maelstrom.Reply(b.n, incoming, "broadcast_ok", maelstrom.EmptyPayload{})
		})

	maelstrom.Handle(b.n, "read",
		func(incoming maelstrom.Message[maelstrom.EmptyPayload]) error {
			payload := readResponse{
				Messages: b.snapshotMessages(),
			}

			return maelstrom.Reply(b.n, incoming, "read_ok", payload)
		})

	maelstrom.Handle(b.n, "topology",
		func(incoming maelstrom.Message[topologyRequest]) error {
			// Ignore the provided topology and instead use a random subset of
			// peers derived from the full cluster, based on fan-out.
			return maelstrom.Reply(b.n, incoming, "topology_ok", maelstrom.EmptyPayload{})
		})

	maelstrom.Handle(b.n, "gossip",
		func(incoming maelstrom.Message[gossipRequest]) error {
			b.msgMu.Lock()

			for _, msg := range incoming.Body.Payload.Messages {
				b.messages[msg] = struct{}{}
			}

			b.msgMu.Unlock()

			return maelstrom.Reply(b.n, incoming, "gossip_ok", maelstrom.EmptyPayload{})
		})

	go b.gossip()
}

func (b *Broadcaster) gossip() {
	var peers []string

	ticker := time.NewTicker(b.interval)
	defer ticker.Stop()

	for range ticker.C {
		if peers == nil {
			if len(b.n.NodeIDs) == 0 {
				continue
			}

			peers = slices.Clone(b.n.NodeIDs)

			// Remove this node from the peers list for future broadcasts.
			if i := slices.Index(peers, b.n.NodeID); i != -1 {
				peers = slices.Delete(peers, i, i+1)
			}
		}

		shufflePeers(peers)
		subset := peers[:min(b.fanout, len(peers))]

		for _, peer := range subset {
			b.knownMu.Lock()

			peer_msgs := b.known[peer]
			delta := b.deltaMessages(peer_msgs)

			b.knownMu.Unlock()

			if len(delta) == 0 {
				continue
			}

			payload := gossipRequest{delta}

			go func() {
				ch, err := maelstrom.RPC[
					maelstrom.EmptyPayload,
					gossipRequest,
				](b.n, peer, "gossip", payload)
				if err != nil {
					slog.Error("failed to send \"gossip\" request",
						slog.Any("error", err))
				}

				select {
				case <-ch:
					b.knownMu.Lock()

					peer_msgs := b.known[peer]
					if peer_msgs == nil {
						peer_msgs = make(map[int]struct{}, len(delta))
					}

					for _, msg := range delta {
						peer_msgs[msg] = struct{}{}
					}

					b.knownMu.Unlock()
				case <-time.After(b.timeout):
					slog.Warn("timed-out waiting for \"gossip\" response")
				}
			}()
		}
	}
}

// deltaMessages computes the difference between this node's message set and the
// peer's known messages. Sending only unseen messages improves gossip
// efficiency by avoiding redundant retransmission and reducing payload sizes.
func (b *Broadcaster) deltaMessages(peer_msgs map[int]struct{}) []int {
	b.msgMu.Lock()
	defer b.msgMu.Unlock()

	delta := make([]int, 0)

	for msg := range b.messages {
		if _, ok := peer_msgs[msg]; !ok {
			delta = append(delta, msg)
		}
	}

	return delta
}

// snapshotMessages converts this node's message set into a slice of messages,
// ready for transmission.
func (b *Broadcaster) snapshotMessages() []int {
	b.msgMu.Lock()
	defer b.msgMu.Unlock()

	msgs := make([]int, 0, len(b.messages))

	for k := range b.messages {
		msgs = append(msgs, k)
	}

	return msgs
}

// shufflePeers randomizes the order of peers. This ensures that gossip messages
// propagate along different paths through the network, preventing hotspots and
// improving overall convergence speed and reliability.
func shufflePeers(peers []string) {
	rand.Shuffle(len(peers), func(i, j int) {
		peers[i], peers[j] = peers[j], peers[i]
	})
}

# ds

Distributed Systems from scratch using [Gossip Glomers] challenges and the
[Maelstrom] test framework to learn concepts through hands-on implementation.

[Gossip Glomers]: https://fly.io/dist-sys/
[Maelstrom]: https://github.com/jepsen-io/maelstrom

## Challenges

Build the Docker image containing Maelstrom:

```bash
docker build -t maelstrom .
```

Maelstrom "echo" workload:

```bash
./maelstrom.sh test -w echo --bin /usr/local/bin/echo --node-count 1 --time-limit 10
```

Maelstrom "unique-ids" workload:

```bash
./maelstrom.sh test -w unique-ids --bin /usr/local/bin/unique_ids --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition
```

Maelstrom "broadcast" workload (single-node):

```bash
./maelstrom.sh test -w broadcast --bin /usr/local/bin/broadcast --node-count 1 --time-limit 20 --rate 10
```

Maelstrom "broadcast" workload (multi-node):

```bash
./maelstrom.sh test -w broadcast --bin /usr/local/bin/broadcast --node-count 5 --time-limit 20 --rate 10
```

Maelstrom "broadcast" workload (multi-node, partitioned):

```bash
./maelstrom.sh test -w broadcast --bin /usr/local/bin/broadcast --node-count 5 --time-limit 20 --rate 10 --nemesis partition
```

Maelstrom "broadcast" workload (multi-node, efficient):

```bash
./maelstrom.sh test -w broadcast --bin /usr/local/bin/broadcast --node-count 25 --time-limit 20 --rate 100 --latency 100
```

Part I: (`WithFanout`=4, `WithInterval`=120ms)

- Messages-per-operation is below 30
- Median latency is below 400ms
- Maximum latency is below 600ms

Part II: (`WithFanout`=3, `WithInterval`=150ms)

- Messages-per-operation is below 20
- Median latency is below 1 second
- Maximum latency is below 2 seconds

Maelstrom "g-counter" workload (multi-node, partitioned):

```bash
./maelstrom.sh test -w g-counter --bin /usr/local/bin/g_counter --node-count 3 --rate 100 --time-limit 20 --nemesis partition
```

Maelstrom "kafka" workload (single-node):

```bash
./maelstrom.sh test -w kafka --bin /usr/local/bin/kafka --node-count 1 --concurrency 2n --time-limit 20 --rate 1000
```

Maelstrom "kafka" workload (multi-node, efficient):

```bash
./maelstrom.sh test -w kafka --bin /usr/local/bin/kafka --node-count 2 --concurrency 2n --time-limit 20 --rate 1000
```

Maelstrom "txn-rw-register" workload (single-node, read uncommitted, totally available)

```bash
./maelstrom.sh test -w txn-rw-register --bin /usr/local/bin/txn --node-count 1 --time-limit 20 --rate 1000 --concurrency 2n --consistency-models read-uncommitted --availability total
```

Maelstrom "txn-rw-register" workload (multi-node, read uncommitted, totally available)

```bash
./maelstrom.sh test -w txn-rw-register --bin /usr/local/bin/txn --node-count 2 --concurrency 2n --time-limit 20 --rate 1000 --consistency-models read-uncommitted --availability total
```

Maelstrom "txn-rw-register" workload (multi-node, read uncommitted, partitioned)

```bash
./maelstrom.sh test -w txn-rw-register --bin /usr/local/bin/txn --node-count 2 --concurrency 2n --time-limit 20 --rate 1000 --consistency-models read-uncommitted --availability total --nemesis partition
```

Maelstrom "txn-rw-register" workload (multi-node, read committed, partitioned)

```bash
./maelstrom.sh test -w txn-rw-register --bin /usr/local/bin/txn --node-count 2 --concurrency 2n --time-limit 20 --rate 1000 --consistency-models read-committed --availability total –-nemesis partition
```

## License

This project is licensed under the [MIT License].

[MIT License]: https://github.com/hmunye/ds/blob/main/LICENSE

## References

- [Gossip Glomers](https://fly.io/dist-sys/)
- [Maelstrom](https://github.com/jepsen-io/maelstrom)

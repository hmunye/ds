# ds

Distributed Systems from scratch using [Gossip Glomers] challenges and the
[Maelstrom] test framework to learn concepts through hands-on implementation.

## Quick Start

Build the Docker image containing Maelstrom:

```bash
docker build -t maelstrom .
```

Maelstrom "echo" workload:

```bash
./maelstrom.sh test -w echo --bin /bin/node --node-count 1 --time-limit 10
```

Maelstrom "unique-ids" workload:

```bash
./maelstrom.sh test -w unique-ids --bin /bin/node --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition
```

Maelstrom "broadcast" workload (single-node):

```bash
./maelstrom.sh test -w broadcast --bin /bin/node --node-count 1 --time-limit 20 --rate 10
```

Maelstrom "broadcast" workload (multi-node):

```bash
./maelstrom.sh test -w broadcast --bin /bin/node --node-count 5 --time-limit 20 --rate 10
```

Maelstrom "broadcast" workload (fault-tolerant):

```bash
./maelstrom.sh test -w broadcast --bin /bin/node --node-count 5 --time-limit 20 --rate 10 --nemesis partition
```

## License

This project is licensed under the [MIT License].

[MIT License]: https://github.com/hmunye/ds/blob/main/LICENSE

## References

- [Gossip Glomers](https://fly.io/dist-sys/)
- [Maelstrom](https://github.com/jepsen-io/maelstrom)

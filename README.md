# ds

Building Distributed Systems from Scratch.

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

## License

This project is licensed under the [MIT License].

[MIT License]: https://github.com/hmunye/ds/blob/main/LICENSE

## References

- [Gossip Glomers](https://fly.io/dist-sys/)
- [Maelstrom](https://github.com/jepsen-io/maelstrom)

# ds

Building Distributed Systems from Scratch.

## Quick Start

Build the Docker image containing Maelstrom:

```bash
docker build -t maelstrom .
```

Test Maelstrom "echo" workload:

```bash
./maelstrom.sh test -w echo --bin /bin/node --node-count 1 --time-limit 10
```

## License

This project is licensed under the [MIT License].

[MIT License]: https://github.com/hmunye/ds/blob/main/LICENSE

## References

- [Build Distributed Systems from Scratch](https://builddistributedsystem.com/)
- [Gossip Glomers](https://fly.io/dist-sys/)
- [Maelstrom](https://github.com/jepsen-io/maelstrom)

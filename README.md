# LLRStore

LLRStore is contained within this monorepo. It is a distributed key-value store using Dynamo style replication and gossip based membership protocol.

It is developed by Leo Steven and Robert Frank and Me for the course "Cloud Database System" at TUM.

- We implemented Dotted Version Vectors for conflict resolution.
- We implemented CRDT-based gossip protocol for membership management.
- We implemented a CLI for interacting with the cluster.
- We implemented storage engine using [Bitcask](https://arpitbhayani.me/blogs/bitcask)

It is set up as a Rust workspace. Here is a brief overview of the structure:

```
client/       # cli client
kv_server/    # kv server (core server)
membership/   # membership service (interface for kv server)
memberlist/   # gossip protocol
shared/       # shared code between client and server
protocol/     # internal APIs with gRPC
```

## Building

### Dependencies

- Rust 1.71.0
- Protobuf Compiler (for gRPC)
- Docker

### Compiling Locally

```sh
cargo build             # for building all artifacts with debug info
cargo build --release   # for building all artifacts tweaked for max performance
```

### Building Docker Images

Run our build script `docker_build.sh` to build all docker images.

## Deploy

### Local Deployment

We provide a Docker Compose file `docker-compose.yaml` for running our project
locally. It will start a cluster of 5 kv servers and 1 client and the networking
is setup accordingly. You can attach to the client via `docker attach client`
and then use the REPL to interact with the cluster.

### Kubernetes Deployment

We provide a Kubernetes deployment file `kv.yaml` for running our project on a Kubernetes cluster. It will start a cluster of 5 kv servers.
To start the cluster, run the following command:

```sh
kubectl apply -f kv.yaml
```
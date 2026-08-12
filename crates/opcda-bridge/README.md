# opcda-bridge

Reusable async Rust client for the [`opcda-bridge`](https://github.com/bytehound-labs/opcda-bridge)
gateway's gRPC API. It provides typed list-servers, browse, read, and write operations without
bringing in the command-line client's presentation dependencies.

```toml
[dependencies]
opcda-bridge = "0.2"
```

See the crate documentation for the API and the repository README for gateway setup and protocol
details.

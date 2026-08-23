# opcda-bridge-proto

Generated Rust gRPC protocol types for the
[`opcda-bridge`](https://github.com/bytehound-labs/opcda-bridge) gateway and client. Most
applications should use the higher-level `opcda-bridge` client library instead.

```toml
[dependencies]
opcda-bridge-proto = "0.3"
```

The protocol is generated from the crate's bundled `bridge.proto` definition. Browse is a unary,
one-level page operation with opaque session and continuation tokens; namespace search is a
separate progressive streaming operation.

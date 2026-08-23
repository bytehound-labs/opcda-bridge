# opcda-bridge-proto

Generated Rust gRPC protocol types for the
[`opcda-bridge`](https://github.com/bytehound-labs/opcda-bridge) gateway and client. Most
applications should use the higher-level `opcda-bridge` client library instead.

```toml
[dependencies]
opcda-bridge-proto = "0.4"
```

The protocol is generated from the crate's bundled `bridge.proto` definition. Browse is a unary,
one-level page operation with opaque session and continuation tokens; live namespace search is a
separate progressive streaming operation. Persistent indexed discovery uses unary
`GetSearchIndexStatus`, `RefreshSearchIndex`, `ControlSearchIndex`, and `SearchIndex` operations.
Indexed matches contain exact ItemIDs and breadcrumb labels but never session-bound browse node
keys.

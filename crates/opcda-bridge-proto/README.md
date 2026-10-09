# opcda-bridge-proto

Generated Rust gRPC protocol types for the
[`opcda-bridge`](https://github.com/bytehound-labs/opcda-bridge) gateway and client. Most
applications should use the higher-level `opcda-bridge` client library instead.

```toml
[dependencies]
opcda-bridge-proto = "0.6"
```

The protocol crate has its own independent release version. Client and gateway package versions
may differ; interoperability is determined by the generated wire contract and capability versions.

`GetGatewayInfo` returns the gateway package version and protocol-feature ranges without contacting
an OPC DA server. Clients use those ranges for compatibility negotiation; `GetCapabilities`
continues to provide per-server operational details and remains available for legacy fallback.

The protocol is generated from the crate's bundled `bridge.proto` definition. Browse is a unary,
one-level page operation with opaque session and continuation tokens; live namespace search is a
separate progressive streaming operation. Persistent indexed discovery uses unary
`GetSearchIndexStatus`, `RefreshSearchIndex`, `ControlSearchIndex`, and `SearchIndex` operations.
The first refresh validates and enrolls a registered ProgID. `ControlSearchIndex` pauses,
resumes, or cancels a build, or deletes an enrolled index. Usable enrolled indexes automatically
participate under gateway policy; cancellation defers the next automatic attempt.
Indexed-search protocol 3 reserves retired status field 3 (`configured`) and control values 4/5
(enable/disable auto-refresh). They are not valid operations or reusable wire numbers.
The optional `IndexSchedulerDiagnostics.auto_refresh_policy` field
reports gateway configuration as allowed, disabled, or paused. Absence is unknown; it does not
mean the administrative policy is disabled.
Indexed matches contain exact ItemIDs and breadcrumb labels but never session-bound browse node
keys.

The `TagValue.value` field is semantic text. For OPC DA `VT_BSTR` values, it contains the exact
BSTR contents; quote characters are data only when present in the BSTR itself.

# Architecture

`opcda-bridge` separates Windows-only OPC DA access from the clients that use it. The gateway
communicates with local OPC DA servers through COM/DCOM; clients use the gateway's gRPC protocol
from Windows, Linux, or macOS.

## Components

| Crate | Role | Platform |
| --- | --- | --- |
| `opcda-bridge-proto` | Protobuf schema, generated RPC types, and the compatibility catalog | Cross-platform |
| `opcda-bridge` | Typed Rust client library for discovery, capabilities, browse, search, read, and write | Cross-platform |
| `opcda-bridge-client` | Command-line client built on the reusable library | Cross-platform |
| `opcda-bridge-gateway` | gRPC service, native OPC DA adapter, and persistent namespace index | Windows |

The reusable library has no command-line presentation layer. Rust applications can use its typed
API directly; scripts and other languages can use the CLI's JSON output or generate gRPC stubs from
the protocol schema.

## Request and data flow

```text
opcda-bridge-client or another gRPC client
                  |
          gRPC over HTTP/2
                  |
     Windows opcda-bridge-gateway
          |                 |
       COM/DCOM       SQLite namespace index
          |
       OPC DA server
```

The gateway exposes server discovery, capabilities, browse, live search, read, write, and index
operations. Browse sessions and continuation state stay on the gateway: clients pass opaque
session, node, and page tokens back unchanged. A browsed node's display label is separate from its
exact OPC ItemID, which remains the identifier for reads and writes.

## Gateway server modules

The gateway crate keeps its public server entry point in
`crates/opcda-bridge-gateway/src/server/mod.rs`, which re-exports
`opcda_bridge_gateway::server::BridgeService`. The implementation is divided into:

- `service.rs` for service construction and tonic RPC handlers.
- `map.rs` for protocol and error conversions.
- `search.rs` for bounded live namespace-search traversal.
- `tests.rs` for service-level behavior tests.

These are internal boundaries; the gRPC protocol, public `BridgeService` path, and handler behavior
remain defined by the gateway contract.

The optional SQLite index stores namespace metadata such as exact ItemIDs, display names, node
kinds, and breadcrumbs. It does not store live tag values. Indexed search operates on a completed
generation and is separate from live browse, read, and write paths.

## Boundaries and scope

- The gateway is the only Windows/COM component. The reusable library, CLI, and protocol types are
  cross-platform.
- The supported gateway artifact is 32-bit x86 (`i686-pc-windows-msvc`), including on 64-bit
  Windows. This matches legacy 32-bit OPC DA/COM registrations.
- The product bridges OPC DA; it does not implement OPC UA, Modbus, or a generic protocol
  multiplexer.
- The native OPC DA adapter uses the open-source `bytehound-opc-da-client` crate rather than a
  proprietary OPC SDK.
- The gRPC transport is plaintext and does not authenticate callers. Deployment and network
  restrictions are part of the security boundary; see
  [Gateway deployment, service, and firewall](gateway-deployment.md) and the
  [security policy](../SECURITY.md).

## Related references

- [Gateway deployment, service, and firewall](gateway-deployment.md)
- [Indexing and search operations](indexing-and-search.md)
- [Protocol and compatibility](protocol-and-compatibility.md)
- [Reusable Rust client](../crates/opcda-bridge/README.md)
- [Command-line client](../crates/opcda-bridge-client/README.md)

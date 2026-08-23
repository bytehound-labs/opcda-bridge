# opcda-bridge-gateway

Windows gateway that exposes native OPC DA (COM/DCOM) servers over the network to
[`opcda-bridge-client`](https://github.com/bytehound-labs/opcda-bridge) and other gRPC clients.

Install it with Cargo on a Windows host:

```sh
cargo install opcda-bridge-gateway
```

The gateway must run on the Windows machine hosting the OPC DA server. See the repository README
for service installation, configuration, and firewall setup.

Tag browsing uses native one-level OPC DA enumeration with bounded pages. The gateway owns opaque
browse sessions and continuation tokens, preserves exact ItemIDs separately from display names,
and reports whether a page is complete. Namespace search is a bounded progressive operation that
can be cancelled by dropping the client stream.

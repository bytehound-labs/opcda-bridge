# opcda-bridge-gateway

Windows gateway that exposes native OPC DA (COM/DCOM) servers over the network to
[`opcda-bridge-client`](https://github.com/bytehound-labs/opcda-bridge) and other gRPC clients.

Install it with Cargo on a Windows host:

```sh
cargo install opcda-bridge-gateway
```

The gateway must run on the Windows machine hosting the OPC DA server. See the repository README
for service installation, configuration, and firewall setup.

Tag browsing uses recursive branch/leaf enumeration for hierarchical OPC DA servers. The gateway
preserves dotted and slash-separated item IDs so clients can expand namespaces such as
`FCS0201/Control/PV` one level at a time.

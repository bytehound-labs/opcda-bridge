# Security Policy

## Supported versions

Security fixes are provided for the latest published release line of each crate and for `main`.
Fixes land on `main` and ship in a new release of each affected crate; earlier patch releases and
release lines do not receive backports.

| Component | Crate                  | Supported              |
| --------- | ---------------------- | ---------------------- |
| Library   | `opcda-bridge`         | Latest `0.5.x` release |
| CLI       | `opcda-bridge-client`  | Latest `0.5.x` release |
| Protocol  | `opcda-bridge-proto`   | Latest `0.5.x` release |
| Gateway   | `opcda-bridge-gateway` | Latest `0.5.x` release |
| Source    | `main` branch          | Yes                    |

Each crate is versioned and released independently; see
[Versions and compatibility](README.md#versions-and-compatibility) for how client and gateway
versions relate.

## Reporting a vulnerability

Do not report security vulnerabilities through public GitHub issues, pull requests, or other
public channels.

Report them privately instead:

- **GitHub private vulnerability reporting (preferred)** — open the repository's
  [Security tab](https://github.com/bytehound-labs/opcda-bridge/security) and select
  **Report a vulnerability**, or go directly to
  <https://github.com/bytehound-labs/opcda-bridge/security/advisories/new>.
- **Email** — [info@bytehound.ca](mailto:info@bytehound.ca).

Include as much of the following as you can:

- the affected component and version or commit;
- how it was installed (release archive, `cargo install`, source build, or AUR package) and, for
  the gateway, whether it runs as a Windows service;
- the Windows version, OPC DA server, and client platform involved;
- reproduction steps or a proof of concept; and
- the impact you observed or expect.

Reports are handled on a best-effort basis; there are no guaranteed response or fix timelines.
Confirmed vulnerabilities are fixed in a coordinated release and disclosed through a GitHub
security advisory. Please keep details private until a fix has been published.

## Security model

`opcda-bridge-gateway` exposes the OPC DA servers on its Windows host to the network. Understand
the following properties before deploying it:

- **No authentication or authorization.** Any client that can reach the gateway's port can call
  every RPC: list servers, browse, search, read and write OPC DA tags, and control persistent
  search indexes (refresh, pause, resume, cancel, delete, and automatic-refresh settings). Writes
  are limited only by what the OPC DA server allows the gateway's Windows account to do.
- **No encryption.** The gateway serves plaintext gRPC over HTTP/2. Requests, responses, tag
  values, and written values cross the network unencrypted.
- **Listens on all interfaces.** The gateway binds `0.0.0.0` (all IPv4 interfaces); its TCP port
  defaults to `7600` and is configurable with `--port`, `OPC_BRIDGE_PORT`, or `port` in the
  configuration file. The bind address is not configurable.
- **Runs as LocalSystem as a service.** `opcda-bridge-gateway install` registers the
  automatically starting `OpcdaBridgeGateway` Windows service, which runs as `LocalSystem` and
  launches the executable path and any `--config`, `--port`, and log flags captured at install
  time.
- **Clients trust the gateway.** The `opcda-bridge` library and the `opcda-bridge-client` CLI
  (which connects to `localhost:7600` unless configured otherwise) do not authenticate the
  gateway, so a spoofed or compromised gateway, or anyone able to intercept the connection, can
  return arbitrary data.
- **Local data.** The configuration file and `logs` directory sit next to the gateway executable
  by default, and the persistent search index is a SQLite database (by default
  `%PROGRAMDATA%\opcda-bridge\index.sqlite3`) with build lock and owner files beside it. Logs and
  the index can reveal the structure of the OPC namespace.

### Deployment recommendations

- Run the gateway only on trusted, segmented OT networks. Never expose its port to the Internet
  or to untrusted networks.
- Neither `opcda-bridge-gateway install` nor the release archives create firewall rules. Add an
  inbound Windows Firewall rule for the gateway's port that is restricted to the specific client
  hosts that need access.
- Keep the gateway executable, configuration file, log directory, and index directory writable
  only by administrators, especially when the gateway runs as `LocalSystem`.
- Grant the gateway's account least-privilege access in the OPC DA server, and use server-side
  permissions to restrict which tags can be written.
- Verify release downloads against the published SHA-256 checksums, keyless Sigstore signatures,
  and GitHub artifact provenance attestations.

## Scope

Examples of issues in scope:

- Memory-safety bugs in the COM/FFI layer, including the gateway's Win32 calls and the
  [`bytehound-opc-da-client`](https://github.com/bytehound-labs/opc-cli) OPC DA/COM crate it
  depends on. If you are unsure which repository an issue belongs to, report it here.
- Crashes, hangs, or resource exhaustion caused by malformed, oversized, or unexpected gRPC
  requests or streams.
- Unsafe handling of configuration, log, index, or lock-file paths.
- Privilege escalation through service installation, service launch arguments, or files the
  `LocalSystem` service reads or writes.
- Code execution or memory corruption in the library or CLI triggered by a malicious gateway
  response.
- Compromise of the release, packaging, or signing process.

The following are known, by-design limitations rather than vulnerabilities on their own:

- the absence of authentication, authorization, and TLS;
- binding to all network interfaces;
- running as `LocalSystem` when installed as a service; and
- any reachable client being able to read and write tags the OPC DA server permits and to manage
  search indexes.

Proposals to harden these areas, such as authentication, TLS, a configurable bind address, or a
read-only mode, are welcome as feature requests.

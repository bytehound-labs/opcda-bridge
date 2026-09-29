# Troubleshooting

Use read-only checks first. The gateway can write to live OPC DA tags, and its network API has no
authentication or encryption. Do not expose it to an untrusted network while diagnosing a
connection problem.

## Client cannot connect

Check the gateway process and service state on the Windows OPC DA host:

```powershell
.\opcda-bridge-gateway.exe status
```

Confirm the client uses the gateway's reachable address and TCP port. The default is
`0.0.0.0:7600`; the service and the release archive do not add an inbound Windows Firewall rule.
Restrict any firewall rule to the client addresses that need access. See
[Gateway deployment, service, and firewall](gateway-deployment.md).

## The server ProgID is unknown

List the ProgIDs registered on the gateway host, then use an exact returned value:

```sh
opcda-bridge-client --host 192.168.1.50:7600 servers
opcda-bridge-client --host 192.168.1.50:7600 capabilities \
  --server Kepware.KepServerEX.V5
```

The client does not guess a default server. `capabilities`, `browse`, `search`, `read`, and
`write` fail if no server is supplied by the command or client configuration.

## Client and gateway disagree about features

Check negotiated features rather than comparing package version strings:

```sh
opcda-bridge-client --host 192.168.1.50:7600 compatibility
opcda-bridge-client --host 192.168.1.50:7600 compatibility \
  --require namespace --require indexed-search
```

Use `--server PROGID` only for legacy gateways that do not implement the gateway-wide protocol
handshake. See [Protocol and compatibility](protocol-and-compatibility.md).

## Browse or search results look incomplete

`browse` returns one bounded page by default. Continue with the returned opaque session, node, and
page-token values; do not reconstruct the namespace from punctuation in an ItemID. `browse --all`
is an explicit bulk operation and stops at its configured result cap. Close a session after use:

```sh
opcda-bridge-client --host 192.168.1.50:7600 close-browse-session SESSION
```

Live `search` can stop at a result or visit cap and report truncation. A persistent
`index-search` no-match result is definitive only when the index is complete. Check
`index-status`; `partial`, `refreshing`, or `failed` does not establish that the entire server
namespace was indexed.

## An index does not start or remains paused

Indexing is opt-in by server. Run `index-refresh` with the exact ProgID returned by server
discovery to enroll it and start its first build. Automatic indexing does not perform a first
build. Check `index-status` and the gateway's `[index]` configuration, including `enabled`,
`paused`, maintenance windows, and per-server automatic-refresh state. A configured `paused = true`
prevents inventory from starting until indexing is resumed.

## An index build fails or SQLite reports a lock

Read status and the structured gateway logs before retrying. Logs include process ID, resolved
database path, server, generation, operation, and terminal build outcome. Verify that every
gateway process has its intended database path and that no two processes are indexing the same
SQLite file.

Build progress, failure updates, and cleanup share a database-wide writer gate. Cleanup defers
while builds are active and runs in bounded batches. If lock errors recur, preserve the active
database and its `-wal`/`-shm` sidecars, record the exact process command lines and database paths,
and stop additional refresh attempts until the contention is understood. Do not remove
`.build.lock` or `.build.owner` while any gateway process may still be running. The advisory lock,
not the presence of the owner sidecar, determines whether a build is active.

A failed or cancelled refresh does not replace a complete active generation. Continue using that
generation for indexed search while the failure is investigated.

## A namespace branch is skipped

OPC DA servers can expose malformed or non-navigable branch names. The gateway validates DA2
branches before queueing them; branch-only names rejected by native navigation with
`E_INVALIDARG` are skipped and included in the completion warning. Names that resolve to exact
items remain selectable. A warning does not mean the active generation is failed, but it must be
reviewed before accepting a full namespace inventory.

When a DA3 root browse returns `RPC_X_NULL_REF_POINTER` or `E_NOTIMPL` and the server also supports
DA2, the gateway retries through DA2 and reports the compatibility fallback. It does not use
server-specific blacklists. Yokogawa branch names such as the `SCS0130` case containing `U+0001`
are handled by the same generic DA2 malformed-branch classification.

## Logs and persistent files

Gateway logs are rolling files under `logs/` beside the executable by default. A Windows service
has no console, so inspect the configured log directory. The namespace index is a SQLite database
in the platform data directory unless `index.database_path` overrides it. Preserve its WAL and
shared-memory sidecars with the database during a backup or recovery investigation.

For full acceptance criteria and safe canary sequencing, see
[Indexing and search operations](indexing-and-search.md).

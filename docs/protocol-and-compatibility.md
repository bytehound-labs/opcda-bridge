# Protocol and compatibility

Client and gateway package versions are independent. Runtime compatibility depends on the
protocol-feature ranges and capabilities advertised by the gateway, not on equal crate or binary
version numbers.

## Check a deployed pair

The compatibility command uses the gateway-wide `GetGatewayInfo` handshake and does not contact
an OPC DA server when the gateway supports it:

```sh
opcda-bridge-client --host 192.168.1.50:7600 compatibility
opcda-bridge-client --host 192.168.1.50:7600 compatibility \
  --require namespace --require indexed-search
```

`--require` makes a deployment requirement explicit. For a gateway that predates
`GetGatewayInfo`, provide `--server` so the client can infer compatibility from the legacy
per-server `GetCapabilities` response. The report distinguishes the client binary version from
the reusable library version implementing its protocol contract.

Compatibility results have these meanings:

| Result         | Meaning                                                                           |
| -------------- | --------------------------------------------------------------------------------- |
| `full`         | All advertised and required features overlap                                      |
| `partial`      | Core read/write operations overlap, but an optional feature does not              |
| `incompatible` | Core compatibility or a required feature is unavailable                           |
| `unknown`      | The gateway cannot describe its protocol                                          |
| `unverified`   | Protocol ranges overlap, but the exact package pair has no recorded test evidence |

Overlapping but unverified pairs remain usable. Optional features can be unsupported while core
read/write operations remain compatible.

## Protocol feature lines

The canonical catalog is
[`crates/opcda-bridge-proto/compatibility.toml`](../crates/opcda-bridge-proto/compatibility.toml).
It defines the supported ranges for:

| Feature        | Contract | Operations                                                                     |
| -------------- | -------: | ------------------------------------------------------------------------------ |
| Core           |        1 | Server discovery, reads, and writes                                            |
| Namespace      |        2 | Capabilities, paged browse, browse sessions, and live search                   |
| Indexed search |        2 | Durable on-demand index enrollment, search, and per-server scheduling controls |

The generated [compatibility report](../COMPATIBILITY.md) and
[machine-readable compatibility catalog](../compatibility.json) are derived from that source.
Regenerate them with `python3 scripts/generate-compatibility-report.py`; do not hand-edit either
generated output.

The catalog's release lines are:

| Release line        | Package versions        | Protocol boundary                     |
| ------------------- | ----------------------- | ------------------------------------- |
| `legacy`            | 0.1.0 through 0.3.1     | Core 1, original streaming browse     |
| `paged`             | 0.3.2 through 0.3.999   | Core 1 and namespace 2                |
| `indexed`           | 0.4.0 through 0.4.999   | Core 1, namespace 2, indexed search 1 |
| `indexed-on-demand` | 0.5.0 through 0.999.999 | Core 1, namespace 2, indexed search 2 |

The 0.5 indexed-search boundary changes index lifecycle semantics to durable on-demand enrollment
and per-server scheduling controls. Protobuf additions can be wire-compatible while introducing a
new negotiated feature boundary. Older client/gateway pairs can continue using overlapping core
and namespace operations without assuming that indexed-search lifecycle features exist.

## Browse and search contract

Browse returns one bounded page of immediate children. Clients pass opaque session IDs, node keys,
and continuation tokens unchanged. Display names are not ItemIDs: only selectable item and
branch-and-item nodes expose the exact ItemID used for read/write. Flat namespaces remain flat
rather than being reconstructed into a guessed hierarchy.

Live `search` traverses the OPC namespace and streams progressive matches and progress. Result or
visit caps can end a stream with a truncation warning. `index-search` queries the gateway's
persistent index, returns exact ItemIDs and breadcrumbs, and does not fall back to a live
traversal. Browse output reports completeness and continuation metadata so partial pages are not
mistaken for a complete inventory.

For JSON output, `--output json` and `--json` are equivalent. Most commands emit pretty-printed
JSON; browse emits session, page, organization, completeness, and warning metadata. Search emits
newline-delimited JSON events to preserve incremental progress. Structured command errors are
written to stderr, and the process exits nonzero. A tag's semantic value is returned as text;
OPC DA `VT_BSTR` contents are preserved without display quoting.

## Versioning and protocol changes

All four workspace crates publish to crates.io. Each has its own version; GitHub Releases and
binary archives are produced for the client and gateway. A client release does not need a matching
gateway release.

The three published Rust libraries are pre-1.0. Adding a public Rust struct field can break
downstream struct literals even when the Protobuf wire change is additive. Such a public API break
requires the affected crate's next minor version and its workspace dependency requirement to be
updated. The semver check compares library APIs with their latest crates.io versions.

An intentional Protobuf wire break requires the `breaking-protobuf` label, a catalog boundary,
updated compatibility evidence, and regenerated reports. Preserve published package names,
Protobuf packages, services, RPCs, fields, field numbers, and enum values. Do not change the wire
contract solely to satisfy a linter.

The gateway migrates older indexed-search SQLite databases transactionally through schema 2, 3,
and 4. Existing generations and full-text data are preserved; only usable active generations are
enabled for scheduled refresh automatically.

## Related references

- [Architecture](architecture.md)
- [Indexing and search operations](indexing-and-search.md)
- [Release and package verification](release.md)
- [Compatibility report](../COMPATIBILITY.md)

# opcda-bridge-client

Cross-platform command-line client for an
[`opcda-bridge`](https://github.com/bytehound-labs/opcda-bridge) gateway.

Install it with Cargo:

```sh
cargo install opcda-bridge-client
```

Read output contains semantic values. An OPC DA `VT_BSTR` is returned exactly as supplied by the
server, so a BSTR containing `AUT` produces `"value": "AUT"` rather than a display-quoted value.

Browse one bounded page of immediate children:

```sh
opcda-bridge-client browse --server Kepware.KepServerEX.V5 --page-size 200
```

Use the returned opaque `session_id`, `node_key`, and `next_page_token` to expand a branch or load
the next page:

```sh
opcda-bridge-client browse --server Kepware.KepServerEX.V5 \
  --session-id SESSION --parent-node-key NODE_KEY
opcda-bridge-client browse --server Kepware.KepServerEX.V5 \
  --session-id SESSION --page-token PAGE_TOKEN
```

`browse --all` is explicit and stops at `--max-results` (10,000 by default).
The `search` command performs compatibility-oriented live traversal and streams exact, prefix, or
contains matches:

```sh
opcda-bridge-client search Device1 --server Kepware.KepServerEX.V5 --match-mode contains
```

Search progress is written to stderr. JSON search output is newline-delimited so matches and
progress remain incremental, and pressing Ctrl+C drops the active gRPC stream.

For interactive discovery, query the gateway-owned persistent index:

```sh
opcda-bridge-client index-status --server Kepware.KepServerEX.V5
opcda-bridge-client index-search Device1 --server Kepware.KepServerEX.V5
opcda-bridge-client index-refresh --server Kepware.KepServerEX.V5
opcda-bridge-client index-pause --server Kepware.KepServerEX.V5
opcda-bridge-client index-resume --server Kepware.KepServerEX.V5
opcda-bridge-client index-cancel --server Kepware.KepServerEX.V5
```

`index-search` defaults to 50 ranked matches, never falls back to live traversal, and reports
`not-indexed`, `partial`, `ready`, `stale`, `refreshing`, or `failed` status with every result set.
Its JSON output is one object containing `matches`, `has_more`, and `status`; indexed matches
contain exact ItemIDs and breadcrumb labels, never browse-session node keys.

Use `opcda-bridge-client --help` for all commands. Prebuilt platform binaries are available from
the repository releases page.

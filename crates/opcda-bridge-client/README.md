# opcda-bridge-client

Cross-platform command-line client for an
[`opcda-bridge`](https://github.com/bytehound-labs/opcda-bridge) gateway.

Install it with Cargo:

```sh
cargo install opcda-bridge-client
```

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
Namespace search is separate and streams exact, prefix, or contains matches:

```sh
opcda-bridge-client search Device1 --server Kepware.KepServerEX.V5 --match-mode contains
```

Search progress is written to stderr. JSON search output is newline-delimited so matches and
progress remain incremental, and pressing Ctrl+C drops the active gRPC stream.

Use `opcda-bridge-client --help` for capabilities, session close, browse, search, read, and write
commands. Prebuilt platform binaries are available from the repository releases page.

# Client/gateway compatibility

Package versions are independent. Runtime compatibility is negotiated by
protocol feature and advertised capability, not by matching client and
gateway package versions.

The `opcda-bridge-client compatibility` command checks a deployed pair
without contacting GitHub or crates.io. It reports the gateway package
version, protocol ranges, negotiated features, and whether the exact
pair has test evidence. It distinguishes the client binary version from
the reusable-library version implementing its protocol contract.

## Protocol features

| Feature        | Current contract | Meaning                                               |
| -------------- | ---------------: | ----------------------------------------------------- |
| Core           |                1 | Server discovery, reads, and writes                   |
| Namespace      |                2 | Capabilities, paged browse, sessions, and live search |
| Indexed search |                2 | Durable on-demand namespace index operations          |

## Release lines

| Release line      | Package versions  | Status    | Core | Namespace | Indexed search | Notes                                                                       |
| ----------------- | ----------------- | --------- | ---: | --------: | -------------: | --------------------------------------------------------------------------- |
| legacy            | 0.1.0 - 0.3.1     | legacy    |    1 |         1 |              0 | Original streaming browse contract.                                         |
| paged             | 0.3.2 - 0.3.999   | supported |    1 |         2 |              0 | Capabilities, paged browse, browse sessions, and live search.               |
| indexed           | 0.4.0 - 0.4.999   | supported |    1 |         2 |              1 | Adds persistent namespace indexing.                                         |
| indexed-on-demand | 0.5.0 - 0.999.999 | supported |    1 |         2 |              2 | Adds durable on-demand index enrollment and per-server scheduling controls. |

A pair whose required protocol ranges overlap is usable even when its
exact package versions have not been exercised together. Such a pair is
reported as `unverified`, not rejected. Optional features may be
`unsupported` while core read/write compatibility remains available.

## Evidence

| Client line       | Client version | Gateway line      | Gateway version | Evidence                 | Notes                                                                                  |
| ----------------- | -------------- | ----------------- | --------------- | ------------------------ | -------------------------------------------------------------------------------------- |
| indexed           | -              | paged             | -               | contract-boundary-tested | The current client negotiates the paged namespace contract with a 0.3.2 gateway.       |
| paged             | -              | indexed           | -               | contract-boundary-tested | A 0.3.2 client reads, writes, and browses through the current gateway.                 |
| indexed           | 0.4.0          | indexed           | 0.4.3           | exact-pair-tested        | The 0.4.0 indexed client reaches the current indexed gateway contract.                 |
| indexed           | 0.4.3          | indexed           | 0.4.3           | exact-pair-tested        | The current client and gateway are exercised together.                                 |
| indexed-on-demand | -              | indexed-on-demand | -               | contract-boundary-tested | The current client and gateway exercise durable on-demand namespace indexing together. |

An intentional feature-contract change can create a new protocol boundary
even when the protobuf encoding remains wire-additive. The 0.5 indexed-search
boundary changes the lifecycle from configured-server behavior to durable
on-demand enrollment and per-server scheduling controls. The affected
protocol crate, reusable library, client, and gateway release independently as
needed, while the compatibility catalog and cross-version evidence are updated
together.

The gateway also migrates the previous indexed-search SQLite schema in place
when opening it with the 0.5 lifecycle. Schema 2 databases are upgraded
through schema 3 before the schema 4 enrollment migration. Existing
generations and full-text entries are preserved; only servers with an active
usable generation are enabled for scheduled refresh automatically. Failed-only
histories remain enrolled but unscheduled until an operator retries them
manually. Each migration step is transactional; if an upgrade fails, the
gateway surfaces the error without retaining a partially applied migration.

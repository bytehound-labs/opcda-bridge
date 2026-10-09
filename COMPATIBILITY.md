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

| Feature | Current contract | Meaning |
| --- | ---: | --- |
| Core | 1 | Server discovery, reads, and writes |
| Namespace | 2 | Capabilities, paged browse, sessions, and live search |
| Indexed search | 3 | Durable on-demand namespace index operations |

## Release lines

| Release line | Package versions | Status | Core | Namespace | Indexed search | Notes |
| --- | --- | --- | ---: | ---: | ---: | --- |
| legacy | 0.1.0 - 0.3.1 | legacy | 1 | 1 | 0 | Original streaming browse contract. |
| paged | 0.3.2 - 0.3.999 | supported | 1 | 2 | 0 | Capabilities, paged browse, browse sessions, and live search. |
| indexed | 0.4.0 - 0.4.999 | supported | 1 | 2 | 1 | Adds persistent namespace indexing. |
| indexed-on-demand | 0.5.0 - 0.5.999 | supported | 1 | 2 | 2 | Adds durable on-demand index enrollment and per-server scheduling controls. |
| indexed-always-on | 0.6.0 - 0.999.999 | supported | 1 | 2 | 3 | Usable enrolled indexes always participate under gateway policy. Per-server opt-in/out is removed; operator cancellation defers the next automatic attempt. |

A pair whose required protocol ranges overlap is usable even when its
exact package versions have not been exercised together. Such a pair is
reported as `unverified`, not rejected. Optional features may be
`unsupported` while core read/write compatibility remains available.

## Evidence

| Client line | Client version | Gateway line | Gateway version | Evidence | Notes |
| --- | --- | --- | --- | --- | --- |
| indexed | - | paged | - | contract-boundary-tested | The current client negotiates the paged namespace contract with a 0.3.2 gateway. |
| paged | - | indexed | - | contract-boundary-tested | A 0.3.2 client reads, writes, and browses through the current gateway. |
| indexed | 0.4.0 | indexed | 0.4.3 | exact-pair-tested | The 0.4.0 indexed client reaches the current indexed gateway contract. |
| indexed | 0.4.3 | indexed | 0.4.3 | exact-pair-tested | The current client and gateway are exercised together. |
| indexed-on-demand | - | indexed-on-demand | - | contract-boundary-tested | The current client and gateway exercise durable on-demand namespace indexing together. |
| indexed-always-on | - | indexed-always-on | - | contract-boundary-tested | The current client and gateway exercise always-participating indexed search, retained administrative policy, and cancellation/deletion lifecycle behavior. |
| indexed-on-demand | - | indexed-always-on | - | contract-boundary-tested | Core and paged namespace operations remain compatible; indexed-search lifecycle versions do not overlap and retired opt-in/out actions are rejected. |

An intentional feature-contract change can create a new protocol boundary
even when the protobuf encoding remains wire-additive. The 0.5 indexed-search
boundary changes the lifecycle from configured-server behavior to durable
on-demand enrollment and per-server scheduling controls. The affected
protocol crate, reusable library, client, and gateway release independently as
needed, while the compatibility catalog and cross-version evidence are updated
together.

The 0.6 indexed-search protocol 3 boundary removes per-server opt-in/out.
Retired status field 3 and control values 4/5, including their names, are
reserved. Usable enrolled indexes participate under gateway policy;
cancellation defers automatic work and deletion requires manual recreation.
Core 1 and namespace 2 remain unchanged.

The gateway also migrates the previous indexed-search SQLite schema in place
through schema 2, 3, 4, and 5. Schema 5 removes retired enrollment preference
columns without rebuilding namespace tables. Existing generations and
full-text entries are preserved; all usable active indexes participate in
scheduled refresh, including previously opted-out caches. Failed-only
histories remain enrolled but unscheduled until an operator retries them
manually. Each migration step is transactional; if an upgrade fails, the
gateway surfaces the error without retaining a partially applied migration.
An older binary requires a pre-migration database backup for rollback.

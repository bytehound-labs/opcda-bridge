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
| Indexed search | 1 | Persistent namespace index operations |

## Release lines

| Release line | Package versions | Status | Core | Namespace | Indexed search | Notes |
| --- | --- | --- | ---: | ---: | ---: | --- |
| legacy | 0.1.0 - 0.3.1 | legacy | 1 | 1 | 0 | Original streaming browse contract. |
| paged | 0.3.2 - 0.3.999 | supported | 1 | 2 | 0 | Capabilities, paged browse, browse sessions, and live search. |
| indexed | 0.4.0 - 0.999.999 | supported | 1 | 2 | 1 | Adds persistent namespace indexing. |

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

An intentional wire-contract break creates a new protocol boundary.
The affected protocol crate, reusable library, client, and gateway
release independently as needed, while the compatibility catalog and
cross-version evidence are updated together.

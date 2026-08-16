## What does this PR do?

<!-- Briefly describe the change and why it's needed. Link an issue if one exists. -->

## Checklist

- [ ] `cargo fmt --all --check` passes
- [ ] `cargo clippy --workspace --all-targets --all-features -- -D warnings` passes
- [ ] `cargo test --workspace` passes
- [ ] Documentation (`README.md`, `AGENTS.md`) updated if this changes user-visible behavior
- [ ] Hardware-in-the-loop changes (real OPC DA server/DCOM): manual verification steps noted
      above, since there's no live OPC DA server in CI

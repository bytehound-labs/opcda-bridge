## What does this PR do?

<!-- Briefly describe the change and why it's needed. Link an issue if one exists. -->

## Validation

<!-- List targeted checks and any manual verification performed. -->

## Checklist

- [ ] This change is on a feature branch and will be squash-merged through a pull request
- [ ] `cargo fmt --all --check` passes
- [ ] `cargo clippy --workspace --all-targets --all-features -- -D warnings` passes
- [ ] `cargo test --workspace` passes
- [ ] `cargo deny check` passes
- [ ] `cargo machete` passes
- [ ] `cargo test --manifest-path compatibility-tests/Cargo.toml --locked` passes when applicable
- [ ] `python3 scripts/generate-compatibility-report.py --check` passes when applicable
- [ ] Documentation (`README.md`, `AGENTS.md`, or `CONTRIBUTING.md`) updated if this changes
      user-visible behavior
- [ ] Applicable SonarQube analysis reports zero `OPEN`/`CONFIRMED` issues, or any remaining
      Accepted/False Positive finding has a documented rationale and related link
- [ ] Hardware-in-the-loop changes (real OPC DA server/DCOM): manual verification steps noted
      above, since there's no live OPC DA server in CI

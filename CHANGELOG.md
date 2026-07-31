# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.1](https://github.com/mikeboiko/opcda-bridge/compare/opcda-bridge-v0.1.0...opcda-bridge-v0.1.1) - 2026-07-31

### Fixed

- *(ci)* checkout repo before enabling auto-merge
- *(ci)* install protoc in the Release-plz workflow

## [0.1.0](https://github.com/mikeboiko/opcda-bridge/releases/tag/opcda-bridge-v0.1.0) - 2026-07-30

### Added

- scaffold workspace with CI/CD and tooling

### Fixed

- install protoc in coverage workflow
- use workspace-level publish=false in release-plz config
- disable crates.io publishing in release-plz config
- add GITHUB_TOKEN env var to release-plz action
- correct release-plz action reference

### Other

- add auto-merge workflow for release-plz PRs
- Merge branch 'feat/gateway' into main
- add code coverage workflow
- add CONTRIBUTING and copilot-instructions, expand README ([#1](https://github.com/mikeboiko/opcda-bridge/pull/1))
- Initial commit

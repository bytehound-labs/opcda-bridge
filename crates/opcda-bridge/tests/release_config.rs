use regex::Regex;
use std::fs;

const RELEASE_COMMITS: &[&str] = &[
    "chore(opcda-bridge-gateway): release v0.2.13 (#47)",
    "chore(opcda-bridge-client): release v0.1.4 (#19)",
    "chore: release v0.2.1 (#34)",
    "chore: release v0.2.0 (#27)",
    "chore: release (#25)",
    "chore: release",
];

const RELEASABLE_COMMITS: &[&str] = &[
    "feat: publish workspace crates",
    "feat(gateway): run as a Windows service (#12)",
    "fix(browse): support recursive hierarchical OPC DA tags",
    "fix(ci): stabilize release PR checks",
    "ci: harden security and release workflows",
    "docs: add cargo install --git option for the client (#20)",
    "revert: undo the thing",
    "perf(server): reduce allocations",
    "chore(deps): bump the cargo-dependencies group across 1 directory",
    "chore(deps-dev): bump prettier",
    "chore(github): update an action",
    "chore(ci): update validation",
];

#[test]
fn release_filter_excludes_release_plz_commits_and_keeps_releasable_commits() {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../release-plz.toml");
    let config = fs::read_to_string(path).unwrap();
    let document: toml::Value = toml::from_str(&config).unwrap();
    let pattern = document["workspace"]["release_commits"].as_str().unwrap();
    let regex = Regex::new(pattern).unwrap();

    for commit in RELEASE_COMMITS {
        assert!(
            !regex.is_match(commit),
            "release-plz commit unexpectedly matched: {commit}"
        );
    }

    for commit in RELEASABLE_COMMITS {
        assert!(
            regex.is_match(commit),
            "releasable commit unexpectedly did not match: {commit}"
        );
    }
}

use std::process::Command;

#[test]
fn test_client_help() {
    // Use Cargo's official env var rather than deriving the binary path from
    // `current_exe()`: the layout of test-binary output directories is a cargo/tool
    // implementation detail (it differs between plain `cargo test` and `cargo llvm-cov`,
    // and has changed across tool versions), so guessing at it is fragile.
    let client_bin = env!("CARGO_BIN_EXE_opcda-bridge-client");
    let output = Command::new(client_bin).arg("--help").output().unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("OPC DA bridge client"));
}

#[test]
fn test_browse_and_search_help_expose_scalable_options() {
    let client_bin = env!("CARGO_BIN_EXE_opcda-bridge-client");
    let browse = Command::new(client_bin)
        .args(["browse", "--help"])
        .output()
        .unwrap();
    assert!(browse.status.success());
    let browse_help = String::from_utf8_lossy(&browse.stdout);
    assert!(browse_help.contains("--page-size"));
    assert!(browse_help.contains("--all"));
    assert!(browse_help.contains("--parent-node-key"));

    let search = Command::new(client_bin)
        .args(["search", "--help"])
        .output()
        .unwrap();
    assert!(search.status.success());
    let search_help = String::from_utf8_lossy(&search.stdout);
    assert!(search_help.contains("--match-mode"));
    assert!(search_help.contains("--max-results"));
    assert!(search_help.contains("--include-branches"));

    let index_search = Command::new(client_bin)
        .args(["index-search", "--help"])
        .output()
        .unwrap();
    assert!(index_search.status.success());
    let index_search_help = String::from_utf8_lossy(&index_search.stdout);
    assert!(index_search_help.contains("--match-mode"));
    assert!(index_search_help.contains("--max-results"));
}

#[test]
fn test_client_run_exercises_full_run_path_on_connection_failure() {
    // `--help` above exits inside `Cli::parse()`, before `lib::run()` ever
    // calls into `run_with_cli`. Connecting to a closed local port instead
    // fails fast (connection refused) while still exercising the full
    // `run()` -> `run_with_cli()` -> `fail()` path in the real binary, with
    // a non-zero exit and a `--json`-formatted error on stderr.
    let client_bin = env!("CARGO_BIN_EXE_opcda-bridge-client");
    let output = Command::new(client_bin)
        .args(["--host", "127.0.0.1:1", "--json", "servers"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    let value: serde_json::Value = serde_json::from_str(stderr.trim()).unwrap();
    assert!(value["error"].is_string());
}

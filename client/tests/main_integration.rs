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

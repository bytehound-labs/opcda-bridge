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

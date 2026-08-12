#![cfg(not(target_os = "windows"))]

use std::process::Command;

#[test]
fn test_non_windows_exit() {
    // Use Cargo's official env var rather than deriving the binary path from
    // `current_exe()`: the layout of test-binary output directories is a cargo/tool
    // implementation detail (it differs between plain `cargo test` and `cargo llvm-cov`,
    // and has changed across tool versions), so guessing at it is fragile.
    let gateway_bin = env!("CARGO_BIN_EXE_opcda-bridge-gateway");
    let output = Command::new(gateway_bin).output().unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("Windows"));
}

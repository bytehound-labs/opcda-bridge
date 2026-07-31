use std::process::Command;

#[test]
fn test_client_help() {
    let test_exe = std::env::current_exe().unwrap();
    let target_dir = test_exe.parent().unwrap().parent().unwrap();
    let client_bin = target_dir.join("opcda-bridge-client");
    let output = Command::new(&client_bin).arg("--help").output().unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("OPC DA bridge client"));
}

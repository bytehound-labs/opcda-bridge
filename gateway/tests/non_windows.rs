use std::process::Command;

#[test]
#[cfg(not(target_os = "windows"))]
fn test_non_windows_exit() {
    let test_exe = std::env::current_exe().unwrap();
    let target_dir = test_exe.parent().unwrap().parent().unwrap();
    let gateway_bin = target_dir.join("opcda-bridge-gateway");
    let output = Command::new(&gateway_bin).output().unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("Windows"));
}

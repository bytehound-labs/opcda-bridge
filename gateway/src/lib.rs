pub mod opc;
pub mod run;
pub mod server;

#[cfg(test)]
mod test_support;

#[cfg(target_os = "windows")]
pub mod opc_da_adapter;

#[cfg(not(target_os = "windows"))]
pub fn non_windows_run() -> ! {
    eprintln!("opcda-bridge gateway requires Windows (COM/DCOM dependency)");
    std::process::exit(1);
}

pub mod browse;
pub mod config;
pub mod controller;
pub mod index;
pub mod logging;
pub mod opc;
pub mod run;
pub mod server;
pub mod service;

#[cfg(test)]
mod test_support;

#[cfg(target_os = "windows")]
pub mod opc_da_adapter;

#[cfg(not(target_os = "windows"))]
pub fn non_windows_run() -> ! {
    use clap::Parser;
    use config::{Cli, ServiceCommand};

    let cli = Cli::parse();
    if matches!(cli.command.as_ref(), Some(ServiceCommand::IndexPrepare)) {
        match run::prepare_index(cli) {
            Ok(()) => std::process::exit(0),
            Err(error) => {
                eprintln!("index preparation failed: {error:#}");
                std::process::exit(1);
            }
        }
    }
    eprintln!("opcda-bridge gateway requires Windows (COM/DCOM dependency)");
    std::process::exit(1);
}

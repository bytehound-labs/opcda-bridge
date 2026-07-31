#[cfg(target_os = "windows")]
use opcda_bridge_gateway::server;

#[cfg(target_os = "windows")]
fn main() -> anyhow::Result<()> {
    use clap::Parser;
    use opc_da_client::ComGuard;
    use opcda_bridge_gateway::config::{self, Cli};
    use opcda_bridge_gateway::logging;
    use opcda_bridge_gateway::run;
    use std::net::SocketAddr;

    let cli = Cli::parse();
    let config = config::load_config(cli.config.as_deref())?;
    let port = config::resolve_port(cli.port, &config);

    let exe = std::env::current_exe().expect("failed to resolve current executable path");
    let default_log_dir = logging::log_dir_from_exe(&exe);
    let log_settings = logging::resolve_log_settings(
        cli.log_level,
        cli.log_dir,
        cli.log_format,
        cli.log_rotation,
        &config.log,
        &default_log_dir,
    );
    // Hold the guard for the process lifetime: dropping it early would
    // silently truncate buffered log lines that haven't yet been flushed.
    let _log_guard = logging::init_tracing(&log_settings)?;

    let _guard = ComGuard::new().expect("COM initialization failed");
    Box::leak(Box::new(_guard));

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let bridge = server::BridgeService::default();

    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async move {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        tracing::info!(addr = %listener.local_addr()?, "opcda-bridge gateway listening");
        run::serve(listener, bridge, run::shutdown_signal()).await?;
        tracing::info!("opcda-bridge gateway shut down");
        Ok::<(), anyhow::Error>(())
    })?;
    Ok(())
}

#[cfg(not(target_os = "windows"))]
fn main() {
    opcda_bridge_gateway::non_windows_run();
}

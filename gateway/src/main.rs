#[cfg(target_os = "windows")]
use opcda_bridge_gateway::server;

#[cfg(target_os = "windows")]
fn init_tracing() {
    use tracing_subscriber::EnvFilter;

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
}

#[cfg(target_os = "windows")]
fn main() -> anyhow::Result<()> {
    use clap::Parser;
    use opc_da_client::ComGuard;
    use opcda_bridge_gateway::config::{self, Cli};
    use opcda_bridge_gateway::run;
    use std::net::SocketAddr;

    init_tracing();

    let cli = Cli::parse();
    let config = config::load_config(cli.config.as_deref())?;
    let port = config::resolve_port(cli.port, &config);

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

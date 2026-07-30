#[cfg(target_os = "windows")]
mod server;

#[cfg(target_os = "windows")]
fn main() -> anyhow::Result<()> {
    use opc_da_client::ComGuard;
    use std::net::SocketAddr;
    use tonic::transport::Server;

    let _guard = ComGuard::new().expect("COM initialization failed");
    Box::leak(Box::new(_guard));

    let port: u16 = std::env::var("OPC_BRIDGE_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(opcda_bridge::DEFAULT_BRIDGE_PORT);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let bridge = server::BridgeService::default();

    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async move {
        println!("opcda-bridge gateway listening on {}", addr);
        Server::builder()
            .add_service(bridge_proto::bridge::bridge_server::BridgeServer::new(
                bridge,
            ))
            .serve(addr)
            .await?;
        Ok::<(), anyhow::Error>(())
    })?;
    Ok(())
}

#[cfg(not(target_os = "windows"))]
fn main() {
    eprintln!("opcda-bridge gateway requires Windows (COM/DCOM dependency)");
    std::process::exit(1);
}

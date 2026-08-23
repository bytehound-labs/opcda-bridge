//! Server run loop with graceful shutdown.
//!
//! [`serve`] is generic over [`OpcClient`] (rather than tied to the
//! Windows-only COM adapter) so the part of the gateway that actually
//! implements graceful shutdown can be exercised by tests on any platform,
//! using the same mock client as `server`'s unit tests. [`run_gateway`]
//! builds on top of it with the full, Windows-only bootstrap (config,
//! logging, COM, bind) shared by both console mode and the Windows service
//! entry point in `service.rs`.

use crate::opc::OpcClient;
use crate::server::BridgeService;
use std::future::Future;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

/// Serves the `Bridge` gRPC service on `listener` until `shutdown`
/// resolves, then drains any in-flight requests before returning.
///
/// Taking an already-bound [`TcpListener`] (rather than binding an address
/// internally) keeps bind failures at the caller, where the real listen
/// address is known, and lets tests reserve an ephemeral port up front.
pub async fn serve<C: OpcClient>(
    listener: TcpListener,
    service: BridgeService<C>,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> anyhow::Result<()> {
    let incoming = TcpListenerStream::new(listener);
    Server::builder()
        .add_service(opcda_bridge_proto::bridge::bridge_server::BridgeServer::new(service))
        .serve_with_incoming_shutdown(incoming, shutdown)
        .await?;
    Ok(())
}

/// Resolves when the OS asks the process to stop: `Ctrl+C`, `Ctrl+Break`, a
/// console window close, a user logoff, or a system shutdown/restart.
///
/// Windows-only, like the rest of the gateway's runtime setup (see
/// `opc_da_adapter.rs`) — the gateway only ever ships for Windows, so this
/// thin binding to `tokio::signal::windows` has no cross-platform
/// counterpart to keep in sync.
#[cfg(target_os = "windows")]
pub async fn shutdown_signal() {
    let mut ctrl_c = tokio::signal::windows::ctrl_c().expect("failed to install Ctrl+C handler");
    let mut ctrl_break =
        tokio::signal::windows::ctrl_break().expect("failed to install Ctrl+Break handler");
    let mut ctrl_close =
        tokio::signal::windows::ctrl_close().expect("failed to install console close handler");
    let mut ctrl_shutdown =
        tokio::signal::windows::ctrl_shutdown().expect("failed to install system shutdown handler");

    tokio::select! {
        _ = ctrl_c.recv() => {}
        _ = ctrl_break.recv() => {}
        _ = ctrl_close.recv() => {}
        _ = ctrl_shutdown.recv() => {}
    }
}

/// Full gateway bootstrap: loads config/logging settings, initializes COM,
/// binds the listener, and serves until `shutdown` resolves. Shared by both
/// interactive (console) mode and the Windows service entry point
/// (`service::run_as_service`), so the two differ only in where their
/// `shutdown` future comes from and how the process's overall lifecycle
/// (plain `main` return vs. SCM status reporting) is handled around this
/// call — not in how the gateway itself starts up.
///
/// Windows-only, like the rest of the gateway's runtime setup: the real
/// `OpcDaAdapter`-backed `BridgeService::default()` only exists on Windows.
#[cfg(target_os = "windows")]
pub async fn run_gateway(
    cli: crate::config::Cli,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> anyhow::Result<()> {
    use std::net::SocketAddr;

    let config = crate::config::load_config(cli.config.as_deref())?;
    let port = crate::config::resolve_port(cli.port, &config);

    // This path selects the adjacent log directory, not a security-sensitive
    // executable or library.
    // nosemgrep: rust.lang.security.current-exe.current-exe
    let exe = std::env::current_exe().expect("failed to resolve current executable path");
    let default_log_dir = crate::logging::log_dir_from_exe(&exe);
    let log_settings = crate::logging::resolve_log_settings(
        cli.log_level,
        cli.log_dir,
        cli.log_format,
        cli.log_rotation,
        &config.log,
        &default_log_dir,
    );
    // Hold the guard for this call's lifetime: dropping it early would
    // silently truncate buffered log lines that haven't yet been flushed.
    let _log_guard = crate::logging::init_tracing(&log_settings)?;

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = TcpListener::bind(addr).await?;
    tracing::info!(addr = %listener.local_addr()?, "opcda-bridge gateway listening");

    let bridge = BridgeService::default();
    serve(listener, bridge, shutdown).await?;
    tracing::info!("opcda-bridge gateway shut down");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::MockOpcClient;
    use opcda_bridge_proto::bridge::ListServersRequest;
    use opcda_bridge_proto::bridge::bridge_client::BridgeClient;
    use std::time::Duration;
    use tokio::sync::oneshot;

    /// Binds an ephemeral localhost port and returns both the listener and
    /// its resolved address, ready to be handed to [`serve`].
    async fn bind_ephemeral() -> (TcpListener, String) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        (listener, format!("http://{addr}"))
    }

    #[tokio::test]
    async fn test_serve_accepts_requests_before_shutdown() {
        let (listener, addr) = bind_ephemeral().await;
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let service = BridgeService::new(MockOpcClient::default());

        let serve_task = tokio::spawn(serve(listener, service, async move {
            let _ = shutdown_rx.await;
        }));

        let mut client = BridgeClient::connect(addr).await.unwrap();
        let response = client
            .list_servers(ListServersRequest {
                host: String::new(),
            })
            .await
            .unwrap();
        assert!(response.into_inner().servers.is_empty());

        shutdown_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), serve_task)
            .await
            .expect("serve did not shut down in time")
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn test_serve_stops_accepting_new_connections_after_shutdown() {
        let (listener, addr) = bind_ephemeral().await;
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let service = BridgeService::new(MockOpcClient::default());

        let serve_task = tokio::spawn(serve(listener, service, async move {
            let _ = shutdown_rx.await;
        }));

        shutdown_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), serve_task)
            .await
            .expect("serve did not shut down in time")
            .unwrap()
            .unwrap();

        assert!(BridgeClient::connect(addr).await.is_err());
    }

    #[tokio::test]
    async fn test_serve_drains_in_flight_request_before_returning() {
        let (listener, addr) = bind_ephemeral().await;
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let mock = MockOpcClient::default();
        *mock.list_servers_result.lock().unwrap() = Ok(vec!["SlowServer".to_string()]);
        *mock.list_servers_delay.lock().unwrap() = Some(Duration::from_millis(100));
        let started = mock.list_servers_started.clone();
        let service = BridgeService::new(mock);

        let serve_task = tokio::spawn(serve(listener, service, async move {
            let _ = shutdown_rx.await;
        }));

        let mut client = BridgeClient::connect(addr).await.unwrap();
        let request_task = tokio::spawn(async move {
            client
                .list_servers(ListServersRequest {
                    host: String::new(),
                })
                .await
        });

        // Wait until the request has genuinely entered the handler (and is
        // now sleeping inside it) before firing shutdown, so this proves
        // real draining rather than a lucky scheduling race.
        started.notified().await;
        shutdown_tx.send(()).unwrap();

        let response = tokio::time::timeout(Duration::from_secs(5), request_task)
            .await
            .expect("in-flight request was not given time to complete")
            .unwrap()
            .expect("in-flight request was severed instead of drained");
        assert_eq!(
            response.into_inner().servers,
            vec!["SlowServer".to_string()]
        );

        tokio::time::timeout(Duration::from_secs(5), serve_task)
            .await
            .expect("serve did not shut down in time")
            .unwrap()
            .unwrap();
    }
}

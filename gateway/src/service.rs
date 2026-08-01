//! Windows service registration and lifecycle management.
//!
//! Only the imperative SCM (Service Control Manager) glue is
//! `#[cfg(target_os = "windows")]` — it is invisible to the Linux coverage
//! run, so it is kept as thin as possible. Everything that can be plain,
//! platform-neutral logic (the service's identity/definition, how CLI flags
//! become launch arguments, the reporting order of the lifecycle, and how a
//! "not launched by the SCM" failure is recognized) lives at the top of this
//! file, is exercised by the tests below on every platform, and is only
//! *mapped onto* the real `windows_service` types inside the Windows-only
//! section.

use crate::config::Cli;
use std::path::PathBuf;

/// Service name registered with the SCM (used for `sc query`, event log
/// sourcing, etc. — must contain no spaces).
pub const SERVICE_NAME: &str = "OpcdaBridgeGateway";
/// Human-readable name shown in `services.msc`.
pub const SERVICE_DISPLAY_NAME: &str = "OPC DA Bridge Gateway";
/// Shown as the service's description in `services.msc`.
pub const SERVICE_DESCRIPTION: &str = "Bridges native OPC DA (COM/DCOM) tags to opcda-bridge clients over the network. \
     https://github.com/mikeboiko/opcda-bridge";

/// Plain, platform-neutral description of how the gateway should be
/// registered with the SCM. Built and tested independent of the
/// Windows-only `windows_service::service::ServiceInfo` it is later mapped
/// onto one field at a time, so this construction logic runs — and is
/// covered — on every platform.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceDefinition {
    pub name: String,
    pub display_name: String,
    pub description: String,
    pub executable_path: PathBuf,
    pub launch_arguments: Vec<String>,
}

/// Re-serialize whichever CLI flags were given to `install` into the
/// argument list the SCM should launch the executable with. The SCM always
/// starts a service's executable bare (no interactive shell, no inherited
/// environment beyond the system default), so anything the operator wants
/// applied every time the service starts — port, config path, log
/// settings — must be baked into the registration itself rather than
/// relying on how the executable happened to be invoked once at install
/// time.
pub fn service_launch_arguments(cli: &Cli) -> Vec<String> {
    let mut args = Vec::new();
    if let Some(config) = &cli.config {
        args.push("--config".to_string());
        args.push(config.display().to_string());
    }
    if let Some(port) = cli.port {
        args.push("--port".to_string());
        args.push(port.to_string());
    }
    if let Some(level) = &cli.log_level {
        args.push("--log-level".to_string());
        args.push(level.clone());
    }
    if let Some(dir) = &cli.log_dir {
        args.push("--log-dir".to_string());
        args.push(dir.display().to_string());
    }
    if let Some(format) = &cli.log_format {
        args.push("--log-format".to_string());
        args.push(format.clone());
    }
    if let Some(rotation) = &cli.log_rotation {
        args.push("--log-rotation".to_string());
        args.push(rotation.clone());
    }
    args
}

/// Build the platform-neutral service definition used by `install`,
/// pairing the current executable's path with whichever CLI flags should
/// carry over into the service's own launch.
pub fn build_service_definition(executable_path: PathBuf, cli: &Cli) -> ServiceDefinition {
    ServiceDefinition {
        name: SERVICE_NAME.to_string(),
        display_name: SERVICE_DISPLAY_NAME.to_string(),
        description: SERVICE_DESCRIPTION.to_string(),
        executable_path,
        launch_arguments: service_launch_arguments(cli),
    }
}

/// The SCM status lifecycle the gateway reports while running as a Windows
/// service, kept as a plain enum (rather than directly using
/// `windows_service::service::ServiceState`, which only exists on Windows)
/// purely so the expected reporting order is itself unit-testable on every
/// platform. The Windows-only reporting code maps each variant onto the
/// real SCM API one-to-one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceLifecycle {
    /// Reported immediately after registering the control handler, while
    /// config/logging are still being resolved.
    StartPending,
    /// Reported once the listener is ready to serve requests.
    Running,
    /// Reported the instant a Stop/Shutdown control event arrives, before
    /// in-flight requests have finished draining.
    StopPending,
    /// Reported after the server has fully drained and returned.
    Stopped,
}

impl ServiceLifecycle {
    /// The state that follows this one in the gateway's fixed reporting
    /// sequence, or `None` after `Stopped` (the sequence's end). Encodes —
    /// and lets tests lock in — the intended order without needing the
    /// Windows-only types the real reporting code sends to the SCM.
    pub fn next(self) -> Option<Self> {
        match self {
            ServiceLifecycle::StartPending => Some(ServiceLifecycle::Running),
            ServiceLifecycle::Running => Some(ServiceLifecycle::StopPending),
            ServiceLifecycle::StopPending => Some(ServiceLifecycle::Stopped),
            ServiceLifecycle::Stopped => None,
        }
    }
}

/// Windows' `ERROR_FAILED_SERVICE_CONTROLLER_CONNECT`: the Win32 error code
/// `StartServiceCtrlDispatcherW` returns when the calling process was
/// launched interactively rather than by the Service Control Manager.
const ERROR_FAILED_SERVICE_CONTROLLER_CONNECT: i32 = 1063;

/// True when `code` is the raw OS error that means "this process wasn't
/// started by the SCM" — i.e. `main` should fall back to running the
/// gateway directly in the foreground rather than treating this as a real
/// failure. Kept as a plain function over the numeric code (rather than
/// matching directly on `windows_service::Error`, which only exists on
/// Windows) so this small but important piece of "which failure means fall
/// back to console mode" logic is still covered by the cross-platform test
/// run.
pub fn is_scm_launch_error_code(code: Option<i32>) -> bool {
    code == Some(ERROR_FAILED_SERVICE_CONTROLLER_CONNECT)
}

#[cfg(target_os = "windows")]
mod windows_impl {
    use super::*;
    use crate::run;
    use clap::Parser;
    use std::ffi::OsString;
    use std::time::Duration;
    use windows_service::service::{
        ServiceAccess, ServiceControl, ServiceControlAccept, ServiceErrorControl, ServiceExitCode,
        ServiceInfo, ServiceStartType, ServiceState, ServiceStatus, ServiceType,
    };
    use windows_service::service_control_handler::ServiceStatusHandle;
    use windows_service::service_control_handler::{self, ServiceControlHandlerResult};
    use windows_service::service_dispatcher;
    use windows_service::service_manager::{ServiceManager, ServiceManagerAccess};

    const SERVICE_TYPE: ServiceType = ServiceType::OWN_PROCESS;

    windows_service::define_windows_service!(ffi_service_main, service_main);

    /// Maps a [`ServiceDefinition`] onto the real, Windows-only
    /// `ServiceInfo` the SCM API needs. Deliberately trivial — all the
    /// actual decision-making already happened in [`build_service_definition`].
    fn to_service_info(definition: &ServiceDefinition) -> ServiceInfo {
        ServiceInfo {
            name: OsString::from(&definition.name),
            display_name: OsString::from(&definition.display_name),
            service_type: SERVICE_TYPE,
            start_type: ServiceStartType::AutoStart,
            error_control: ServiceErrorControl::Normal,
            executable_path: definition.executable_path.clone(),
            launch_arguments: definition
                .launch_arguments
                .iter()
                .map(OsString::from)
                .collect(),
            dependencies: vec![],
            account_name: None, // Run as LocalSystem.
            account_password: None,
        }
    }

    /// Registers the gateway with the SCM (does not start it).
    pub fn install(cli: &Cli) -> anyhow::Result<()> {
        let exe = std::env::current_exe()?;
        let definition = build_service_definition(exe, cli);
        let manager = ServiceManager::local_computer(
            None::<&str>,
            ServiceManagerAccess::CONNECT | ServiceManagerAccess::CREATE_SERVICE,
        )?;
        let service =
            manager.create_service(&to_service_info(&definition), ServiceAccess::CHANGE_CONFIG)?;
        service.set_description(&definition.description)?;
        println!(
            "Installed '{}' ({}). Start it with: opcda-bridge-gateway.exe start",
            definition.display_name, definition.name
        );
        Ok(())
    }

    /// Stops (if running) and removes the registered service.
    pub fn uninstall() -> anyhow::Result<()> {
        let manager = ServiceManager::local_computer(None::<&str>, ServiceManagerAccess::CONNECT)?;
        let service = manager.open_service(
            SERVICE_NAME,
            ServiceAccess::QUERY_STATUS | ServiceAccess::STOP | ServiceAccess::DELETE,
        )?;
        if service.query_status()?.current_state != ServiceState::Stopped {
            service.stop()?;
        }
        service.delete()?;
        println!("Uninstalled '{SERVICE_DISPLAY_NAME}'.");
        Ok(())
    }

    /// Starts the registered service.
    pub fn start() -> anyhow::Result<()> {
        let manager = ServiceManager::local_computer(None::<&str>, ServiceManagerAccess::CONNECT)?;
        let service = manager.open_service(SERVICE_NAME, ServiceAccess::START)?;
        service.start::<&std::ffi::OsStr>(&[])?;
        println!("Started '{SERVICE_DISPLAY_NAME}'.");
        Ok(())
    }

    /// Requests the running service to stop.
    pub fn stop() -> anyhow::Result<()> {
        let manager = ServiceManager::local_computer(None::<&str>, ServiceManagerAccess::CONNECT)?;
        let service = manager.open_service(SERVICE_NAME, ServiceAccess::STOP)?;
        service.stop()?;
        println!("Stop requested for '{SERVICE_DISPLAY_NAME}'.");
        Ok(())
    }

    /// Prints the registered service's current SCM state.
    pub fn status() -> anyhow::Result<()> {
        let manager = ServiceManager::local_computer(None::<&str>, ServiceManagerAccess::CONNECT)?;
        let service = manager.open_service(SERVICE_NAME, ServiceAccess::QUERY_STATUS)?;
        let status = service.query_status()?;
        println!("{SERVICE_DISPLAY_NAME}: {:?}", status.current_state);
        Ok(())
    }

    /// True when `err` is the specific failure `service_dispatcher::start`
    /// returns when this process was launched interactively rather than by
    /// the SCM — the signal that `main` should fall back to console mode.
    pub fn is_run_outside_scm(err: &windows_service::Error) -> bool {
        matches!(
            err,
            windows_service::Error::Winapi(io_err) if is_scm_launch_error_code(io_err.raw_os_error())
        )
    }

    /// Registers the generated service entry point with the SCM and blocks
    /// until the service stops. Returns immediately with an error — no
    /// threads spawned, nothing torn down — if this process was not
    /// actually launched by the SCM; see [`is_run_outside_scm`].
    pub fn run_as_service() -> windows_service::Result<()> {
        service_dispatcher::start(SERVICE_NAME, ffi_service_main)
    }

    /// Reports one step of the gateway's SCM lifecycle. `controls_accepted`
    /// is only meaningful while `Running` — a service in a pending state
    /// cannot yet (or any longer) accept control events. `wait_hint` gives
    /// the SCM how long to wait before considering the service hung; the
    /// generous `StopPending` hint gives in-flight gRPC requests time to
    /// drain, matching the graceful-shutdown behavior `run::serve` already
    /// provides.
    fn report_status(
        handle: &ServiceStatusHandle,
        state: ServiceLifecycle,
    ) -> windows_service::Result<()> {
        let (current_state, controls_accepted, wait_hint) = match state {
            ServiceLifecycle::StartPending => (
                ServiceState::StartPending,
                ServiceControlAccept::empty(),
                Duration::from_secs(5),
            ),
            ServiceLifecycle::Running => (
                ServiceState::Running,
                ServiceControlAccept::STOP | ServiceControlAccept::SHUTDOWN,
                Duration::default(),
            ),
            ServiceLifecycle::StopPending => (
                ServiceState::StopPending,
                ServiceControlAccept::empty(),
                Duration::from_secs(30),
            ),
            ServiceLifecycle::Stopped => (
                ServiceState::Stopped,
                ServiceControlAccept::empty(),
                Duration::default(),
            ),
        };
        handle.set_service_status(ServiceStatus {
            service_type: SERVICE_TYPE,
            current_state,
            controls_accepted,
            exit_code: ServiceExitCode::Win32(0),
            checkpoint: 0,
            wait_hint,
            process_id: None,
        })
    }

    /// The service entry point invoked by the SCM on a background thread.
    /// `_arguments` is the SCM's secondary start-parameter channel (e.g. an
    /// operator running `sc start name extra`) — distinct from the
    /// process's real argv, which is what `Cli::parse()` inside
    /// `run_service` sees, identically to console-mode startup, since it's
    /// the same launch command `install` registered.
    fn service_main(_arguments: Vec<OsString>) {
        if let Err(e) = run_service() {
            // No console and, if this failed early, possibly no SCM status
            // handle either — file logging (once initialized) is the real
            // record of this; stderr is a last-resort breadcrumb.
            eprintln!("opcda-bridge gateway service run failed: {e:?}");
        }
    }

    fn run_service() -> anyhow::Result<()> {
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let shutdown_tx = std::sync::Mutex::new(Some(shutdown_tx));

        let event_handler = move |control_event| -> ServiceControlHandlerResult {
            match control_event {
                // All services must accept Interrogate even as a no-op.
                ServiceControl::Interrogate => ServiceControlHandlerResult::NoError,
                ServiceControl::Stop | ServiceControl::Shutdown => {
                    if let Some(tx) = shutdown_tx.lock().unwrap_or_else(|e| e.into_inner()).take() {
                        let _ = tx.send(());
                    }
                    ServiceControlHandlerResult::NoError
                }
                _ => ServiceControlHandlerResult::NotImplemented,
            }
        };

        let status_handle = service_control_handler::register(SERVICE_NAME, event_handler)?;
        report_status(&status_handle, ServiceLifecycle::StartPending)?;

        // Real launch arguments (baked in by `install`), not the SCM's
        // secondary `_arguments` parameter above.
        let cli = Cli::parse();

        report_status(&status_handle, ServiceLifecycle::Running)?;

        // Report `StopPending` the instant the stop signal arrives, before
        // `run::run_gateway`'s shutdown future actually resolves and the
        // in-flight-request drain begins — this is exactly the moment the
        // SCM needs to stop expecting an immediate `Stopped`. `ServiceStatusHandle`
        // is `Copy` (and documented safe to use from any thread), so this is a
        // plain copy, not a deep clone.
        let stop_status_handle = status_handle;
        let shutdown = async move {
            let _ = shutdown_rx.await;
            let _ = report_status(&stop_status_handle, ServiceLifecycle::StopPending);
        };

        let rt = tokio::runtime::Runtime::new()?;
        let result = rt.block_on(run::run_gateway(cli, shutdown));

        report_status(&status_handle, ServiceLifecycle::Stopped)?;
        result
    }
}

#[cfg(target_os = "windows")]
pub use windows_impl::{
    install, is_run_outside_scm, run_as_service, start, status, stop, uninstall,
};

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn cli_with(
        config: Option<&str>,
        port: Option<u16>,
        log_level: Option<&str>,
        log_dir: Option<&str>,
        log_format: Option<&str>,
        log_rotation: Option<&str>,
    ) -> Cli {
        Cli {
            command: None,
            config: config.map(PathBuf::from),
            port,
            log_level: log_level.map(str::to_string),
            log_dir: log_dir.map(PathBuf::from),
            log_format: log_format.map(str::to_string),
            log_rotation: log_rotation.map(str::to_string),
        }
    }

    #[test]
    fn test_service_launch_arguments_empty_when_no_flags_set() {
        let cli = cli_with(None, None, None, None, None, None);
        assert_eq!(service_launch_arguments(&cli), Vec::<String>::new());
    }

    #[test]
    fn test_service_launch_arguments_includes_every_set_flag() {
        let cli = cli_with(
            Some("C:\\cfg.toml"),
            Some(7700),
            Some("debug"),
            Some("C:\\logs"),
            Some("json"),
            Some("hourly"),
        );
        assert_eq!(
            service_launch_arguments(&cli),
            vec![
                "--config".to_string(),
                "C:\\cfg.toml".to_string(),
                "--port".to_string(),
                "7700".to_string(),
                "--log-level".to_string(),
                "debug".to_string(),
                "--log-dir".to_string(),
                "C:\\logs".to_string(),
                "--log-format".to_string(),
                "json".to_string(),
                "--log-rotation".to_string(),
                "hourly".to_string(),
            ]
        );
    }

    #[test]
    fn test_service_launch_arguments_partial_flags() {
        let cli = cli_with(None, Some(8080), None, None, Some("json"), None);
        assert_eq!(
            service_launch_arguments(&cli),
            vec![
                "--port".to_string(),
                "8080".to_string(),
                "--log-format".to_string(),
                "json".to_string(),
            ]
        );
    }

    #[test]
    fn test_build_service_definition_carries_identity_and_arguments() {
        let cli = cli_with(None, Some(9000), None, None, None, None);
        let definition = build_service_definition(PathBuf::from("/opt/gateway.exe"), &cli);
        assert_eq!(definition.name, SERVICE_NAME);
        assert_eq!(definition.display_name, SERVICE_DISPLAY_NAME);
        assert_eq!(definition.description, SERVICE_DESCRIPTION);
        assert_eq!(
            definition.executable_path,
            PathBuf::from("/opt/gateway.exe")
        );
        assert_eq!(
            definition.launch_arguments,
            vec!["--port".to_string(), "9000".to_string()]
        );
    }

    #[test]
    fn test_service_lifecycle_sequence_order() {
        assert_eq!(
            ServiceLifecycle::StartPending.next(),
            Some(ServiceLifecycle::Running)
        );
        assert_eq!(
            ServiceLifecycle::Running.next(),
            Some(ServiceLifecycle::StopPending)
        );
        assert_eq!(
            ServiceLifecycle::StopPending.next(),
            Some(ServiceLifecycle::Stopped)
        );
    }

    #[test]
    fn test_service_lifecycle_stopped_is_terminal() {
        assert_eq!(ServiceLifecycle::Stopped.next(), None);
    }

    #[test]
    fn test_is_scm_launch_error_code_matches_expected_code() {
        assert!(is_scm_launch_error_code(Some(1063)));
    }

    #[test]
    fn test_is_scm_launch_error_code_rejects_other_codes() {
        assert!(!is_scm_launch_error_code(Some(5)));
        assert!(!is_scm_launch_error_code(None));
    }
}

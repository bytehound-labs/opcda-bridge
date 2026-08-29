#[cfg(target_os = "windows")]
fn main() -> anyhow::Result<()> {
    use clap::Parser;
    use opcda_bridge_gateway::config::{Cli, ServiceCommand};
    use opcda_bridge_gateway::{run, service};

    let cli = Cli::parse();

    if matches!(cli.command.as_ref(), Some(ServiceCommand::IndexPrepare)) {
        return run::prepare_index(cli);
    }

    if let Some(command) = &cli.command {
        return match command {
            ServiceCommand::Install => service::install(&cli),
            ServiceCommand::Uninstall => service::uninstall(),
            ServiceCommand::Start => service::start(),
            ServiceCommand::Stop => service::stop(),
            ServiceCommand::Status => service::status(),
        };
    }

    // No subcommand: either running interactively (console mode) or
    // launched bare by the SCM, exactly as `install` registered it. Try SCM
    // dispatch first; only fall back to console mode if that failed
    // specifically because the SCM didn't actually launch this process.
    match service::run_as_service() {
        Ok(()) => Ok(()),
        Err(e) if service::is_run_outside_scm(&e) => {
            let rt = tokio::runtime::Runtime::new()?;
            rt.block_on(run::run_gateway(cli, run::shutdown_signal(), || {}))
        }
        Err(e) => Err(e.into()),
    }
}

#[cfg(not(target_os = "windows"))]
fn main() {
    opcda_bridge_gateway::non_windows_run();
}

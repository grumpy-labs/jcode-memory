use anyhow::Result;
use clap::Parser;

use crate::{logging, perf, server, startup_profile, storage, telemetry};

use super::{args::Args, dispatch, output, terminal};

pub async fn run() -> Result<()> {
    startup_profile::init();

    terminal::install_panic_hook();
    startup_profile::mark("panic_hook");

    logging::init();
    startup_profile::mark("logging_init");
    logging::cleanup_old_logs();
    startup_profile::mark("log_cleanup");
    logging::info("jcode starting");
    crate::platform::raise_nofile_limit_best_effort(8_192);
    startup_profile::mark("nofile_limit");

    storage::harden_user_config_permissions();
    startup_profile::mark("perm_harden");

    perf::init_background();
    startup_profile::mark("perf_init");

    telemetry::record_install_if_first_run();
    telemetry::record_upgrade_if_needed();
    startup_profile::mark("telemetry_check");

    let args = parse_and_prepare_args()?;

    if let Err(e) = dispatch::run_main(args).await {
        report_main_error(&e);
        return Err(e);
    }

    Ok(())
}

fn parse_and_prepare_args() -> Result<Args> {
    let args = Args::parse();
    startup_profile::mark("args_parse");

    output::set_quiet_enabled(args.quiet);

    if let Some(cwd) = &args.cwd {
        std::env::set_current_dir(cwd)?;
        logging::info(&format!("Changed working directory to: {}", cwd));
    }

    if args.trace {
        crate::env::set_var("JCODE_TRACE", "1");
    }

    if let Some(ref socket) = args.socket {
        server::set_socket_path(socket);
    }

    crate::process_title::set_initial_title(&args);

    Ok(args)
}

fn report_main_error(error: &anyhow::Error) {
    let error_str = format!("{:?}", error);
    logging::error(&error_str);

    if let Some(session_id) = terminal::get_current_session() {
        output::stderr_blank_line();
        output::stderr_info("\x1b[33mTo restore this session, run:\x1b[0m");
        output::stderr_info(format!("  jcode --resume {}", session_id));
        output::stderr_blank_line();
    }
}

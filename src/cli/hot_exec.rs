use anyhow::Result;
use std::path::Path;
use std::process::Command as ProcessCommand;

use crate::{build, tui::RunResult};

pub fn has_requested_action(run_result: &RunResult) -> bool {
    run_result.reload_session.is_some()
        || run_result.rebuild_session.is_some()
        || run_result.restart_session.is_some()
}

pub fn execute_requested_action(run_result: &RunResult) -> Result<()> {
    if let Some(ref reload_session_id) = run_result.reload_session {
        hot_reload(reload_session_id)?;
    }

    if let Some(ref rebuild_session_id) = run_result.rebuild_session {
        hot_rebuild(rebuild_session_id)?;
    }

    if let Some(ref restart_session_id) = run_result.restart_session {
        hot_restart(restart_session_id)?;
    }

    Ok(())
}

pub fn hot_restart(session_id: &str) -> Result<()> {
    let cwd = std::env::current_dir()?;
    let exe = std::env::current_exe()?;
    let is_selfdev = crate::cli::selfdev::client_selfdev_requested();

    crate::logging::info(&format!("Restarting with current binary: {:?}", exe));

    crate::env::set_var("JCODE_RESUMING", "1");

    let mut cmd = ProcessCommand::new(&exe);
    if is_selfdev {
        cmd.arg("self-dev");
    }
    cmd.arg("--resume").arg(session_id).current_dir(&cwd);
    let err = crate::platform::replace_process(&mut cmd);

    Err(anyhow::anyhow!("Failed to exec {:?}: {}", exe, err))
}

pub fn hot_reload(session_id: &str) -> Result<()> {
    let cwd = std::env::current_dir()?;

    crate::env::set_var("JCODE_RESUMING", "1");

    if let Ok(migrate_binary) = std::env::var("JCODE_MIGRATE_BINARY") {
        let binary_path = std::path::PathBuf::from(&migrate_binary);
        if binary_path.exists() {
            crate::logging::info("Migrating to stable binary...");
            let err = crate::platform::replace_process(
                ProcessCommand::new(&binary_path)
                    .arg("--resume")
                    .arg(session_id)
                    .current_dir(cwd),
            );
            return Err(anyhow::anyhow!("Failed to exec {:?}: {}", binary_path, err));
        } else {
            crate::logging::warn(&format!(
                "Migration binary not found at {:?}, falling back to local binary",
                binary_path
            ));
        }
    }

    let is_selfdev = crate::cli::selfdev::client_selfdev_requested();
    let (exe, _label) = build::preferred_reload_candidate(is_selfdev)
        .ok_or_else(|| anyhow::anyhow!("No reloadable binary found"))?;

    if let Ok(metadata) = std::fs::metadata(&exe) {
        let age = metadata
            .modified()
            .ok()
            .and_then(|m| m.elapsed().ok())
            .map(|d| {
                let secs = d.as_secs();
                if secs < 60 {
                    format!("{} seconds ago", secs)
                } else if secs < 3600 {
                    format!("{} minutes ago", secs / 60)
                } else {
                    format!("{} hours ago", secs / 3600)
                }
            })
            .unwrap_or_else(|| "unknown".to_string());
        crate::logging::info(&format!("Reloading with binary built {}...", age));
    }

    for attempt in 0..3 {
        if attempt > 0 {
            std::thread::sleep(std::time::Duration::from_millis(200));
            if !exe.exists() {
                continue;
            }
        }
        let mut cmd = ProcessCommand::new(&exe);
        if is_selfdev {
            cmd.arg("self-dev");
        }
        cmd.arg("--resume").arg(session_id).current_dir(&cwd);
        let err = crate::platform::replace_process(&mut cmd);

        if err.kind() == std::io::ErrorKind::NotFound && attempt < 2 {
            crate::logging::warn(&format!(
                "exec attempt {} failed (ENOENT) for {:?}, retrying...",
                attempt + 1,
                exe
            ));
            continue;
        }
        return Err(anyhow::anyhow!("Failed to exec {:?}: {}", exe, err));
    }
    Err(anyhow::anyhow!(
        "Failed to exec {:?}: binary not found after retries",
        exe
    ))
}

pub fn hot_rebuild(session_id: &str) -> Result<()> {
    let cwd = std::env::current_dir()?;
    let repo_dir =
        build::get_repo_dir().ok_or_else(|| anyhow::anyhow!("Could not find jcode repository"))?;

    eprintln!("Rebuilding jcode with session {}...", session_id);

    eprintln!("Building...");
    let build_status = ProcessCommand::new("cargo")
        .args(["build", "--release"])
        .current_dir(&repo_dir)
        .status()?;

    if !build_status.success() {
        anyhow::bail!("Build failed - staying on current version");
    }

    eprintln!("Running tests...");
    let test = ProcessCommand::new("cargo")
        .args(["test", "--release", "--", "--test-threads=1"])
        .current_dir(&repo_dir)
        .status()?;

    if !test.success() {
        eprintln!("\n⚠️  Tests failed! Aborting reload to protect your session.");
        eprintln!("Fix the failing tests and try /rebuild again.");
        anyhow::bail!("Tests failed - staying on current version");
    }

    eprintln!("✓ All tests passed");

    let is_selfdev = crate::cli::selfdev::client_selfdev_requested();
    let exe = build::release_binary_path(&repo_dir);
    if !exe.exists() {
        anyhow::bail!("Binary not found at {:?}", exe);
    }

    eprintln!("Restarting with session {}...", session_id);

    crate::env::set_var("JCODE_RESUMING", "1");

    let mut cmd = ProcessCommand::new(&exe);
    if is_selfdev {
        cmd.arg("self-dev");
    }
    cmd.arg("--resume").arg(session_id).current_dir(&cwd);
    let err = crate::platform::replace_process(&mut cmd);

    Err(anyhow::anyhow!("Failed to exec {:?}: {}", exe, err))
}

fn rebuild_version_label(repo_dir: &Path) -> String {
    build::current_build_info(repo_dir)
        .map(|info| {
            if info.dirty {
                format!("{}-dirty", info.hash)
            } else {
                info.hash
            }
        })
        .unwrap_or_else(|_| "local source build".to_string())
}

pub fn spawn_background_session_rebuild(session_id: String) {
    std::thread::spawn(move || {
        use crate::bus::{Bus, BusEvent, ClientMaintenanceAction, SessionUpdateStatus};

        let action = ClientMaintenanceAction::Rebuild;
        let publish = |status| Bus::global().publish(BusEvent::SessionUpdateStatus(status));

        let Some(repo_dir) = build::get_repo_dir() else {
            publish(SessionUpdateStatus::Error {
                session_id,
                action,
                message: "Rebuild failed: could not find the jcode repository.".to_string(),
            });
            return;
        };

        publish(SessionUpdateStatus::Status {
            session_id: session_id.clone(),
            action,
            message: "Building release binary in the background...".to_string(),
        });
        let build_status = match ProcessCommand::new("cargo")
            .args(["build", "--release"])
            .current_dir(&repo_dir)
            .status()
        {
            Ok(status) => status,
            Err(error) => {
                publish(SessionUpdateStatus::Error {
                    session_id,
                    action,
                    message: format!("Rebuild failed while starting cargo build: {}", error),
                });
                return;
            }
        };

        if !build_status.success() {
            publish(SessionUpdateStatus::Error {
                session_id,
                action,
                message: "Build failed — staying on the current binary.".to_string(),
            });
            return;
        }

        publish(SessionUpdateStatus::Status {
            session_id: session_id.clone(),
            action,
            message: "Running release tests in the background...".to_string(),
        });
        let test_status = match ProcessCommand::new("cargo")
            .args(["test", "--release", "--", "--test-threads=1"])
            .current_dir(&repo_dir)
            .status()
        {
            Ok(status) => status,
            Err(error) => {
                publish(SessionUpdateStatus::Error {
                    session_id,
                    action,
                    message: format!("Rebuild failed while starting tests: {}", error),
                });
                return;
            }
        };

        if !test_status.success() {
            publish(SessionUpdateStatus::Error {
                session_id,
                action,
                message: "Tests failed — staying on the current binary. Fix the failing tests and try /rebuild again.".to_string(),
            });
            return;
        }

        let exe = build::release_binary_path(&repo_dir);
        if !exe.exists() {
            publish(SessionUpdateStatus::Error {
                session_id,
                action,
                message: format!(
                    "Rebuild finished but no reloadable binary was found at {:?}.",
                    exe
                ),
            });
            return;
        }

        publish(SessionUpdateStatus::ReadyToReload {
            session_id,
            action,
            version: rebuild_version_label(&repo_dir),
        });
    });
}

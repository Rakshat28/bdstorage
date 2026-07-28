use anyhow::{Context, Result};
use std::fs;
use std::path::Path;
use std::process::Command;

const UNIT_NAME: &str = "bdstorage-dedupe.service";
const UNIT_PATH: &str = "/etc/systemd/system/bdstorage-dedupe.service";

pub fn install_daemon_service(
    target: &Path,
    interval_secs: u64,
    allow_unsafe_hardlinks: bool,
) -> Result<()> {
    let exe = std::env::current_exe().context("resolve current executable")?;
    let exe = exe
        .canonicalize()
        .unwrap_or(exe)
        .to_string_lossy()
        .to_string();
    let target = target
        .canonicalize()
        .unwrap_or_else(|_| target.to_path_buf())
        .to_string_lossy()
        .to_string();

    let service_user = detect_service_user()?;
    let unit = render_unit(
        &exe,
        &target,
        interval_secs,
        &service_user,
        allow_unsafe_hardlinks,
    );

    if unsafe { libc::geteuid() == 0 } {
        fs::write(UNIT_PATH, unit).with_context(|| format!("write {UNIT_PATH}"))?;

        let reload = Command::new("systemctl")
            .arg("daemon-reload")
            .status()
            .context("run systemctl daemon-reload")?;
        if !reload.success() {
            anyhow::bail!("systemctl daemon-reload failed with status {reload}");
        }

        let enable = Command::new("systemctl")
            .args(["enable", "--now", UNIT_NAME])
            .status()
            .context("run systemctl enable --now")?;
        if !enable.success() {
            anyhow::bail!("systemctl enable --now {UNIT_NAME} failed with status {enable}");
        }

        println!("Installed and started {UNIT_NAME} at {UNIT_PATH}");
        return Ok(());
    }

    println!("Generated systemd unit for {UNIT_NAME}.");
    println!("Run the following commands:");
    println!();
    println!("sudo tee {UNIT_PATH} >/dev/null <<'EOF'");
    print!("{unit}");
    println!("EOF");
    println!("sudo systemctl daemon-reload");
    println!("sudo systemctl enable --now {UNIT_NAME}");
    println!("sudo journalctl -u {UNIT_NAME} -f");

    Ok(())
}

fn detect_service_user() -> Result<String> {
    std::env::var("SUDO_USER")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| {
            std::env::var("USER")
                .ok()
                .filter(|value| !value.trim().is_empty())
        })
        .context("unable to resolve service user from SUDO_USER or USER")
}

fn render_unit(
    exe: &str,
    target: &str,
    interval_secs: u64,
    service_user: &str,
    allow_unsafe_hardlinks: bool,
) -> String {
    let mut exec_start = format!(
        "{} daemon run {} --interval-secs {}",
        escape_for_systemd(exe),
        escape_for_systemd(target),
        interval_secs,
    );
    if allow_unsafe_hardlinks {
        exec_start.push_str(" --allow-unsafe-hardlinks");
    }

    format!(
        "[Unit]\nDescription=bdstorage deduplication daemon\nAfter=local-fs.target\n\n[Service]\nType=simple\nUser={}\nGroup={}\nExecStart={}\nRestart=on-failure\nRestartSec=30\nStandardOutput=journal\nStandardError=journal\n\n[Install]\nWantedBy=multi-user.target\n",
        escape_for_systemd(service_user),
        escape_for_systemd(service_user),
        exec_start,
    )
}

fn escape_for_systemd(arg: &str) -> String {
    if arg.is_empty() {
        return "\"\"".to_string();
    }
    if arg
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '/' | '-' | '_' | '.' | ':'))
    {
        return arg.to_string();
    }

    let escaped = arg
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('$', "$$");
    format!("\"{escaped}\"")
}

pub fn notify_ready() -> Result<()> {
    // notify systemd that the daemon is ready
    let state = [(libsystemd::daemon::STATE_READY, "1")];
    if libsystemd::daemon::booted() {
        libsystemd::daemon::notify(false, &state)
            .context("failed to notify systemd")?;
    }
    Ok(())
}

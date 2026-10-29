use super::*;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{RecvTimeoutError, Sender};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::config::ConfigStore;

/// Local, non-synced bookkeeping: which device wrote the remote backup and how
/// recent our last successful exchange was. Kept outside sessions.json so a
/// download can never overwrite it.
const SYNC_STATE_FILE: &str = "webdav-sync.json";
const UPLOAD_DEBOUNCE: Duration = Duration::from_secs(3);
const STARTUP_TIMEOUT: Duration = Duration::from_secs(4);

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct SyncState {
    device_id: String,
    #[serde(default)]
    last_remote_updated: u64,
}

impl SyncState {
    fn path() -> std::path::PathBuf {
        crate::config::data_dir().join(SYNC_STATE_FILE)
    }

    fn load() -> Self {
        let path = Self::path();
        if let Ok(raw) = std::fs::read_to_string(&path) {
            if let Ok(state) = serde_json::from_str::<Self>(&raw) {
                if !state.device_id.is_empty() {
                    return state;
                }
            }
        }
        let state = Self {
            device_id: uuid::Uuid::new_v4().to_string(),
            last_remote_updated: 0,
        };
        state.save();
        state
    }

    fn save(&self) {
        let path = Self::path();
        let raw = match serde_json::to_string_pretty(self) {
            Ok(raw) => raw,
            Err(_) => return,
        };
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if std::fs::write(&path, &raw).is_ok() {
                let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600));
            }
        }
        #[cfg(not(unix))]
        let _ = std::fs::write(&path, &raw);
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Saves triggered by an import must not be uploaded back as a new revision.
static SUPPRESS_UNTIL_MS: AtomicU64 = AtomicU64::new(0);

fn suppress_auto_upload(for_ms: u64) {
    SUPPRESS_UNTIL_MS.store(now_ms().saturating_add(for_ms), Ordering::Relaxed);
}

fn auto_upload_suppressed() -> bool {
    now_ms() < SUPPRESS_UNTIL_MS.load(Ordering::Relaxed)
}

/// `(device_id, updated_at)` from a v2 backup; `None` for legacy/v1 files.
fn backup_meta(raw: &str) -> Option<(String, u64)> {
    let value: serde_json::Value = serde_json::from_str(raw).ok()?;
    if value.get("meatshell_backup")?.as_u64()? != 2 {
        return None;
    }
    Some((
        value.get("device_id")?.as_str()?.to_string(),
        value.get("updated_at")?.as_u64()?,
    ))
}

struct UploadJob {
    json: String,
    updated_at: u64,
    url: String,
    username: String,
    password: String,
    accept_invalid_certs: bool,
}

/// Start the debounced auto-upload worker and register the post-save hook. The
/// hook checks `webdav_enabled` at call time, so toggling sync in Settings takes
/// effect without restarting.
pub(super) fn wire_auto_upload(store: &Rc<RefCell<ConfigStore>>) {
    let (tx, rx) = std::sync::mpsc::channel::<UploadJob>();
    std::thread::spawn(move || {
        loop {
            let mut job = match rx.recv() {
                Ok(job) => job,
                Err(_) => return,
            };
            // Coalesce a burst of saves (typing in a dialog, dragging panels…).
            loop {
                match rx.recv_timeout(UPLOAD_DEBOUNCE) {
                    Ok(next) => job = next,
                    Err(RecvTimeoutError::Timeout) => break,
                    Err(RecvTimeoutError::Disconnected) => return,
                }
            }
            let res = webdav_put_json(
                &job.url,
                &job.username,
                &job.password,
                job.accept_invalid_certs,
                job.json,
            );
            match res {
                Ok(()) => {
                    let mut state = SyncState::load();
                    state.last_remote_updated = job.updated_at;
                    state.save();
                    tracing::info!("webdav auto-upload ok");
                }
                Err(e) => tracing::warn!("webdav auto-upload failed: {e:#}"),
            }
        }
    });

    store.borrow().set_save_hook(Some(Arc::new({
        let tx: Sender<UploadJob> = tx;
        move |s: &ConfigStore| {
            if !s.webdav_enabled() || auto_upload_suppressed() {
                return;
            }
            // Only touch the local state file once sync is actually in use.
            let device_id = SyncState::load().device_id;
            let updated_at = now_ms();
            let Ok((json, _)) = s.export_backup_json(updated_at, &device_id) else {
                return;
            };
            let _ = tx.send(UploadJob {
                json,
                updated_at,
                url: s.webdav_url().to_string(),
                username: s.webdav_username().to_string(),
                password: s.webdav_password().to_string(),
                accept_invalid_certs: s.webdav_accept_invalid_certs(),
            });
        }
    })));
}

/// Startup pull: if the remote backup is newer than our last exchange and was
/// written by another device, import it before the UI is built so every field
/// applies. Network failures are non-fatal (offline launches keep local data).
pub(super) fn startup_pull(store: &mut ConfigStore) {
    if !store.webdav_enabled() {
        return;
    }
    let url = store.webdav_url().to_string();
    if url.trim().is_empty() {
        return;
    }
    let state = SyncState::load();
    let agent = webdav_agent_with_timeout(store.webdav_accept_invalid_certs(), STARTUP_TIMEOUT);
    let raw = match webdav_get_json_with(
        &agent,
        &url,
        store.webdav_username(),
        store.webdav_password(),
    ) {
        Ok(raw) => raw,
        Err(e) => {
            tracing::debug!("webdav startup pull skipped: {e:#}");
            return;
        }
    };
    let Some((device_id, updated_at)) = backup_meta(&raw) else {
        return;
    };
    if device_id == state.device_id {
        // Our own last upload; just remember its revision.
        if updated_at > state.last_remote_updated {
            let mut state = state;
            state.last_remote_updated = updated_at;
            state.save();
        }
        return;
    }
    if updated_at <= state.last_remote_updated {
        return;
    }
    match store.import_backup_json(&raw) {
        Ok(summary) => {
            let mut state = state;
            state.last_remote_updated = updated_at;
            state.save();
            tracing::info!(
                sessions = summary.added,
                quick_commands = summary.quick_commands,
                "webdav startup pull applied"
            );
        }
        Err(e) => tracing::warn!("webdav startup pull import failed: {e:#}"),
    }
}

/// Name of the profile backing file inside the user-chosen sync directory.
const BACKUP_FILE: &str = "meatshell-backup.json";

/// Validate and normalise the configured sync directory (no file name).
fn webdav_dir_url(base: &str) -> Result<String> {
    let base = base.trim().trim_end_matches('/');
    if base.is_empty() {
        anyhow::bail!(
            "{}",
            t("WebDAV 目录不能为空", "the WebDAV folder cannot be empty")
        );
    }
    if !base.starts_with("http://") && !base.starts_with("https://") {
        anyhow::bail!(
            "{}",
            t(
                "WebDAV 地址必须以 http:// 或 https:// 开头",
                "WebDAV URL must start with http:// or https://"
            )
        );
    }
    if base.starts_with("http://") && webdav_url_uses_port(base, 5006) {
        anyhow::bail!(
            "{}",
            t(
                "飞牛 WebDAV 的 5006 通常是 HTTPS 端口，请改用 https://...:5006；如果要用 HTTP，请改用 5005 端口",
                "FnOS WebDAV port 5006 is usually HTTPS; use https://...:5006, or use port 5005 for HTTP"
            )
        );
    }
    if base.starts_with("https://") && webdav_url_uses_port(base, 5005) {
        anyhow::bail!(
            "{}",
            t(
                "飞牛 WebDAV 的 5005 通常是 HTTP 端口，请改用 http://...:5005；如果要用 HTTPS，请改用 5006 端口",
                "FnOS WebDAV port 5005 is usually HTTP; use http://...:5005, or use port 5006 for HTTPS"
            )
        );
    }
    if base.ends_with(".json") {
        anyhow::bail!(
            "{}",
            t(
                "请填写 WebDAV 目录地址，不要带文件名；备份文件由程序自动写入该目录",
                "Enter the WebDAV folder only, without a file name; the backup file is written into it automatically"
            )
        );
    }
    if (webdav_url_uses_port(base, 5005) || webdav_url_uses_port(base, 5006))
        && !webdav_url_has_path(base)
    {
        anyhow::bail!(
            "{}",
            t(
                "飞牛 WebDAV 需要写入某个共享目录，不能直接写到根路径；请把地址改成 https://IP:5006/all/ 这样的目录",
                "FnOS WebDAV needs a writable shared folder, not the server root; use a folder URL such as https://IP:5006/all/"
            )
        );
    }
    Ok(base.to_string())
}

/// Full URL of the backup file inside the sync directory.
fn webdav_file_url(base: &str) -> Result<String> {
    Ok(format!("{}/{}", webdav_dir_url(base)?, BACKUP_FILE))
}

fn webdav_url_uses_port(base: &str, port: u16) -> bool {
    let Some(authority) = base.split("://").nth(1) else {
        return false;
    };
    let host_port = authority.split('/').next().unwrap_or(authority);
    host_port
        .rsplit_once(':')
        .and_then(|(_, p)| p.parse::<u16>().ok())
        == Some(port)
}

fn webdav_url_has_path(base: &str) -> bool {
    let Some(authority) = base.split("://").nth(1) else {
        return false;
    };
    authority
        .split_once('/')
        .is_some_and(|(_, path)| !path.is_empty())
}

fn webdav_auth_header(username: &str, password: &str) -> Option<String> {
    if username.is_empty() && password.is_empty() {
        return None;
    }
    use base64::{engine::general_purpose::STANDARD, Engine as _};
    Some(format!(
        "Basic {}",
        STANDARD.encode(format!("{username}:{password}"))
    ))
}

fn webdav_auth_req(mut req: ureq::Request, auth: Option<&str>) -> ureq::Request {
    if let Some(auth) = auth {
        req = req.set("Authorization", auth);
    }
    req
}

fn webdav_agent(accept_invalid_certs: bool) -> ureq::Agent {
    webdav_agent_with_timeout(accept_invalid_certs, std::time::Duration::from_secs(20))
}

fn webdav_agent_with_timeout(
    accept_invalid_certs: bool,
    timeout: std::time::Duration,
) -> ureq::Agent {
    let mut builder = ureq::AgentBuilder::new().timeout(timeout);
    if accept_invalid_certs {
        let tls_config = ureq::rustls::ClientConfig::builder_with_provider(
            ureq::rustls::crypto::ring::default_provider().into(),
        )
        .with_protocol_versions(&[&ureq::rustls::version::TLS12, &ureq::rustls::version::TLS13])
        .expect("rustls ring provider supports TLS 1.2 and TLS 1.3")
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(WebDavAcceptAnyCertVerifier))
        .with_no_client_auth();
        builder = builder.tls_config(Arc::new(tls_config));
    }
    builder.build()
}

fn webdav_error(e: ureq::Error) -> anyhow::Error {
    if let ureq::Error::Status(status, response) = e {
        let url = response.get_url().to_string();
        let body = response.into_string().unwrap_or_default();
        let body = body.trim();
        let detail = if body.is_empty() {
            String::new()
        } else {
            format!(": {}", body.chars().take(240).collect::<String>())
        };
        if status == 400 {
            return anyhow::anyhow!(
                "{}: {url}: status code 400{detail}",
                t(
                    "请求被 WebDAV 服务拒绝，请检查地址协议/端口是否匹配，以及远端文件所在目录是否已开启 WebDAV 协议访问",
                    "WebDAV rejected the request; check the URL scheme/port and whether the remote folder allows WebDAV access"
                )
            );
        }
        if status == 405 {
            return anyhow::anyhow!(
                "{}: {url}: status code 405{detail}",
                t(
                    "当前 WebDAV 路径不允许上传；飞牛请写入已开启协议访问的共享目录，例如 WebDAV 地址填 https://IP:5006/all/，或远端文件填 all/meatshell-connections.json",
                    "The current WebDAV path does not allow upload; for FnOS, write into a shared folder such as https://IP:5006/all/ or set remote file to all/meatshell-connections.json"
                )
            );
        }
        return anyhow::anyhow!("{url}: status code {status}{detail}");
    }
    let msg = e.to_string();
    if msg.contains("UnknownIssuer") || msg.contains("invalid peer certificate") {
        anyhow::anyhow!(
            "{} ({msg})",
            t(
                "HTTPS 证书不受信任；如果这是可信 NAS/局域网 WebDAV，请在设置里开启“信任自签名/内网证书”",
                "HTTPS certificate is not trusted; enable \"Trust self-signed / intranet certs\" for a trusted NAS/LAN WebDAV"
            )
        )
    } else {
        anyhow::anyhow!("{msg}")
    }
}

fn webdav_parent_dirs(url: &str) -> Vec<String> {
    let Some((scheme, rest)) = url.split_once("://") else {
        return Vec::new();
    };
    let Some((authority, path)) = rest.split_once('/') else {
        return Vec::new();
    };
    let parts: Vec<&str> = path.split('/').filter(|part| !part.is_empty()).collect();
    if parts.len() <= 1 {
        return Vec::new();
    }
    let mut dirs = Vec::with_capacity(parts.len() - 1);
    let mut current = format!("{scheme}://{authority}");
    for part in parts.iter().take(parts.len() - 1) {
        current.push('/');
        current.push_str(part);
        current.push('/');
        dirs.push(current.clone());
        current.pop();
    }
    dirs
}

fn webdav_dir_missing_or_no_create_error() -> anyhow::Error {
    anyhow::anyhow!(
        "{}",
        t(
            "文件夹不存在也无权限创建",
            "folder does not exist and cannot be created"
        )
    )
}

fn webdav_dir_exists(agent: &ureq::Agent, url: &str, auth: Option<&str>) -> Result<bool> {
    let req = webdav_auth_req(agent.request("PROPFIND", url).set("Depth", "0"), auth);
    match req.call() {
        Ok(_) => Ok(true),
        Err(ureq::Error::Status(status, _)) if status == 404 || status == 409 => Ok(false),
        Err(ureq::Error::Status(status, _)) if status == 401 || status == 403 || status == 405 => {
            Err(webdav_dir_missing_or_no_create_error())
        }
        Err(e) => Err(webdav_error(e)),
    }
}

fn webdav_create_dir(agent: &ureq::Agent, url: &str, auth: Option<&str>) -> Result<()> {
    let req = webdav_auth_req(agent.request("MKCOL", url), auth);
    match req.call() {
        Ok(_) => Ok(()),
        Err(ureq::Error::Status(status, _)) if status == 405 => Ok(()),
        Err(ureq::Error::Status(status, _))
            if status == 401 || status == 403 || status == 404 || status == 409 =>
        {
            Err(webdav_dir_missing_or_no_create_error())
        }
        Err(e) => Err(webdav_error(e)),
    }
}

fn webdav_ensure_parent_dirs(agent: &ureq::Agent, url: &str, auth: Option<&str>) -> Result<()> {
    for dir in webdav_parent_dirs(url) {
        if !webdav_dir_exists(agent, &dir, auth)? {
            webdav_create_dir(agent, &dir, auth)?;
        }
    }
    Ok(())
}

pub(super) fn webdav_put_json(
    base_url: &str,
    username: &str,
    password: &str,
    accept_invalid_certs: bool,
    json: String,
) -> Result<()> {
    let url = webdav_file_url(base_url)?;
    let agent = webdav_agent(accept_invalid_certs);
    let auth = webdav_auth_header(username, password);
    webdav_ensure_parent_dirs(&agent, &url, auth.as_deref())?;
    let req = webdav_auth_req(
        agent.put(&url).set("Content-Type", "application/json"),
        auth.as_deref(),
    );
    req.send_string(&json).map(|_| ()).map_err(webdav_error)
}

pub(super) fn webdav_get_json(
    base_url: &str,
    username: &str,
    password: &str,
    accept_invalid_certs: bool,
) -> Result<String> {
    let agent = webdav_agent(accept_invalid_certs);
    webdav_get_json_with(&agent, base_url, username, password)
}

fn webdav_get_json_with(
    agent: &ureq::Agent,
    base_url: &str,
    username: &str,
    password: &str,
) -> Result<String> {
    let url = webdav_file_url(base_url)?;
    let auth = webdav_auth_header(username, password);
    let req = webdav_auth_req(agent.get(&url), auth.as_deref());
    req.call()
        .map_err(webdav_error)?
        .into_string()
        .map_err(|e| anyhow::anyhow!("{e}"))
}

/// WebDAV config sync (#185): settings and manual upload.
pub(super) fn wire_webdav_upload(ctx: &WinCtx) {
    let WinCtx { store, window, .. } = ctx;
    // WebDAV config sync (#185): manual upload/download of the portable session
    // export JSON. It is intentionally not automatic on startup.
    {
        let s = store.borrow();
        window.set_webdav_enabled(s.webdav_enabled());
        window.set_webdav_url(s.webdav_url().into());
        window.set_webdav_username(s.webdav_username().into());
        window.set_webdav_password(s.webdav_password().into());
        window.set_webdav_accept_invalid_certs(s.webdav_accept_invalid_certs());
        window.set_webdav_status(String::new().into());
    }
    {
        let store = store.clone();
        window.on_save_webdav_settings(
            move |enabled: bool,
                  url: SharedString,
                  username: SharedString,
                  password: SharedString,
                  accept_invalid_certs: bool| {
                let mut s = store.borrow_mut();
                s.set_webdav_settings(
                    enabled,
                    url.to_string(),
                    username.to_string(),
                    password.to_string(),
                    accept_invalid_certs,
                );
                let _ = s.save();
            },
        );
    }
    {
        let weak = window.as_weak();
        let store = store.clone();
        window.on_webdav_upload(move || {
            let Some(w) = weak.upgrade() else { return };
            let enabled = w.get_webdav_enabled();
            let url = w.get_webdav_url().to_string();
            let username = w.get_webdav_username().to_string();
            let password = w.get_webdav_password().to_string();
            let accept_invalid_certs = w.get_webdav_accept_invalid_certs();
            {
                let mut s = store.borrow_mut();
                s.set_webdav_settings(
                    enabled,
                    url.clone(),
                    username.clone(),
                    password.clone(),
                    accept_invalid_certs,
                );
                let _ = s.save();
            }
            if !enabled {
                w.set_webdav_status(t("请先启用 WebDAV 同步", "enable WebDAV sync first").into());
                return;
            }
            let updated_at = now_ms();
            let device_id = SyncState::load().device_id;
            let res = store
                .borrow()
                .export_backup_json(updated_at, &device_id)
                .and_then(|(json, count)| {
                    webdav_put_json(
                        &url,
                        &username,
                        &password,
                        accept_invalid_certs,
                        json,
                    )
                    .map(|_| count)
                });
            let msg = match res {
                Ok(n) => {
                    let mut state = SyncState::load();
                    state.last_remote_updated = updated_at;
                    state.save();
                    format!(
                        "{} {} {}",
                        t("已上传", "uploaded"),
                        n,
                        t("个连接及全部设置", "connections and all settings")
                    )
                }
                Err(e) => format!("{}: {}", t("上传失败", "upload failed"), e),
            };
            w.set_webdav_status(msg.into());
        });
    }
}

/// WebDAV config sync (#185): manual download and import.
pub(super) fn wire_webdav_download(ctx: &WinCtx, sessions_model: &Rc<VecModel<SessionInfo>>) {
    let WinCtx {
        store,
        registry,
        window,
        ..
    } = ctx;
    {
        let weak = window.as_weak();
        let store = store.clone();
        let sessions_model = sessions_model.clone();
        let registry = registry.clone();
        window.on_webdav_download(move || {
            let Some(w) = weak.upgrade() else { return };
            let enabled = w.get_webdav_enabled();
            let url = w.get_webdav_url().to_string();
            let username = w.get_webdav_username().to_string();
            let password = w.get_webdav_password().to_string();
            let accept_invalid_certs = w.get_webdav_accept_invalid_certs();
            {
                let mut s = store.borrow_mut();
                s.set_webdav_settings(
                    enabled,
                    url.clone(),
                    username.clone(),
                    password.clone(),
                    accept_invalid_certs,
                );
                let _ = s.save();
            }
            if !enabled {
                w.set_webdav_status(t("请先启用 WebDAV 同步", "enable WebDAV sync first").into());
                return;
            }
            let res = webdav_get_json(
                &url,
                &username,
                &password,
                accept_invalid_certs,
            )
            .and_then(|json| {
                // The import saves the profile itself; keep auto-upload from
                // echoing it straight back as a new revision.
                suppress_auto_upload(2000);
                let updated_at = backup_meta(&json).map(|(_, updated)| updated);
                let summary = store.borrow_mut().import_backup_json(&json)?;
                if let Some(updated_at) = updated_at {
                    let mut state = SyncState::load();
                    state.last_remote_updated = updated_at.max(state.last_remote_updated);
                    state.save();
                }
                Ok(summary)
            });
            let msg = match res {
                Ok(summary) => {
                    sync_sessions_for_window(&weak, &store.borrow(), &sessions_model);
                    registry.broadcast_config_changed();
                    if summary.settings {
                        format!(
                            "{} {} {}、{} {}；{}",
                            t("已下载", "downloaded"),
                            summary.added,
                            t("个连接", "connections"),
                            summary.quick_commands,
                            t("条快捷命令", "quick commands"),
                            t("布局类设置重启后生效", "layout settings apply after restart")
                        )
                    } else {
                        format!(
                            "{} {}, {} {}",
                            t("已导入", "imported"),
                            summary.added,
                            t("跳过", "skipped"),
                            summary.skipped
                        )
                    }
                }
                Err(e) => format!("{}: {}", t("下载失败", "download failed"), e),
            };
            w.set_webdav_status(msg.into());
        });
    }
}

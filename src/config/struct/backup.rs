use serde::{Deserialize, Serialize};

use super::ConfigFile;

/// Full-profile backup exchanged through WebDAV. Sessions' secrets are
/// re-encrypted with the portable export key so another machine can restore
/// them; `webdav_password` is stripped before upload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupFile {
    pub meatshell_backup: u32,
    /// Milliseconds since the Unix epoch, set by the writer.
    pub updated_at: u64,
    /// Random per-install id; lets a device recognise its own uploads.
    pub device_id: String,
    pub config: ConfigFile,
}

/// Outcome of importing a backup; counts only, no connection details.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackupImport {
    pub added: usize,
    pub skipped: usize,
    pub quick_commands: usize,
    /// True when the backup also replaced global settings (v2).
    pub settings: bool,
}

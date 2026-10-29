use std::path::PathBuf;

use super::ConfigFile;

pub struct ConfigStore {
    pub(crate) path: PathBuf,
    pub(crate) backup_dir: Option<PathBuf>,
    pub(crate) cache: ConfigFile,
    /// Ciphertext snapshot used to reject stale full-file writes.
    pub(crate) disk_snapshot: std::cell::RefCell<Option<String>>,
    /// ChaCha20-Poly1305 key loaded from (or freshly generated into)
    /// `secret.key` in the same directory as `sessions.json`.
    pub(crate) key: [u8; 32],
    /// Fired after a successful save; used by WebDAV auto-upload. Runs on the
    /// saving thread and must not touch the store through its RefCell.
    pub(crate) save_hook:
        std::cell::RefCell<Option<std::sync::Arc<dyn Fn(&ConfigStore) + Send + Sync>>>,
}

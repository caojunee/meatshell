use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Commands sent to the SFTP worker task from the UI thread.
#[derive(Debug)]
pub enum SftpCommand {
    /// List the contents of a remote directory.
    ListDir(String),
    /// Fill owner/group names after the initial listing is already visible.
    EnrichEntries { path: String, entries: Vec<crate::ssh::RemoteEntry> },
    /// Refresh button: re-list the directory *and* re-sync the whole expanded
    /// left tree, so external/own changes (deleted/created dirs) show up without
    /// a reconnect (#189). Plain navigation uses `ListDir` to avoid the extra
    /// per-click tree round-trips.
    RefreshDir(String),
    /// Toggle a directory node in the tree (expand if collapsed, collapse if expanded).
    ToggleTreeNode(String),
    /// Download a remote file to a local directory.
    Download {
        remote: String,
        local_dir: String,
        conflict: DownloadConflict,
    },
    /// Multi-select download (#100): tar the named entries under `remote_dir`
    /// into one archive on the remote, download it, then delete the temp.
    DownloadArchive {
        remote_dir: String,
        names: Vec<String>,
        local_dir: String,
    },
    /// Cancel an in-progress transfer by its id (#100). The partial local file
    /// (and any remote temp archive) are cleaned up.
    CancelTransfer(String),
    /// Upload a local file into a remote directory.
    Upload {
        local: PathBuf,
        remote_dir: String,
        cleanup_after: Option<PathBuf>,
    },
    /// Re-upload an externally edited temp file to its exact original path.
    /// The local name may include a host prefix, so deriving the remote name
    /// from it would overwrite the wrong file (#318).
    UploadEdited { local: PathBuf, remote: String },
    /// Copy remote entries from this session into another SFTP session.
    CopyTo {
        remotes: Vec<String>,
        target: UnboundedSender<SftpCommand>,
        target_dir: String,
    },
    /// Paste entries copied within MeatShell into `dest_dir` on the *same*
    /// session: run a server-side `cp -a` so the bytes never round-trip through
    /// the local machine (copying a huge file stays instant).
    CopyRemote { paths: Vec<String>, dest_dir: String },
    /// Delete a remote file (falls back to removing an empty directory).
    Delete(String),
    /// Download a file to a temp dir and open it with the OS default app
    /// ("Open/Edit externally", #81). When `edit` is set, watch the temp copy
    /// and re-upload on every change.
    OpenTemp { remote: String, edit: bool },
    /// Rename / move a remote file or directory (#69).
    Rename { from: String, to: String },
    /// Change a remote path's permission bits (POSIX mode, e.g. 0o755) (#69).
    Chmod { path: String, mode: u32 },
    /// Create an empty remote directory (#69).
    MkDir(String),
    /// Create an empty remote file (#69).
    TouchFile(String),
    /// Read a remote file's text for the built-in viewer/editor (#70).
    ReadText { remote: String, edit: bool },
    /// Double-click on a file (#493): open it in the built-in editor when it
    /// is text the editor accepts, otherwise ask the UI to download it.
    OpenOrDownload { remote: String },
    /// Overwrite a remote file with text from the built-in editor (#70).
    WriteText { remote: String, content: String },
    /// Drag-out to the OS file manager (#504): stat `roots` and walk any
    /// folders, replying with every file/folder to create on the local side.
    WalkForExport {
        roots: Vec<String>,
        reply: std::sync::mpsc::Sender<Result<Vec<ExportEntry>, String>>,
    },
    /// Drag-out (#504): stream one remote file's bytes, in order, into the
    /// bounded `chunks` channel. The stream ends when the sender is dropped;
    /// an `Err` chunk reports a failure. Dropping the receiver cancels it.
    StreamFile {
        remote: String,
        chunks: tokio::sync::mpsc::Sender<Result<Vec<u8>, String>>,
        /// The drag this file belongs to: one Transfers-panel row per drag.
        progress: Arc<DragTransfer>,
    },
    /// Drag-out finished (Explorer released the data): report the drag's
    /// final state in the Transfers panel.
    FinishDragTransfer(Arc<DragTransfer>),
    /// Gracefully shut down the SFTP worker.
    Close,
}

/// One item to materialise when remote files are dragged out (#504).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExportEntry {
    /// Absolute remote path.
    pub remote: String,
    /// Path relative to the drop target, `/`-separated, starting with the
    /// dragged item's own name (`logs/2026/app.log`).
    pub relative: String,
    pub is_dir: bool,
    pub size: u64,
    /// Unix seconds; 0 when unknown.
    pub mtime: u64,
}

/// Progress of one drag-out (#504), shown as a single row in the Transfers
/// panel however many files Explorer pulls. Shared between the UI-side data
/// object (which creates and finishes it) and the worker's stream tasks
/// (which add bytes and emit progress).
#[derive(Debug)]
pub struct DragTransfer {
    pub id: String,
    pub name: String,
    /// Sum of all file sizes in the drag.
    pub total: u64,
    /// Bytes delivered so far, across all files.
    pub done: std::sync::atomic::AtomicU64,
    /// Flipped by the panel's Cancel button (registered as a transfer id).
    pub cancel: Arc<std::sync::atomic::AtomicBool>,
    /// A file stopped before its end (Explorer cancelled / skipped it).
    pub interrupted: std::sync::atomic::AtomicBool,
    pub(crate) report: Mutex<DragReport>,
}

/// Emission bookkeeping for [`DragTransfer`].
#[derive(Debug, Default)]
pub(crate) struct DragReport {
    /// A row exists in the panel (some bytes were requested).
    pub started: bool,
    /// The final state was emitted; later progress is ignored.
    pub finished: bool,
    pub error: Option<String>,
    pub last_emit: Option<std::time::Instant>,
}

impl DragTransfer {
    pub fn new(id: String, name: String, total: u64) -> Self {
        Self {
            id,
            name,
            total,
            done: Default::default(),
            cancel: Default::default(),
            interrupted: Default::default(),
            report: Default::default(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DownloadConflict {
    Replace,
    KeepBoth,
}

/// Handle retained by the UI to drive a running SFTP worker.
pub struct SftpHandle {
    pub commands: UnboundedSender<SftpCommand>,
    #[allow(dead_code)]
    pub join: JoinHandle<()>,
}

pub(crate) type SftpHandles = Arc<Mutex<HashMap<String, SftpHandle>>>;

/// Last terminal cwd followed by each SFTP panel.
pub(crate) type SftpLastCwd = Arc<Mutex<HashMap<String, String>>>;

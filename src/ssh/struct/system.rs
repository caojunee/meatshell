/// One process row sampled from the remote `ps` (#23). CPU/mem are percentages
/// as reported by `ps` (pcpu/pmem); `command` is the (width-truncated) args.
#[derive(Debug, Clone)]
pub struct ProcInfo {
    pub pid: u32,
    pub user: String,
    pub cpu: f32,
    pub mem: f32,
    pub command: String,
}

/// One socket row for the bottom panel's 网络 (Network) tab, sampled from
/// `ss -tunap` (or `netstat -tunap` as a fallback). `pid`/`process` are empty
/// when the process info is not visible to the connecting user.
#[derive(Debug, Clone, Default)]
pub struct NetConnection {
    pub proto: String,   // tcp / udp (with suffix like tcp6)
    pub local: String,   // local address:port
    pub remote: String,  // peer address:port
    pub state: String,   // ESTAB / LISTEN / UNCONN / …
    pub pid: String,
    pub process: String,
    /// TCP byte counters from `ss -i`'s tcp_info (`bytes_acked` / `bytes_sent`
    /// for upload, `bytes_received` for download). Only TCP non-LISTEN sockets
    /// have them; `has_bytes` is false for UDP / listening sockets.
    pub sent: u64,
    pub received: u64,
    pub has_bytes: bool,
    /// Bytes per second since the previous sample (0 on the first sample of a
    /// connection).
    pub up_rate: f64,
    pub down_rate: f64,
}

#[derive(Debug, Clone, Default)]
pub struct SystemDetails {
    pub overview: Vec<(String, String)>,
    pub cpu_info: Vec<(String, String)>,
    pub gpu_info: Vec<(String, String)>,
    pub cpu_usage: Vec<(String, String)>,
    pub memory: Vec<(String, String)>,
    pub swap: Vec<(String, String)>,
    pub networks: Vec<(String, String, String, String, String)>,
    pub filesystems: Vec<(String, String, String, String, String)>,
}

/// One SSH tunnel row shown in the runtime tunnel panel (#206).
#[derive(Debug, Clone)]
pub struct RuntimeTunnelInfo {
    pub id: String,
    pub name: String,
    pub kind: String,
    pub bind_addr: String,
    pub bind_port: u16,
    pub host: String,
    pub host_port: u16,
    pub active: bool,
    pub status: String,
}

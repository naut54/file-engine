use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct Progress {
    pub bytes_done: u64,
    pub bytes_total: u64,
    pub files_done: u64,
    pub files_total: u64,
    pub current_file: Option<PathBuf>,
}

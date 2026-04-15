use std::fs;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Sender};
use std::sync::OnceLock;
use std::thread;

static CLEANUP_QUEUE: OnceLock<Sender<PathBuf>> = OnceLock::new();

pub fn enqueue_cleanup(path: PathBuf) {
    if let Some(sender) = CLEANUP_QUEUE
        .get_or_init(start_cleanup_worker)
        .send(path.clone())
        .err()
    {
        let fallback = sender.0;
        let _ = thread::Builder::new()
            .name("compaction-cleanup-fallback".to_string())
            .spawn(move || {
                let _ = cleanup_path(&fallback);
            });
    }
}

pub fn cleanup_now(path: impl AsRef<Path>) {
    let _ = cleanup_path(path.as_ref());
}

pub fn sweep_staging_root(root: impl AsRef<Path>) {
    let root = root.as_ref();
    if let Ok(entries) = fs::read_dir(root) {
        for entry in entries.flatten() {
            enqueue_cleanup(entry.path());
        }
    }
}

fn start_cleanup_worker() -> Sender<PathBuf> {
    let (tx, rx) = mpsc::channel::<PathBuf>();
    let _ = thread::Builder::new()
        .name("compaction-cleanup".to_string())
        .spawn(move || {
            while let Ok(path) = rx.recv() {
                let _ = cleanup_path(&path);
            }
        });
    tx
}

fn cleanup_path(path: &Path) -> std::io::Result<()> {
    if !path.exists() {
        return Ok(());
    }
    if path.is_dir() {
        fs::remove_dir_all(path)
    } else {
        fs::remove_file(path)
    }
}

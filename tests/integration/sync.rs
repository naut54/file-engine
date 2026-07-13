use std::fs;

use file_engine::FileEngine;

#[tokio::test]
async fn additive_sync_copies_new_files() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src");
    let dst = dir.path().join("dst");
    fs::create_dir_all(&src).unwrap();
    fs::write(src.join("a.txt"), b"a").unwrap();

    let engine = FileEngine::new();
    let summary = engine.sync(&src, &dst).start().unwrap().await.unwrap();

    assert_eq!(summary.copied, 1);
    assert_eq!(fs::read(dst.join("a.txt")).unwrap(), b"a");
}

#[tokio::test]
async fn detects_updated_files_by_size() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src");
    let dst = dir.path().join("dst");
    fs::create_dir_all(&src).unwrap();
    fs::create_dir_all(&dst).unwrap();
    fs::write(src.join("a.txt"), b"new content").unwrap();
    fs::write(dst.join("a.txt"), b"old").unwrap();

    let engine = FileEngine::new();
    let summary = engine.sync(&src, &dst).start().unwrap().await.unwrap();

    assert_eq!(summary.updated, 1);
    assert_eq!(fs::read(dst.join("a.txt")).unwrap(), b"new content");
}

#[tokio::test]
async fn skips_unchanged_files() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src");
    let dst = dir.path().join("dst");
    fs::create_dir_all(&src).unwrap();
    fs::write(src.join("a.txt"), b"a").unwrap();

    let engine = FileEngine::new();
    engine.sync(&src, &dst).start().unwrap().await.unwrap();

    // Second sync of the same unchanged tree should be a no-op copy-wise.
    let summary = engine.sync(&src, &dst).start().unwrap().await.unwrap();
    assert_eq!(summary.copied, 0);
    assert_eq!(summary.updated, 0);
    assert_eq!(summary.skipped, 1);
}

#[tokio::test]
async fn delete_extraneous_removes_files_absent_from_src() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src");
    let dst = dir.path().join("dst");
    fs::create_dir_all(&src).unwrap();
    fs::create_dir_all(&dst).unwrap();
    fs::write(dst.join("extra.txt"), b"extra").unwrap();

    let engine = FileEngine::new();

    // Without delete_extraneous, the extra file survives.
    engine.sync(&src, &dst).start().unwrap().await.unwrap();
    assert!(dst.join("extra.txt").exists());

    // With it, it's removed.
    let summary = engine
        .sync(&src, &dst)
        .delete_extraneous(true)
        .start()
        .unwrap()
        .await
        .unwrap();
    assert_eq!(summary.deleted, 1);
    assert!(!dst.join("extra.txt").exists());
}

#[cfg(feature = "checksum")]
#[tokio::test]
async fn compare_by_hash_catches_same_size_same_mtime_different_content() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src");
    let dst = dir.path().join("dst");
    fs::create_dir_all(&src).unwrap();
    fs::create_dir_all(&dst).unwrap();

    fs::write(src.join("a.txt"), b"AAAAA").unwrap();
    fs::write(dst.join("a.txt"), b"BBBBB").unwrap();

    // Force identical mtimes so size+mtime comparison alone can't tell
    // these apart — only `compare_by_hash` should catch the difference.
    let shared_mtime = std::time::SystemTime::now();
    fs::File::open(src.join("a.txt"))
        .unwrap()
        .set_modified(shared_mtime)
        .unwrap();
    fs::File::open(dst.join("a.txt"))
        .unwrap()
        .set_modified(shared_mtime)
        .unwrap();

    let engine = FileEngine::new();
    let summary = engine
        .sync(&src, &dst)
        .compare_by_hash(true)
        .start()
        .unwrap()
        .await
        .unwrap();

    assert_eq!(summary.updated, 1);
    assert_eq!(fs::read(dst.join("a.txt")).unwrap(), b"AAAAA");
}

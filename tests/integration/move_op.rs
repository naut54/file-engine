use std::fs;

use file_engine::{FileEngine, FileEngineError};

#[tokio::test]
async fn moves_a_single_file_same_filesystem() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src.txt");
    let dst = dir.path().join("dst.txt");
    fs::write(&src, b"hello world").unwrap();

    let engine = FileEngine::new();
    engine.move_path(&src, &dst).start().unwrap().await.unwrap();

    assert!(!src.exists());
    assert_eq!(fs::read(&dst).unwrap(), b"hello world");
}

#[tokio::test]
async fn moves_a_directory_tree() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src");
    let dst = dir.path().join("dst");
    fs::create_dir_all(src.join("nested")).unwrap();
    fs::write(src.join("a.txt"), b"a").unwrap();
    fs::write(src.join("nested/b.txt"), b"b").unwrap();

    let engine = FileEngine::new();
    engine.move_path(&src, &dst).start().unwrap().await.unwrap();

    assert!(!src.exists());
    assert_eq!(fs::read(dst.join("a.txt")).unwrap(), b"a");
    assert_eq!(fs::read(dst.join("nested/b.txt")).unwrap(), b"b");
}

#[tokio::test]
async fn refuses_to_overwrite_by_default() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src.txt");
    let dst = dir.path().join("dst.txt");
    fs::write(&src, b"new").unwrap();
    fs::write(&dst, b"old").unwrap();

    let engine = FileEngine::new();
    let result = engine.move_path(&src, &dst).start().unwrap().await;

    assert!(matches!(result, Err(FileEngineError::DestinationExists(_))));
    assert!(src.exists());
    assert_eq!(fs::read(&dst).unwrap(), b"old");
}

use std::fs;

use file_engine::{FileEngine, FileEngineError};

#[tokio::test]
async fn copies_a_single_file() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src.txt");
    let dst = dir.path().join("dst.txt");
    fs::write(&src, b"hello world").unwrap();

    let engine = FileEngine::new();
    engine.copy(&src, &dst).start().unwrap().await.unwrap();

    assert_eq!(fs::read(&dst).unwrap(), b"hello world");
}

#[tokio::test]
async fn refuses_to_overwrite_by_default() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src.txt");
    let dst = dir.path().join("dst.txt");
    fs::write(&src, b"new").unwrap();
    fs::write(&dst, b"old").unwrap();

    let engine = FileEngine::new();
    let result = engine.copy(&src, &dst).start().unwrap().await;

    assert!(matches!(result, Err(FileEngineError::DestinationExists(_))));
    assert_eq!(fs::read(&dst).unwrap(), b"old");
}

#[tokio::test]
async fn overwrite_true_replaces_destination() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src.txt");
    let dst = dir.path().join("dst.txt");
    fs::write(&src, b"new").unwrap();
    fs::write(&dst, b"old").unwrap();

    let engine = FileEngine::new();
    engine
        .copy(&src, &dst)
        .overwrite(true)
        .start()
        .unwrap()
        .await
        .unwrap();

    assert_eq!(fs::read(&dst).unwrap(), b"new");
}

#[tokio::test]
async fn copies_a_directory_tree() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src");
    let dst = dir.path().join("dst");
    fs::create_dir_all(src.join("nested")).unwrap();
    fs::write(src.join("a.txt"), b"a").unwrap();
    fs::write(src.join("nested/b.txt"), b"b").unwrap();

    let engine = FileEngine::new();
    engine.copy(&src, &dst).start().unwrap().await.unwrap();

    assert_eq!(fs::read(dst.join("a.txt")).unwrap(), b"a");
    assert_eq!(fs::read(dst.join("nested/b.txt")).unwrap(), b"b");
}

#[tokio::test]
async fn cancellation_before_start_is_observed() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src.txt");
    let dst = dir.path().join("dst.txt");
    fs::write(&src, vec![0u8; 1024 * 1024]).unwrap();

    let engine = FileEngine::new();
    let token = tokio_util::sync::CancellationToken::new();
    token.cancel();

    let result = engine
        .copy(&src, &dst)
        .cancellation_token(token)
        .start()
        .unwrap()
        .await;

    assert!(matches!(result, Err(FileEngineError::Cancelled)));
}

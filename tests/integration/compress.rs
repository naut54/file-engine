use std::fs;

use file_engine::FileEngine;

#[tokio::test]
async fn round_trips_a_directory_through_zip() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src");
    let archive = dir.path().join("archive.zip");
    let extracted = dir.path().join("extracted");
    fs::create_dir_all(src.join("nested")).unwrap();
    fs::write(src.join("a.txt"), b"a").unwrap();
    fs::write(src.join("nested/b.txt"), b"b").unwrap();

    let engine = FileEngine::new();
    engine
        .compress(&src, &archive)
        .start()
        .unwrap()
        .await
        .unwrap();
    assert!(archive.exists());

    engine
        .decompress(&archive, &extracted)
        .start()
        .unwrap()
        .await
        .unwrap();

    assert_eq!(fs::read(extracted.join("a.txt")).unwrap(), b"a");
    assert_eq!(fs::read(extracted.join("nested/b.txt")).unwrap(), b"b");
}

#[tokio::test]
async fn round_trips_a_single_file_through_gzip() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src.txt");
    let archive = dir.path().join("src.txt.gz");
    let extracted = dir.path().join("extracted.txt");
    fs::write(&src, b"hello world").unwrap();

    let engine = FileEngine::new();
    engine
        .compress(&src, &archive)
        .start()
        .unwrap()
        .await
        .unwrap();
    assert!(archive.exists());

    engine
        .decompress(&archive, &extracted)
        .start()
        .unwrap()
        .await
        .unwrap();

    assert_eq!(fs::read(&extracted).unwrap(), b"hello world");
}

#[tokio::test]
async fn gzip_on_a_directory_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src");
    fs::create_dir_all(&src).unwrap();
    let archive = dir.path().join("archive.gz");

    let engine = FileEngine::new();
    let result = engine
        .compress(&src, &archive)
        .format(file_engine::CompressFormat::Gzip)
        .start()
        .unwrap()
        .await;

    assert!(matches!(
        result,
        Err(file_engine::FileEngineError::GzipRequiresFile(_))
    ));
}

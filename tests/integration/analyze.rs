use std::fs;

use file_engine::{FileEngine, FileKind};

#[tokio::test]
async fn reports_kind_and_size_for_a_fixture_tree() {
    let dir = tempfile::tempdir().unwrap();
    fs::create_dir_all(dir.path().join("nested")).unwrap();
    fs::write(dir.path().join("a.txt"), b"hello").unwrap();
    fs::write(dir.path().join("nested/b.txt"), b"hi").unwrap();

    let engine = FileEngine::new();
    let results = engine.analyze(dir.path()).start().unwrap().await.unwrap();

    let a = results
        .iter()
        .find(|f| f.path.ends_with("a.txt"))
        .expect("a.txt should be present");
    assert!(matches!(a.kind, FileKind::File));
    assert_eq!(a.size, 5);

    let nested = results
        .iter()
        .find(|f| f.path.ends_with("nested"))
        .expect("nested dir should be present");
    assert!(matches!(nested.kind, FileKind::Directory));
}

#[tokio::test]
async fn detects_content_type_from_magic_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let png = dir.path().join("fixture.png");
    // Minimal PNG signature — enough for `infer` to recognize the format
    // without a full, valid PNG payload.
    fs::write(&png, [0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A]).unwrap();

    let engine = FileEngine::new();
    let results = engine.analyze(dir.path()).start().unwrap().await.unwrap();

    let entry = results
        .iter()
        .find(|f| f.path.ends_with("fixture.png"))
        .expect("fixture.png should be present");
    assert_eq!(entry.content_type, Some("image/png"));
}

#[cfg(feature = "checksum")]
#[tokio::test]
async fn with_hash_produces_a_stable_hash() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("a.txt"), b"hello").unwrap();

    let engine = FileEngine::new();
    let results = engine
        .analyze(dir.path())
        .with_hash(true)
        .start()
        .unwrap()
        .await
        .unwrap();

    let a = results
        .iter()
        .find(|f| f.path.ends_with("a.txt"))
        .expect("a.txt should be present");

    let expected = blake3::hash(b"hello").to_hex().to_string();
    assert_eq!(a.hash.as_deref(), Some(expected.as_str()));
}

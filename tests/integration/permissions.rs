use std::fs;
use std::os::unix::fs::PermissionsExt;

use file_engine::FileEngine;

#[tokio::test]
async fn preserve_permissions_true_carries_the_executable_bit() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src.sh");
    let dst = dir.path().join("dst.sh");
    fs::write(&src, b"#!/bin/sh\necho hi\n").unwrap();
    fs::set_permissions(&src, fs::Permissions::from_mode(0o755)).unwrap();

    let engine = FileEngine::new();
    engine
        .copy(&src, &dst)
        .preserve_permissions(true)
        .start()
        .unwrap()
        .await
        .unwrap();

    let dst_mode = fs::metadata(&dst).unwrap().permissions().mode();
    assert_eq!(dst_mode & 0o777, 0o755);
}

#[tokio::test]
async fn preserve_permissions_false_does_not_carry_the_executable_bit() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src.sh");
    let dst = dir.path().join("dst.sh");
    fs::write(&src, b"#!/bin/sh\necho hi\n").unwrap();
    fs::set_permissions(&src, fs::Permissions::from_mode(0o755)).unwrap();

    let engine = FileEngine::new();
    engine.copy(&src, &dst).start().unwrap().await.unwrap();

    let dst_mode = fs::metadata(&dst).unwrap().permissions().mode();
    assert_ne!(dst_mode & 0o777, 0o755);
}

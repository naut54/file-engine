//! Run with: cargo run --example with_diagnostics --features diagnostics

#[cfg(feature = "diagnostics")]
use file_engine::FileEngine;

#[cfg(feature = "diagnostics")]
#[tokio::main]
async fn main() {
    let engine = FileEngine::new();

    let handle = engine
        .copy("src.txt", "dst.txt")
        .start()
        .expect("failed to start copy");

    if let Err(err) = handle.await {
        // TODO(implementation): once `error-engine`'s presentation API is
        // confirmed, format `err` through it here (code/severity/context).
        eprintln!("copy failed: {err}");
    }
}

#[cfg(not(feature = "diagnostics"))]
fn main() {
    eprintln!("this example requires --features diagnostics");
}

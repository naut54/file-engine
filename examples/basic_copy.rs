use file_engine::FileEngine;

#[tokio::main]
async fn main() -> file_engine::Result<()> {
    let engine = FileEngine::new();

    let mut handle = engine.copy("src.txt", "dst.txt").overwrite(true).start()?;

    while let Some(progress) = tokio_stream::StreamExt::next(handle.progress()).await {
        println!("{:?}", progress);
    }

    handle.await?;
    Ok(())
}

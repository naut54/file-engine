use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use tokio::sync::mpsc::UnboundedSender;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::sync::CancellationToken;

use crate::engine::FileEngine;
use crate::error::{from_io, FileEngineError, Result};
use crate::handle::Handle;
use crate::progress::Progress;

/// See design doc §8.4. `Zip` handles a single file or a directory tree;
/// `Gzip` is a single compressed stream and therefore only accepts a single
/// file as source (there is no `tar` dependency to fold a directory into a
/// `.tar.gz`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressFormat {
    Zip,
    Gzip,
}

fn infer_format(path: &Path) -> Option<CompressFormat> {
    match path.extension().and_then(|ext| ext.to_str()) {
        Some("zip") => Some(CompressFormat::Zip),
        Some("gz") => Some(CompressFormat::Gzip),
        _ => None,
    }
}

fn zip_error(path: &Path, err: zip::result::ZipError) -> FileEngineError {
    from_io(path.to_path_buf(), std::io::Error::other(err))
}

/// `compress` doesn't depend on `analyze` (§4 of the design doc — the two
/// are independent features), so this can't reuse `analyze`'s walkdir error
/// mapping; small enough to duplicate rather than introduce a shared
/// always-on module for it.
fn walkdir_error(err: walkdir::Error) -> FileEngineError {
    let path = err.path().map(Path::to_path_buf).unwrap_or_default();
    match err.into_io_error() {
        Some(io_err) => from_io(path, io_err),
        None => FileEngineError::Io {
            path,
            source: std::io::Error::other("walk error with no underlying io::Error"),
        },
    }
}

/// Bundles per-call context for `copy_with_progress` — `Zip` (per-entry)
/// and `Gzip` (single-stream) both need the same handful of values, and
/// clippy's `too_many_arguments` flags them as separate parameters.
struct CopyProgressCtx<'a> {
    buffer_size: usize,
    current_file: &'a Path,
    files_done: u64,
    files_total: u64,
    progress_tx: &'a UnboundedSender<Progress>,
    cancel_token: &'a CancellationToken,
}

/// Copies `reader` into `writer` in chunks, emitting `Progress` after each
/// chunk and checking `cancel_token` before each read. Shared by the `Zip`
/// per-entry writes and the `Gzip` single-stream path.
fn copy_with_progress(
    mut reader: impl Read,
    mut writer: impl Write,
    ctx: &CopyProgressCtx,
) -> Result<()> {
    let mut buf = vec![0u8; ctx.buffer_size.max(1)];
    let mut bytes_done = 0u64;

    loop {
        if ctx.cancel_token.is_cancelled() {
            return Err(FileEngineError::Cancelled);
        }

        let n = reader
            .read(&mut buf)
            .map_err(|e| from_io(ctx.current_file.to_path_buf(), e))?;
        if n == 0 {
            break;
        }

        writer
            .write_all(&buf[..n])
            .map_err(|e| from_io(ctx.current_file.to_path_buf(), e))?;

        bytes_done += n as u64;
        let _ = ctx.progress_tx.send(Progress {
            bytes_done,
            bytes_total: 0,
            files_done: ctx.files_done,
            files_total: ctx.files_total,
            current_file: Some(ctx.current_file.to_path_buf()),
        });
    }

    Ok(())
}

fn write_zip(
    src: &Path,
    dst: &Path,
    buffer_size: usize,
    progress_tx: &UnboundedSender<Progress>,
    cancel_token: &CancellationToken,
) -> Result<()> {
    let file = File::create(dst).map_err(|e| from_io(dst.to_path_buf(), e))?;
    let mut writer = zip::ZipWriter::new(file);
    let options = zip::write::SimpleFileOptions::default();

    let src_metadata = std::fs::symlink_metadata(src).map_err(|e| from_io(src.to_path_buf(), e))?;

    if src_metadata.is_dir() {
        let entries: Vec<walkdir::DirEntry> = walkdir::WalkDir::new(src)
            .into_iter()
            .filter(|e| !matches!(e, Ok(e) if e.path() == src))
            .collect::<walkdir::Result<Vec<_>>>()
            .map_err(walkdir_error)?;
        let files_total = entries.len() as u64;

        for (files_done, entry) in entries.into_iter().enumerate() {
            if cancel_token.is_cancelled() {
                return Err(FileEngineError::Cancelled);
            }

            let rel = entry.path().strip_prefix(src).unwrap_or(entry.path());
            let name = rel.to_string_lossy().replace('\\', "/");

            if entry.file_type().is_dir() {
                writer
                    .add_directory(format!("{name}/"), options)
                    .map_err(|e| zip_error(dst, e))?;
            } else {
                writer
                    .start_file(name, options)
                    .map_err(|e| zip_error(dst, e))?;
                let reader =
                    File::open(entry.path()).map_err(|e| from_io(entry.path().to_path_buf(), e))?;
                copy_with_progress(
                    reader,
                    &mut writer,
                    &CopyProgressCtx {
                        buffer_size,
                        current_file: entry.path(),
                        files_done: files_done as u64,
                        files_total,
                        progress_tx,
                        cancel_token,
                    },
                )?;
            }
        }
    } else {
        let name = src
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| "file".to_string());
        writer
            .start_file(name, options)
            .map_err(|e| zip_error(dst, e))?;
        let reader = File::open(src).map_err(|e| from_io(src.to_path_buf(), e))?;
        copy_with_progress(
            reader,
            &mut writer,
            &CopyProgressCtx {
                buffer_size,
                current_file: src,
                files_done: 0,
                files_total: 1,
                progress_tx,
                cancel_token,
            },
        )?;
    }

    writer.finish().map_err(|e| zip_error(dst, e))?;
    Ok(())
}

fn write_gzip(
    src: &Path,
    dst: &Path,
    buffer_size: usize,
    progress_tx: &UnboundedSender<Progress>,
    cancel_token: &CancellationToken,
) -> Result<()> {
    let reader = File::open(src).map_err(|e| from_io(src.to_path_buf(), e))?;
    let output = File::create(dst).map_err(|e| from_io(dst.to_path_buf(), e))?;
    let mut encoder = flate2::write::GzEncoder::new(output, flate2::Compression::default());

    copy_with_progress(
        reader,
        &mut encoder,
        &CopyProgressCtx {
            buffer_size,
            current_file: src,
            files_done: 0,
            files_total: 1,
            progress_tx,
            cancel_token,
        },
    )?;

    encoder
        .finish()
        .map_err(|e| from_io(dst.to_path_buf(), e))?;
    Ok(())
}

fn read_zip(src: &Path, dst: &Path) -> Result<()> {
    let file = File::open(src).map_err(|e| from_io(src.to_path_buf(), e))?;
    let mut archive = zip::ZipArchive::new(file).map_err(|e| zip_error(src, e))?;
    archive.extract(dst).map_err(|e| zip_error(src, e))?;
    Ok(())
}

fn read_gzip(
    src: &Path,
    dst: &Path,
    buffer_size: usize,
    progress_tx: &UnboundedSender<Progress>,
    cancel_token: &CancellationToken,
) -> Result<()> {
    let input = File::open(src).map_err(|e| from_io(src.to_path_buf(), e))?;
    let decoder = flate2::read::GzDecoder::new(input);
    let output = File::create(dst).map_err(|e| from_io(dst.to_path_buf(), e))?;

    copy_with_progress(
        decoder,
        output,
        &CopyProgressCtx {
            buffer_size,
            current_file: src,
            files_done: 0,
            files_total: 1,
            progress_tx,
            cancel_token,
        },
    )?;
    Ok(())
}

pub struct CompressBuilder {
    src: PathBuf,
    dst: PathBuf,
    format: Option<CompressFormat>,
    buffer_size: usize,
    cancel_token: Option<CancellationToken>,
}

impl CompressBuilder {
    pub(crate) fn new(src: PathBuf, dst: PathBuf, buffer_size: usize) -> Self {
        Self {
            src,
            dst,
            format: None,
            buffer_size,
            cancel_token: None,
        }
    }

    /// Optional — inferred from `dst`'s extension when omitted (`.zip` →
    /// `Zip`, `.gz` → `Gzip`).
    pub fn format(mut self, format: CompressFormat) -> Self {
        self.format = Some(format);
        self
    }

    pub fn cancellation_token(mut self, token: CancellationToken) -> Self {
        self.cancel_token = Some(token);
        self
    }

    pub fn start(self) -> Result<Handle<()>> {
        let cancel_token = self.cancel_token.unwrap_or_default();
        let (progress_tx, progress_rx) = tokio::sync::mpsc::unbounded_channel();

        let task_cancel_token = cancel_token.clone();
        let buffer_size = self.buffer_size;
        let src = self.src;
        let dst = self.dst;
        let requested_format = self.format;

        let join = tokio::spawn(async move {
            let format = match requested_format.or_else(|| infer_format(&dst)) {
                Some(format) => format,
                None => return Err(FileEngineError::UnknownCompressFormat(dst)),
            };

            if format == CompressFormat::Gzip && src.is_dir() {
                return Err(FileEngineError::GzipRequiresFile(src));
            }

            tokio::task::spawn_blocking(move || match format {
                CompressFormat::Zip => {
                    write_zip(&src, &dst, buffer_size, &progress_tx, &task_cancel_token)
                }
                CompressFormat::Gzip => {
                    write_gzip(&src, &dst, buffer_size, &progress_tx, &task_cancel_token)
                }
            })
            .await
            .map_err(|e| FileEngineError::Io {
                path: PathBuf::new(),
                source: std::io::Error::other(e),
            })?
        });

        Ok(Handle {
            join,
            progress_rx: UnboundedReceiverStream::new(progress_rx),
            cancel_token,
        })
    }
}

pub struct DecompressBuilder {
    src: PathBuf,
    dst: PathBuf,
    format: Option<CompressFormat>,
    buffer_size: usize,
    cancel_token: Option<CancellationToken>,
}

impl DecompressBuilder {
    pub(crate) fn new(src: PathBuf, dst: PathBuf, buffer_size: usize) -> Self {
        Self {
            src,
            dst,
            format: None,
            buffer_size,
            cancel_token: None,
        }
    }

    /// Optional — inferred from `src`'s extension when omitted.
    pub fn format(mut self, format: CompressFormat) -> Self {
        self.format = Some(format);
        self
    }

    pub fn cancellation_token(mut self, token: CancellationToken) -> Self {
        self.cancel_token = Some(token);
        self
    }

    pub fn start(self) -> Result<Handle<()>> {
        let cancel_token = self.cancel_token.unwrap_or_default();
        let (progress_tx, progress_rx) = tokio::sync::mpsc::unbounded_channel();

        let task_cancel_token = cancel_token.clone();
        let buffer_size = self.buffer_size;
        let src = self.src;
        let dst = self.dst;
        let requested_format = self.format;

        let join = tokio::spawn(async move {
            let format = match requested_format.or_else(|| infer_format(&src)) {
                Some(format) => format,
                None => return Err(FileEngineError::UnknownCompressFormat(src)),
            };

            tokio::task::spawn_blocking(move || match format {
                CompressFormat::Zip => read_zip(&src, &dst),
                CompressFormat::Gzip => {
                    read_gzip(&src, &dst, buffer_size, &progress_tx, &task_cancel_token)
                }
            })
            .await
            .map_err(|e| FileEngineError::Io {
                path: PathBuf::new(),
                source: std::io::Error::other(e),
            })?
        });

        Ok(Handle {
            join,
            progress_rx: UnboundedReceiverStream::new(progress_rx),
            cancel_token,
        })
    }
}

#[cfg(feature = "compress")]
impl FileEngine {
    pub fn compress(&self, src: impl Into<PathBuf>, dst: impl Into<PathBuf>) -> CompressBuilder {
        CompressBuilder::new(src.into(), dst.into(), self.options().buffer_size)
    }

    pub fn decompress(
        &self,
        src: impl Into<PathBuf>,
        dst: impl Into<PathBuf>,
    ) -> DecompressBuilder {
        DecompressBuilder::new(src.into(), dst.into(), self.options().buffer_size)
    }
}

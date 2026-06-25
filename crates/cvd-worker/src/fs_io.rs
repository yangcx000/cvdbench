//! File I/O abstraction: buffered tokio I/O or blocking O_DIRECT I/O.

use std::path::Path;
use std::time::{Duration, Instant};

use cvd_common::spec::IoMode;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use sha2::{Digest, Sha256};
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

pub mod direct_io;

#[derive(Clone, Copy, Debug)]
pub struct IoProfile {
    pub io_mode: IoMode,
    pub io_aligned: bool,
    pub direct_io: bool,
    pub block_size: u64,
}

#[derive(Clone, Debug)]
pub struct ReadChunk {
    pub bytes: Vec<u8>,
    pub latency: Duration,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadStreamPhase {
    Open,
    Read,
    Close,
}

#[derive(Debug)]
pub enum ReadStreamEvent {
    Open { latency: Duration },
    Chunk(ReadChunk),
    Close { latency: Duration },
}

#[derive(Debug)]
pub struct ReadStreamError {
    pub phase: ReadStreamPhase,
    pub source: std::io::Error,
}

pub async fn read_file(path: &Path, profile: IoProfile) -> std::io::Result<Vec<ReadChunk>> {
    if profile.direct_io {
        let block_size = usize::try_from(profile.block_size.max(1)).unwrap_or(1 << 20);
        let path = path.to_path_buf();
        let chunks =
            tokio::task::spawn_blocking(move || direct_io::read_file_blocking(&path, block_size))
                .await
                .map_err(|e| std::io::Error::other(format!("direct read task join: {e}")))??;
        // 直接 IO 路径每块独立计 latency；spawn_blocking 收到的是 (chunk, elapsed) tuple。
        return Ok(chunks
            .into_iter()
            .map(|(bytes, latency)| ReadChunk { bytes, latency })
            .collect());
    }

    match profile.io_mode {
        IoMode::Seq if profile.io_aligned => read_file_seq(path, profile.block_size).await,
        IoMode::Seq => read_file_unaligned_seq(path, profile.block_size).await,
        IoMode::Rand => read_file_rand(path, profile.block_size, profile.io_aligned).await,
    }
}

/// 流式读取：每读到一块就 push 给 `tx`，避免把整文件拉进内存（spec §6.5）。
///
/// 任意一块发送失败（消费方关闭）即停；任意 IO 错误以 `Err(io::Error)` 形式
/// push 给 `tx` 后退出。
pub async fn stream_read_file(
    path: &Path,
    profile: IoProfile,
    tx: tokio::sync::mpsc::Sender<Result<ReadStreamEvent, ReadStreamError>>,
) {
    if profile.direct_io {
        // direct IO：spawn_blocking + std::sync::mpsc → forward 到 tokio mpsc。
        let block_size = usize::try_from(profile.block_size.max(1)).unwrap_or(1 << 20);
        let path = path.to_path_buf();
        let (sync_tx, sync_rx) =
            std::sync::mpsc::sync_channel::<Result<ReadStreamEvent, ReadStreamError>>(2);
        let blocker = tokio::task::spawn_blocking(move || {
            direct_io::stream_read_file_blocking(&path, block_size, &sync_tx);
        });
        // 把 sync_rx 的内容转到 async tx；阻塞 recv 用 spawn_blocking 包一层。
        loop {
            let recv_res = tokio::task::block_in_place(|| sync_rx.recv());
            let event = match recv_res {
                Ok(c) => c,
                Err(_) => break, // sync_tx dropped
            };
            if tx.send(event).await.is_err() {
                break;
            }
        }
        let _ = blocker.await;
        return;
    }

    // 非 direct_io：用 tokio::fs 流式读。
    let result = match profile.io_mode {
        IoMode::Seq if profile.io_aligned => stream_seq(path, profile.block_size, &tx).await,
        IoMode::Seq => stream_unaligned_seq(path, profile.block_size, &tx).await,
        IoMode::Rand => stream_rand(path, profile.block_size, profile.io_aligned, &tx).await,
    };
    if let Err(e) = result {
        let _ = tx.send(Err(e)).await;
    }
}

async fn stream_seq(
    path: &Path,
    block_size: u64,
    tx: &tokio::sync::mpsc::Sender<Result<ReadStreamEvent, ReadStreamError>>,
) -> Result<(), ReadStreamError> {
    let start = Instant::now();
    let mut file = fs::File::open(path)
        .await
        .map_err(|source| ReadStreamError {
            phase: ReadStreamPhase::Open,
            source,
        })?;
    if tx
        .send(Ok(ReadStreamEvent::Open {
            latency: start.elapsed(),
        }))
        .await
        .is_err()
    {
        return Ok(());
    }
    let buf_size = usize::try_from(block_size.max(1)).unwrap_or(1 << 20);
    let mut buf = vec![0u8; buf_size];
    loop {
        let start = Instant::now();
        let n = file
            .read(&mut buf)
            .await
            .map_err(|source| ReadStreamError {
                phase: ReadStreamPhase::Read,
                source,
            })?;
        let elapsed = start.elapsed();
        if n == 0 {
            let start = Instant::now();
            drop(file);
            let _ = tx
                .send(Ok(ReadStreamEvent::Close {
                    latency: start.elapsed(),
                }))
                .await;
            return Ok(());
        }
        let event = ReadStreamEvent::Chunk(ReadChunk {
            bytes: buf[..n].to_vec(),
            latency: elapsed,
        });
        if tx.send(Ok(event)).await.is_err() {
            return Ok(());
        }
    }
}

async fn stream_unaligned_seq(
    path: &Path,
    block_size: u64,
    tx: &tokio::sync::mpsc::Sender<Result<ReadStreamEvent, ReadStreamError>>,
) -> Result<(), ReadStreamError> {
    let start = Instant::now();
    let mut file = fs::File::open(path)
        .await
        .map_err(|source| ReadStreamError {
            phase: ReadStreamPhase::Open,
            source,
        })?;
    if tx
        .send(Ok(ReadStreamEvent::Open {
            latency: start.elapsed(),
        }))
        .await
        .is_err()
    {
        return Ok(());
    }
    let len = file
        .metadata()
        .await
        .map_err(|source| ReadStreamError {
            phase: ReadStreamPhase::Read,
            source,
        })?
        .len();
    if len == 0 {
        let start = Instant::now();
        drop(file);
        let _ = tx
            .send(Ok(ReadStreamEvent::Close {
                latency: start.elapsed(),
            }))
            .await;
        return Ok(());
    }
    let block_size = usize::try_from(block_size.max(1)).unwrap_or(1 << 20);
    for (offset, wanted) in unaligned_ranges(len as usize, block_size, path) {
        file.seek(std::io::SeekFrom::Start(offset as u64))
            .await
            .map_err(|source| ReadStreamError {
                phase: ReadStreamPhase::Read,
                source,
            })?;
        let mut buf = vec![0u8; wanted];
        let start = Instant::now();
        let n = file
            .read(&mut buf)
            .await
            .map_err(|source| ReadStreamError {
                phase: ReadStreamPhase::Read,
                source,
            })?;
        let elapsed = start.elapsed();
        if n == 0 {
            continue;
        }
        buf.truncate(n);
        let event = ReadStreamEvent::Chunk(ReadChunk {
            bytes: buf,
            latency: elapsed,
        });
        if tx.send(Ok(event)).await.is_err() {
            return Ok(());
        }
    }
    let start = Instant::now();
    drop(file);
    let _ = tx
        .send(Ok(ReadStreamEvent::Close {
            latency: start.elapsed(),
        }))
        .await;
    Ok(())
}

async fn stream_rand(
    path: &Path,
    block_size: u64,
    aligned: bool,
    tx: &tokio::sync::mpsc::Sender<Result<ReadStreamEvent, ReadStreamError>>,
) -> Result<(), ReadStreamError> {
    let start = Instant::now();
    let mut file = fs::File::open(path)
        .await
        .map_err(|source| ReadStreamError {
            phase: ReadStreamPhase::Open,
            source,
        })?;
    if tx
        .send(Ok(ReadStreamEvent::Open {
            latency: start.elapsed(),
        }))
        .await
        .is_err()
    {
        return Ok(());
    }
    let len = file
        .metadata()
        .await
        .map_err(|source| ReadStreamError {
            phase: ReadStreamPhase::Read,
            source,
        })?
        .len();
    if len == 0 {
        let start = Instant::now();
        drop(file);
        let _ = tx
            .send(Ok(ReadStreamEvent::Close {
                latency: start.elapsed(),
            }))
            .await;
        return Ok(());
    }
    let block_size = usize::try_from(block_size.max(1)).unwrap_or(1 << 20);
    let mut ranges = if aligned {
        chunk_ranges(len as usize, block_size)
    } else {
        unaligned_ranges(len as usize, block_size, path)
    };
    shuffle_ranges(&mut ranges, path, len as usize);
    for (offset, wanted) in ranges {
        file.seek(std::io::SeekFrom::Start(offset as u64))
            .await
            .map_err(|source| ReadStreamError {
                phase: ReadStreamPhase::Read,
                source,
            })?;
        let mut buf = vec![0u8; wanted];
        let start = Instant::now();
        let n = file
            .read(&mut buf)
            .await
            .map_err(|source| ReadStreamError {
                phase: ReadStreamPhase::Read,
                source,
            })?;
        let elapsed = start.elapsed();
        if n == 0 {
            continue;
        }
        buf.truncate(n);
        let event = ReadStreamEvent::Chunk(ReadChunk {
            bytes: buf,
            latency: elapsed,
        });
        if tx.send(Ok(event)).await.is_err() {
            return Ok(());
        }
    }
    let start = Instant::now();
    drop(file);
    let _ = tx
        .send(Ok(ReadStreamEvent::Close {
            latency: start.elapsed(),
        }))
        .await;
    Ok(())
}

pub async fn write_file(
    path: &Path,
    payload: &[u8],
    profile: IoProfile,
    fsync: bool,
) -> std::io::Result<Duration> {
    let start = Instant::now();
    if profile.direct_io {
        let path = path.to_path_buf();
        let payload = payload.to_vec();
        tokio::task::spawn_blocking(move || direct_io::write_file_blocking(&path, &payload, fsync))
            .await
            .map_err(|e| std::io::Error::other(format!("direct write task join: {e}")))??;
        return Ok(start.elapsed());
    }

    let mut file = fs::File::create(path).await?;
    match profile.io_mode {
        IoMode::Seq => {
            if profile.io_aligned {
                file.write_all(payload).await?;
            } else {
                let block_size = usize::try_from(profile.block_size.max(1)).unwrap_or(1 << 20);
                for (offset, len) in unaligned_ranges(payload.len(), block_size, path) {
                    file.seek(std::io::SeekFrom::Start(offset as u64)).await?;
                    file.write_all(&payload[offset..offset + len]).await?;
                }
            }
        }
        IoMode::Rand => {
            let block_size = usize::try_from(profile.block_size.max(1)).unwrap_or(1 << 20);
            let mut ranges = chunk_ranges(payload.len(), block_size);
            shuffle_ranges(&mut ranges, path, payload.len());
            for (offset, len) in ranges {
                file.seek(std::io::SeekFrom::Start(offset as u64)).await?;
                file.write_all(&payload[offset..offset + len]).await?;
            }
        }
    }
    if fsync {
        file.sync_all().await?;
    }
    file.flush().await?;
    Ok(start.elapsed())
}

async fn read_file_seq(path: &Path, block_size: u64) -> std::io::Result<Vec<ReadChunk>> {
    let mut file = fs::File::open(path).await?;
    let buf_size = usize::try_from(block_size.max(1)).unwrap_or(1 << 20);
    let mut buf = vec![0u8; buf_size];
    let mut chunks = Vec::new();
    loop {
        let start = Instant::now();
        let n = file.read(&mut buf).await?;
        let elapsed = start.elapsed();
        if n == 0 {
            break;
        }
        chunks.push(ReadChunk {
            bytes: buf[..n].to_vec(),
            latency: elapsed,
        });
    }
    Ok(chunks)
}

async fn read_file_rand(
    path: &Path,
    block_size: u64,
    aligned: bool,
) -> std::io::Result<Vec<ReadChunk>> {
    let mut file = fs::File::open(path).await?;
    let len = file.metadata().await?.len();
    if len == 0 {
        return Ok(Vec::new());
    }
    let block_size = usize::try_from(block_size.max(1)).unwrap_or(1 << 20);
    let mut ranges = if aligned {
        chunk_ranges(len as usize, block_size)
    } else {
        unaligned_ranges(len as usize, block_size, path)
    };
    shuffle_ranges(&mut ranges, path, len as usize);
    let mut chunks = Vec::with_capacity(ranges.len());
    for (offset, wanted) in ranges {
        file.seek(std::io::SeekFrom::Start(offset as u64)).await?;
        let mut buf = vec![0u8; wanted];
        let start = Instant::now();
        let n = file.read(&mut buf).await?;
        let elapsed = start.elapsed();
        if n == 0 {
            continue;
        }
        buf.truncate(n);
        chunks.push((
            offset,
            ReadChunk {
                bytes: buf,
                latency: elapsed,
            },
        ));
    }
    chunks.sort_by_key(|(offset, _)| *offset);
    Ok(chunks.into_iter().map(|(_, chunk)| chunk).collect())
}

async fn read_file_unaligned_seq(path: &Path, block_size: u64) -> std::io::Result<Vec<ReadChunk>> {
    let mut file = fs::File::open(path).await?;
    let len = file.metadata().await?.len();
    if len == 0 {
        return Ok(Vec::new());
    }
    let block_size = usize::try_from(block_size.max(1)).unwrap_or(1 << 20);
    let mut chunks = Vec::new();
    for (offset, wanted) in unaligned_ranges(len as usize, block_size, path) {
        file.seek(std::io::SeekFrom::Start(offset as u64)).await?;
        let mut buf = vec![0u8; wanted];
        let start = Instant::now();
        let n = file.read(&mut buf).await?;
        let elapsed = start.elapsed();
        if n == 0 {
            continue;
        }
        buf.truncate(n);
        chunks.push(ReadChunk {
            bytes: buf,
            latency: elapsed,
        });
    }
    Ok(chunks)
}

fn chunk_ranges(len: usize, block_size: usize) -> Vec<(usize, usize)> {
    let mut ranges = Vec::new();
    let mut offset = 0usize;
    while offset < len {
        let remaining = len - offset;
        let n = remaining.min(block_size.max(1));
        ranges.push((offset, n));
        offset += n;
    }
    ranges
}

fn unaligned_ranges(len: usize, block_size: usize, path: &Path) -> Vec<(usize, usize)> {
    let block_size = block_size.max(1);
    if len == 0 {
        return Vec::new();
    }
    let mut rng = rng_for(path, len);
    let start = rng.gen_range(0..len);
    let mut ranges = Vec::new();
    let mut offset = start;
    while offset < len {
        let n = (len - offset).min(block_size);
        ranges.push((offset, n));
        offset += n;
    }
    ranges
}

fn shuffle_ranges(ranges: &mut [(usize, usize)], path: &Path, len: usize) {
    use rand::seq::SliceRandom;
    let mut rng = rng_for(path, len);
    ranges.shuffle(&mut rng);
}

fn rng_for(path: &Path, len: usize) -> StdRng {
    let mut hasher = Sha256::new();
    hasher.update(path.as_os_str().as_encoded_bytes());
    hasher.update(len.to_le_bytes());
    let digest: [u8; 32] = hasher.finalize().into();
    StdRng::from_seed(digest)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_ranges_cover_file() {
        assert_eq!(chunk_ranges(10, 4), vec![(0, 4), (4, 4), (8, 2)]);
    }

    #[test]
    fn unaligned_ranges_start_at_deterministic_random_offset() {
        let ranges = unaligned_ranges(26, 8, Path::new("abc"));
        let ranges_again = unaligned_ranges(26, 8, Path::new("abc"));
        assert_eq!(ranges, ranges_again);
        let first_offset = ranges.first().map_or(0, |(offset, _)| *offset);
        assert!(first_offset < 26);
        assert_ne!(first_offset, 0);
        let mut next = first_offset;
        for (offset, len) in ranges {
            assert_eq!(offset, next);
            next += len;
        }
        assert_eq!(next, 26);
    }

    #[tokio::test]
    async fn rand_read_returns_file_bytes_in_chunks() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("f");
        fs::write(&path, b"abcdefghijklmnopqrstuvwxyz")
            .await
            .unwrap();
        let chunks = read_file(
            &path,
            IoProfile {
                io_mode: IoMode::Rand,
                io_aligned: true,
                direct_io: false,
                block_size: 4,
            },
        )
        .await
        .unwrap();
        let total: usize = chunks.iter().map(|c| c.bytes.len()).sum();
        assert_eq!(total, 26);
    }

    #[tokio::test]
    async fn unaligned_seq_read_starts_after_random_offset() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("f");
        fs::write(&path, b"abcdefghijklmnopqrstuvwxyz")
            .await
            .unwrap();
        let chunks = read_file(
            &path,
            IoProfile {
                io_mode: IoMode::Seq,
                io_aligned: false,
                direct_io: false,
                block_size: 4,
            },
        )
        .await
        .unwrap();
        let bytes: Vec<u8> = chunks.into_iter().flat_map(|chunk| chunk.bytes).collect();
        assert!(!bytes.is_empty());
        assert!(b"abcdefghijklmnopqrstuvwxyz".ends_with(&bytes));
        assert_ne!(bytes, b"abcdefghijklmnopqrstuvwxyz");
    }

    #[tokio::test]
    async fn unaligned_seq_write_preserves_payload() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("f");
        let payload = b"abcdefghijklmnopqrstuvwxyz";
        write_file(
            &path,
            payload,
            IoProfile {
                io_mode: IoMode::Seq,
                io_aligned: false,
                direct_io: false,
                block_size: 8,
            },
            false,
        )
        .await
        .unwrap();
        assert_eq!(fs::read(&path).await.unwrap(), payload);
    }
}

//! O_DIRECT helpers: blocking POSIX I/O with aligned buffers.

use std::alloc::{alloc_zeroed, dealloc, Layout};
use std::fs::OpenOptions;
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::ptr::NonNull;
use std::sync::mpsc::SyncSender;
use std::time::{Duration, Instant};

use super::{ReadChunk, ReadStreamError, ReadStreamEvent, ReadStreamPhase};

const DEFAULT_ALIGNMENT: usize = 4096;

/// 阻塞模式按 block_size 分块读取整个文件，返回每块的 (bytes, latency)。
/// 仅供小文件 / 测试使用；大文件请用 [`stream_read_file_blocking`] 流式接口。
pub fn read_file_blocking(path: &Path, block_size: usize) -> io::Result<Vec<(Vec<u8>, Duration)>> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECT)
        .open(path)?;
    let file_len = file.metadata()?.len();
    let fd = file.as_raw_fd();
    let mut offset = 0u64;
    let mut chunks = Vec::new();
    let read_size = aligned_io_size(block_size)?;
    let mut buffer = AlignedBuffer::new(read_size, DEFAULT_ALIGNMENT)?;

    while offset < file_len {
        let start = Instant::now();
        let n = pread_all(fd, buffer.as_mut_ptr(), read_size, offset)?;
        let elapsed = start.elapsed();
        if n == 0 {
            break;
        }
        let remaining = usize::try_from(file_len.saturating_sub(offset)).unwrap_or(usize::MAX);
        let logical = n.min(remaining);
        chunks.push((buffer.as_slice()[..logical].to_vec(), elapsed));
        offset = offset.saturating_add(n as u64);
    }
    Ok(chunks)
}

/// 阻塞模式按 block_size 分块流式读：每读到一块就 push 给 `tx`。
/// 任意 IO 错误会以 `Err` push；`tx` 关闭即立刻退出。
pub fn stream_read_file_blocking(
    path: &Path,
    block_size: usize,
    tx: &SyncSender<Result<ReadStreamEvent, ReadStreamError>>,
) {
    let start = Instant::now();
    let file = match OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECT)
        .open(path)
    {
        Ok(f) => {
            if tx
                .send(Ok(ReadStreamEvent::Open {
                    latency: start.elapsed(),
                }))
                .is_err()
            {
                return;
            }
            f
        }
        Err(e) => {
            let _ = tx.send(Err(ReadStreamError {
                phase: ReadStreamPhase::Open,
                source: e,
            }));
            return;
        }
    };
    let file_len = match file.metadata() {
        Ok(m) => m.len(),
        Err(e) => {
            let _ = tx.send(Err(ReadStreamError {
                phase: ReadStreamPhase::Read,
                source: e,
            }));
            return;
        }
    };
    let fd = file.as_raw_fd();
    let read_size = match aligned_io_size(block_size) {
        Ok(s) => s,
        Err(e) => {
            let _ = tx.send(Err(ReadStreamError {
                phase: ReadStreamPhase::Read,
                source: e,
            }));
            return;
        }
    };
    let mut buffer = match AlignedBuffer::new(read_size, DEFAULT_ALIGNMENT) {
        Ok(b) => b,
        Err(e) => {
            let _ = tx.send(Err(ReadStreamError {
                phase: ReadStreamPhase::Read,
                source: e,
            }));
            return;
        }
    };
    let mut offset = 0u64;
    while offset < file_len {
        let start = Instant::now();
        let n = match pread_all(fd, buffer.as_mut_ptr(), read_size, offset) {
            Ok(n) => n,
            Err(e) => {
                let _ = tx.send(Err(ReadStreamError {
                    phase: ReadStreamPhase::Read,
                    source: e,
                }));
                return;
            }
        };
        let elapsed = start.elapsed();
        if n == 0 {
            let start = Instant::now();
            drop(file);
            let _ = tx.send(Ok(ReadStreamEvent::Close {
                latency: start.elapsed(),
            }));
            return;
        }
        let remaining = usize::try_from(file_len.saturating_sub(offset)).unwrap_or(usize::MAX);
        let logical = n.min(remaining);
        let chunk = ReadChunk {
            bytes: buffer.as_slice()[..logical].to_vec(),
            latency: elapsed,
        };
        if tx.send(Ok(ReadStreamEvent::Chunk(chunk))).is_err() {
            return;
        }
        offset = offset.saturating_add(n as u64);
    }
    let start = Instant::now();
    drop(file);
    let _ = tx.send(Ok(ReadStreamEvent::Close {
        latency: start.elapsed(),
    }));
}

pub fn write_file_blocking(path: &Path, payload: &[u8], fsync: bool) -> io::Result<()> {
    let file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .custom_flags(libc::O_DIRECT)
        .open(path)?;
    let fd = file.as_raw_fd();
    // 按 alignment 分块写：每次最多写一个 aligned chunk，避免一次性把整文件
    // 灌入内存（spec §6.5）。最后一块允许 padding 到 alignment（直接 IO 必须
    // 对齐写）；写完后 set_len 把文件大小修回 payload.len()。
    let mut buffer = AlignedBuffer::new(DEFAULT_ALIGNMENT, DEFAULT_ALIGNMENT)?;
    let mut offset = 0u64;
    let total = payload.len();
    let mut written_logical = 0usize;
    while written_logical < total {
        let logical_chunk = (total - written_logical).min(DEFAULT_ALIGNMENT);
        // 拷贝到对齐缓冲；尾段不足 DEFAULT_ALIGNMENT 时尾部保留 0 padding。
        buffer.as_mut_slice()[..logical_chunk]
            .copy_from_slice(&payload[written_logical..written_logical + logical_chunk]);
        if logical_chunk < DEFAULT_ALIGNMENT {
            // 尾段 padding：清掉 buffer 尾部脏数据（上轮残留）
            for b in &mut buffer.as_mut_slice()[logical_chunk..] {
                *b = 0;
            }
        }
        pwrite_all(fd, buffer.as_ptr(), DEFAULT_ALIGNMENT, offset)?;
        offset = offset.saturating_add(DEFAULT_ALIGNMENT as u64);
        written_logical += logical_chunk;
    }
    file.set_len(payload.len() as u64)?;
    if fsync {
        file.sync_all()?;
    }
    Ok(())
}

fn aligned_io_size(size: usize) -> io::Result<usize> {
    if size == 0 {
        return Ok(DEFAULT_ALIGNMENT);
    }
    size.checked_next_multiple_of(DEFAULT_ALIGNMENT)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "aligned I/O size overflow"))
}

fn pread_all(fd: i32, ptr: *mut u8, len: usize, offset: u64) -> io::Result<usize> {
    #[allow(unsafe_code)]
    let n = unsafe { libc::pread(fd, ptr.cast(), len, offset as libc::off_t) };
    if n < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(n as usize)
    }
}

fn pwrite_all(fd: i32, ptr: *const u8, len: usize, mut offset: u64) -> io::Result<()> {
    let mut written = 0usize;
    while written < len {
        #[allow(unsafe_code)]
        let n = unsafe {
            libc::pwrite(
                fd,
                ptr.add(written).cast(),
                len - written,
                offset as libc::off_t,
            )
        };
        if n < 0 {
            return Err(io::Error::last_os_error());
        }
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "direct pwrite wrote zero bytes",
            ));
        }
        written += n as usize;
        offset = offset.saturating_add(n as u64);
    }
    Ok(())
}

struct AlignedBuffer {
    ptr: NonNull<u8>,
    len: usize,
    layout: Layout,
}

impl AlignedBuffer {
    fn new(len: usize, align: usize) -> io::Result<Self> {
        let layout = Layout::from_size_align(len, align).map_err(|e| {
            io::Error::new(io::ErrorKind::InvalidInput, format!("invalid layout: {e}"))
        })?;
        #[allow(unsafe_code)]
        let raw = unsafe { alloc_zeroed(layout) };
        let ptr = NonNull::new(raw).ok_or_else(|| io::Error::other("aligned alloc failed"))?;
        Ok(Self { ptr, len, layout })
    }

    fn as_ptr(&self) -> *const u8 {
        self.ptr.as_ptr()
    }

    fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    fn as_slice(&self) -> &[u8] {
        #[allow(unsafe_code)]
        unsafe {
            std::slice::from_raw_parts(self.ptr.as_ptr(), self.len)
        }
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        #[allow(unsafe_code)]
        unsafe {
            std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len)
        }
    }
}

impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        #[allow(unsafe_code)]
        unsafe {
            dealloc(self.ptr.as_ptr(), self.layout);
        }
    }
}

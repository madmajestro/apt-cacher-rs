use std::{
    num::NonZero,
    sync::{Arc, Mutex},
};

use crate::ringbuffer::RingBuffer;

#[derive(Debug)]
struct LogStoreImpl {
    entries: RingBuffer<String>,
    buffer: Vec<u8>,
}

impl LogStoreImpl {
    fn new(capacity: NonZero<usize>) -> Self {
        Self {
            entries: RingBuffer::new(capacity),
            buffer: Vec::with_capacity(1024),
        }
    }

    fn iter(&self) -> std::collections::vec_deque::Iter<'_, std::string::String> {
        self.entries.iter()
    }
}

impl std::io::Write for LogStoreImpl {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buffer.extend_from_slice(buf);

        let mut buf_slice = self.buffer.as_mut_slice();

        while let Some(pos) = buf_slice.iter().position(|&x| x == b'\n') {
            let buf_len = buf_slice.len();

            let (line, _rest) = buf_slice.split_at(pos);

            let s = String::from_utf8_lossy(line);
            self.entries.push(s.trim().to_string());

            buf_slice.copy_within((pos + 1).., 0);
            self.buffer.truncate(buf_len - pos - 1);

            buf_slice = self.buffer.as_mut_slice();
        }

        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub(crate) struct LogStore {
    inner: Arc<Mutex<LogStoreImpl>>,
}

impl LogStore {
    #[must_use]
    pub(crate) fn new(capacity: NonZero<usize>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(LogStoreImpl::new(capacity))),
        }
    }
}

impl LogStore {
    #[must_use]
    pub(crate) fn entries(&self) -> LogStoreEntryListGuard<'_> {
        let guard = self.inner.lock().expect("Other users should not panic");
        LogStoreEntryListGuard { guard }
    }
}

impl std::io::Write for LogStore {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut inner = self.inner.lock().expect("Other users should not panic");
        inner.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub(crate) struct LogStoreEntryListGuard<'a> {
    guard: std::sync::MutexGuard<'a, LogStoreImpl>,
}

impl LogStoreEntryListGuard<'_> {
    #[must_use]
    pub(crate) fn iter(&self) -> std::collections::vec_deque::Iter<'_, std::string::String> {
        self.guard.iter()
    }
}

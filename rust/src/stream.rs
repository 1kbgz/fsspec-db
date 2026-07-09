use std::cmp::min;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::sync::{Arc, Mutex};

use arrow::csv::writer::{Writer as CsvWriter, WriterBuilder as CsvWriterBuilder};
use arrow::ipc::writer::StreamWriter;
use arrow::json::writer::LineDelimitedWriter;
use arrow::record_batch::RecordBatch;
use fsspec_rs::{FileInfo, FsFile, FsResult};
use parquet::arrow::ArrowWriter;

use crate::database::RecordBatchStream;
use crate::path::DataFormat;
use crate::{DbError, Result};

#[derive(Clone, Default)]
struct SharedBuffer {
    inner: Arc<Mutex<Vec<u8>>>,
}

impl SharedBuffer {
    fn take(&self) -> io::Result<Vec<u8>> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| io::Error::other("stream buffer lock is poisoned"))?;
        Ok(std::mem::take(&mut *guard))
    }
}

impl Write for SharedBuffer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| io::Error::other("stream buffer lock is poisoned"))?;
        guard.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

enum FormatEncoder {
    Parquet(ArrowWriter<SharedBuffer>),
    Arrow(StreamWriter<SharedBuffer>),
    Csv(Box<CsvWriter<SharedBuffer>>),
    Jsonl(LineDelimitedWriter<SharedBuffer>),
}

impl FormatEncoder {
    fn new(format: DataFormat, reader: &RecordBatchStream, sink: SharedBuffer) -> Result<Self> {
        let schema = reader.schema();
        match format {
            DataFormat::Parquet => Ok(Self::Parquet(ArrowWriter::try_new(sink, schema, None)?)),
            DataFormat::Arrow => Ok(Self::Arrow(StreamWriter::try_new(sink, &schema)?)),
            DataFormat::Csv => Ok(Self::Csv(Box::new(
                CsvWriterBuilder::new().with_header(true).build(sink),
            ))),
            DataFormat::Jsonl => Ok(Self::Jsonl(LineDelimitedWriter::new(sink))),
            DataFormat::Sql => Err(DbError::NotSupported(
                "SQL DDL encoding is handled by DatabaseFs".to_string(),
            )),
        }
    }

    fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        match self {
            Self::Parquet(writer) => {
                writer.write(batch)?;
                writer.flush()?;
            }
            Self::Arrow(writer) => {
                writer.write(batch)?;
                writer.flush()?;
            }
            Self::Csv(writer) => writer.write(batch)?,
            Self::Jsonl(writer) => writer.write(batch)?,
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        match self {
            Self::Parquet(writer) => {
                writer.finish()?;
            }
            Self::Arrow(writer) => writer.finish()?,
            Self::Csv(_) => {}
            Self::Jsonl(writer) => writer.finish()?,
        }
        Ok(())
    }
}

pub struct EncodedDbFile {
    reader: RecordBatchStream,
    encoder: FormatEncoder,
    sink: SharedBuffer,
    emitted: Vec<u8>,
    position: u64,
    info: FileInfo,
    eof: bool,
}

impl EncodedDbFile {
    pub fn new(reader: RecordBatchStream, format: DataFormat, info: FileInfo) -> Result<Self> {
        let sink = SharedBuffer::default();
        let encoder = FormatEncoder::new(format, &reader, sink.clone())?;
        let mut file = Self {
            reader,
            encoder,
            sink,
            emitted: Vec::new(),
            position: 0,
            info,
            eof: false,
        };
        file.drain_sink()?;
        Ok(file)
    }

    fn drain_sink(&mut self) -> io::Result<()> {
        self.emitted.extend_from_slice(&self.sink.take()?);
        Ok(())
    }

    fn fill_until(&mut self, target_len: Option<usize>) -> io::Result<()> {
        while !self.eof && target_len.is_none_or(|target| self.emitted.len() < target) {
            match self.reader.next() {
                Some(Ok(batch)) => {
                    self.encoder.write_batch(&batch).map_err(db_error_to_io)?;
                    self.drain_sink()?;
                }
                Some(Err(err)) => return Err(io::Error::new(io::ErrorKind::InvalidData, err)),
                None => {
                    self.encoder.finish().map_err(db_error_to_io)?;
                    self.eof = true;
                    self.drain_sink()?;
                }
            }
        }
        Ok(())
    }
}

impl Read for EncodedDbFile {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        let start = usize::try_from(self.position).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "file position is too large")
        })?;
        let target = start.saturating_add(buf.len());
        self.fill_until(Some(target))?;
        if start >= self.emitted.len() {
            return Ok(0);
        }
        let count = min(buf.len(), self.emitted.len() - start);
        buf[..count].copy_from_slice(&self.emitted[start..start + count]);
        self.position += count as u64;
        Ok(count)
    }
}

impl Write for EncodedDbFile {
    fn write(&mut self, _buf: &[u8]) -> io::Result<usize> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "database file is read-only",
        ))
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Seek for EncodedDbFile {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let new_position = match pos {
            SeekFrom::Start(position) => position,
            SeekFrom::Current(offset) => offset_position(self.position, offset)?,
            SeekFrom::End(offset) => {
                self.fill_until(None)?;
                offset_position(self.emitted.len() as u64, offset)?
            }
        };
        if usize::try_from(new_position).is_ok() {
            self.fill_until(Some(new_position as usize))?;
        }
        self.position = new_position;
        Ok(self.position)
    }
}

impl FsFile for EncodedDbFile {
    fn info(&self) -> FsResult<FileInfo> {
        let mut info = self.info.clone();
        if self.eof {
            info.size = self.emitted.len() as u64;
            info.extra
                .insert("size_known".to_string(), "true".to_string());
        }
        Ok(info)
    }

    fn size(&self) -> FsResult<Option<u64>> {
        Ok(self.eof.then_some(self.emitted.len() as u64))
    }
}

fn offset_position(position: u64, offset: i64) -> io::Result<u64> {
    if offset >= 0 {
        position
            .checked_add(offset as u64)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "seek position overflow"))
    } else {
        position
            .checked_sub(offset.unsigned_abs())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "negative seek position"))
    }
}

fn db_error_to_io(err: DbError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, err)
}

use std::cmp::min;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::sync::{Arc, Mutex};

use arrow::record_batch::RecordBatch;
use fsspec_data::{CodecWriter, DataFormat as InterchangeFormat, DEFAULT_REGISTRY};
use fsspec_rs::{FileInfo, FsFile, FsResult};

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

struct FormatEncoder {
    writer: Option<Box<dyn CodecWriter>>,
}

impl FormatEncoder {
    fn new(format: DataFormat, reader: &RecordBatchStream, sink: SharedBuffer) -> Result<Self> {
        let schema = reader.schema();
        let format = match format {
            DataFormat::Parquet => InterchangeFormat::Parquet,
            DataFormat::Arrow => InterchangeFormat::Arrow,
            DataFormat::Csv => InterchangeFormat::Csv,
            DataFormat::Jsonl => InterchangeFormat::JsonLines,
            DataFormat::Sql => Err(DbError::NotSupported(
                "SQL DDL encoding is handled by DatabaseFs".to_string(),
            ))?,
        };
        let writer = DEFAULT_REGISTRY
            .get(format)?
            .start_owned_writer(schema, Box::new(sink))?;
        Ok(Self {
            writer: Some(writer),
        })
    }

    fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.writer.as_mut().unwrap().write_batch(batch)?;
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        if let Some(writer) = self.writer.take() {
            writer.finish()?;
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

#[cfg(test)]
mod tests {
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use arrow::error::ArrowError;
    use arrow::record_batch::RecordBatchReader;

    use super::*;

    struct CountingReader {
        schema: SchemaRef,
        batches: std::vec::IntoIter<RecordBatch>,
        reads: Arc<Mutex<usize>>,
    }

    impl Iterator for CountingReader {
        type Item = std::result::Result<RecordBatch, ArrowError>;

        fn next(&mut self) -> Option<Self::Item> {
            let batch = self.batches.next()?;
            *self.reads.lock().unwrap() += 1;
            Some(Ok(batch))
        }
    }

    impl RecordBatchReader for CountingReader {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }
    }

    #[test]
    fn text_and_arrow_reads_encode_database_batches_incrementally() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Utf8,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec!["x".repeat(10_000)]))],
        )
        .unwrap();
        for format in [DataFormat::Arrow, DataFormat::Csv, DataFormat::Jsonl] {
            let reads = Arc::new(Mutex::new(0));
            let reader = CountingReader {
                schema: schema.clone(),
                batches: vec![batch.clone(), batch.clone()].into_iter(),
                reads: Arc::clone(&reads),
            };
            let mut file =
                EncodedDbFile::new(Box::new(reader), format, FileInfo::file("/main/data", 0))
                    .unwrap();

            let mut first_chunk = [0; 1024];
            file.read_exact(&mut first_chunk).unwrap();
            assert_eq!(*reads.lock().unwrap(), 1);

            let mut remainder = Vec::new();
            file.read_to_end(&mut remainder).unwrap();
            assert_eq!(*reads.lock().unwrap(), 2);
        }
    }
}

use std::io::Cursor;
use std::sync::Arc;

use arrow::csv::{reader::ReaderBuilder as CsvReaderBuilder, writer::WriterBuilder};
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::json::{reader::ReaderBuilder as JsonReaderBuilder, LineDelimitedWriter};
use arrow::record_batch::{RecordBatch, RecordBatchIterator};
use fsspec_data::{
    CancellationToken, DataFormat as InterchangeFormat, InterchangeError, StreamOptions,
    DEFAULT_REGISTRY,
};

use crate::database::RecordBatchStream;
use crate::path::DataFormat;
use crate::{DbError, Result};

pub fn rows_to_arrow(batches: Vec<RecordBatch>) -> Result<RecordBatchStream> {
    let schema = batches
        .first()
        .map(|batch| batch.schema())
        .unwrap_or_else(|| Arc::new(arrow::datatypes::Schema::empty()));
    Ok(Box::new(RecordBatchIterator::new(
        batches.into_iter().map(Ok),
        schema,
    )))
}

pub fn format_reader(reader: RecordBatchStream, format: &DataFormat) -> Result<Vec<u8>> {
    match format {
        DataFormat::Parquet => arrow_to_parquet(reader),
        DataFormat::Arrow => arrow_to_ipc(reader),
        DataFormat::Csv => arrow_to_csv(reader),
        DataFormat::Jsonl => arrow_to_jsonl(reader),
        DataFormat::Sql => Err(DbError::NotSupported(
            "SQL DDL encoding is handled by DatabaseFs".to_string(),
        )),
    }
}

pub fn arrow_to_parquet(reader: RecordBatchStream) -> Result<Vec<u8>> {
    encode_with_interchange(reader, InterchangeFormat::Parquet)
}

pub fn parquet_to_arrow(data: Vec<u8>) -> Result<RecordBatchStream> {
    decode_with_interchange(data, InterchangeFormat::Parquet)
}

pub fn arrow_to_ipc(reader: RecordBatchStream) -> Result<Vec<u8>> {
    encode_with_interchange(reader, InterchangeFormat::Arrow)
}

pub fn ipc_to_arrow(data: Vec<u8>) -> Result<RecordBatchStream> {
    decode_with_interchange(data, InterchangeFormat::Arrow)
}

fn encode_with_interchange(
    reader: RecordBatchStream,
    format: InterchangeFormat,
) -> Result<Vec<u8>> {
    let schema = reader.schema();
    let codec = DEFAULT_REGISTRY.get(format)?;
    let mut batches = reader.map(|batch| batch.map_err(InterchangeError::from));
    let mut output = Vec::new();
    codec.encode_stream(schema, &mut batches, &mut output)?;
    Ok(output)
}

fn decode_with_interchange(data: Vec<u8>, format: InterchangeFormat) -> Result<RecordBatchStream> {
    let codec = DEFAULT_REGISTRY.get(format)?;
    let options = if format == InterchangeFormat::Arrow {
        StreamOptions {
            batch_size: usize::MAX,
            ..StreamOptions::default()
        }
    } else {
        StreamOptions::default()
    };
    let stream = codec.decode_stream(data, None, options, CancellationToken::new())?;
    let schema = stream.schema.clone();
    let batches = stream.map(|batch| batch.map_err(interchange_to_arrow));
    Ok(Box::new(RecordBatchIterator::new(batches, schema)))
}

fn interchange_to_arrow(err: InterchangeError) -> ArrowError {
    match err {
        InterchangeError::Arrow(err) => err,
        err => ArrowError::ExternalError(Box::new(err)),
    }
}

pub fn arrow_to_csv(mut reader: RecordBatchStream) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    {
        let mut writer = WriterBuilder::new().with_header(true).build(&mut out);
        write_batches(&mut reader, |batch| {
            writer.write(batch).map_err(DbError::from)
        })?;
    }
    Ok(out)
}

pub fn csv_to_arrow(data: Vec<u8>, schema: SchemaRef) -> Result<RecordBatchStream> {
    let reader = CsvReaderBuilder::new(schema)
        .with_header(true)
        .build(Cursor::new(data))?;
    Ok(Box::new(reader))
}

pub fn arrow_to_jsonl(mut reader: RecordBatchStream) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    {
        let mut writer = LineDelimitedWriter::new(&mut out);
        write_batches(&mut reader, |batch| {
            writer.write(batch).map_err(DbError::from)
        })?;
        writer.finish()?;
    }
    Ok(out)
}

pub fn jsonl_to_arrow(data: Vec<u8>, schema: SchemaRef) -> Result<RecordBatchStream> {
    let reader = JsonReaderBuilder::new(schema).build(Cursor::new(data))?;
    Ok(Box::new(reader))
}

fn write_batches(
    reader: &mut RecordBatchStream,
    mut write: impl FnMut(&RecordBatch) -> Result<()>,
) -> Result<()> {
    for batch in reader {
        let batch = batch.map_err(map_arrow)?;
        write(&batch)?;
    }
    Ok(())
}

fn map_arrow(err: ArrowError) -> DbError {
    DbError::Arrow(err)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use super::*;

    fn batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("ada"), Some("grace")])) as ArrayRef,
            ],
        )
        .unwrap()
    }

    #[test]
    fn roundtrips_ipc() {
        let input = batch();
        let data = arrow_to_ipc(rows_to_arrow(vec![input.clone()]).unwrap()).unwrap();
        let mut reader = ipc_to_arrow(data).unwrap();
        assert_eq!(reader.next().unwrap().unwrap(), input);
    }

    #[test]
    fn ipc_preserves_large_batch_boundaries() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let input = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from_iter_values(0..1025)) as ArrayRef],
        )
        .unwrap();
        let data = arrow_to_ipc(rows_to_arrow(vec![input]).unwrap()).unwrap();
        let batches = ipc_to_arrow(data)
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1025);
    }

    #[test]
    fn roundtrips_csv() {
        let input = batch();
        let data = arrow_to_csv(rows_to_arrow(vec![input.clone()]).unwrap()).unwrap();
        let mut reader = csv_to_arrow(data, input.schema()).unwrap();
        assert_eq!(reader.next().unwrap().unwrap(), input);
    }

    #[test]
    fn roundtrips_jsonl() {
        let input = batch();
        let data = arrow_to_jsonl(rows_to_arrow(vec![input.clone()]).unwrap()).unwrap();
        let mut reader = jsonl_to_arrow(data, input.schema()).unwrap();
        assert_eq!(reader.next().unwrap().unwrap(), input);
    }

    #[test]
    fn roundtrips_parquet() {
        let input = batch();
        let data = arrow_to_parquet(rows_to_arrow(vec![input.clone()]).unwrap()).unwrap();
        let mut reader = parquet_to_arrow(data).unwrap();
        assert_eq!(reader.next().unwrap().unwrap(), input);
    }
}

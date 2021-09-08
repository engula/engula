use std::sync::Arc;

use async_trait::async_trait;
use parquet::{
    column::writer::ColumnWriter,
    data_type::ByteArray,
    file::{
        properties::WriterProperties,
        writer::{FileWriter, RowGroupWriter, SerializedFileWriter},
    },
    schema::parser::parse_message_type,
    util::cursor::InMemoryWriteableCursor,
};

use super::table::TableBuilder;
use super::{ParquetOptions, Timestamp};
use crate::error::{Error, Result};
use crate::file_system::SequentialWriter;

const SCHEMA_MESSAGE: &str = "
    message schema {
        REQUIRED BYTE_ARRAY ts;
        REQUIRED BYTE_ARRAY key;
        REQUIRED BYTE_ARRAY value;
    }
";

pub struct ParquetBuilder {
    options: ParquetOptions,
    file: Box<dyn SequentialWriter>,
    error: Option<Error>,
    buffer: Option<InMemoryWriteableCursor>,
    writer: Option<SerializedFileWriter<InMemoryWriteableCursor>>,
    tss: Vec<ByteArray>,
    keys: Vec<ByteArray>,
    values: Vec<ByteArray>,
    finished: bool,
    current_group_size: usize,
}

#[allow(dead_code)]
impl ParquetBuilder {
    pub fn new(options: ParquetOptions, file: Box<dyn SequentialWriter>) -> ParquetBuilder {
        ParquetBuilder {
            options,
            file,
            error: None,
            buffer: None,
            writer: None,
            tss: Vec::new(),
            keys: Vec::new(),
            values: Vec::new(),
            finished: false,
            current_group_size: 0,
        }
    }

    fn flush_row_group(&mut self) -> Result<()> {
        if self.writer.is_none() {
            let buffer = InMemoryWriteableCursor::default();
            let schema = parse_message_type(SCHEMA_MESSAGE)?;
            let properties = WriterProperties::builder().build();
            let writer =
                SerializedFileWriter::new(buffer.clone(), Arc::new(schema), Arc::new(properties))?;
            self.buffer = Some(buffer);
            self.writer = Some(writer);
        }
        let writer = self.writer.as_mut().unwrap();
        let mut row = writer.next_row_group()?;
        write_bytes_column(&mut row, &self.tss)?;
        write_bytes_column(&mut row, &self.keys)?;
        write_bytes_column(&mut row, &self.values)?;
        writer.close_row_group(row)?;
        Ok(())
    }
}

fn write_bytes_column(row: &mut Box<dyn RowGroupWriter>, batch: &[ByteArray]) -> Result<()> {
    let mut col = row.next_column()?.unwrap();
    match &mut col {
        ColumnWriter::ByteArrayColumnWriter(w) => {
            w.write_batch(batch, None, None)?;
        }
        _ => unimplemented!(),
    }
    row.close_column(col)?;
    Ok(())
}

#[async_trait]
impl TableBuilder for ParquetBuilder {
    async fn add(&mut self, ts: Timestamp, key: &[u8], value: &[u8]) {
        assert!(!self.finished);
        let ts_bytes = ts.to_be_bytes();
        self.tss.push(ts_bytes.to_vec().into());
        self.keys.push(key.to_vec().into());
        self.values.push(value.to_vec().into());
        self.current_group_size += ts_bytes.len() + key.len() + value.len();
        if self.current_group_size as u64 >= self.options.row_group_size {
            if let Err(error) = self.flush_row_group() {
                self.error = Some(error);
            }
            self.current_group_size = 0;
        }
    }

    async fn finish(&mut self) -> Result<usize> {
        assert!(!self.finished);
        self.finished = true;
        if let Some(error) = &self.error {
            return Err(error.clone());
        }
        if self.current_group_size > 0 {
            self.flush_row_group()?;
        }
        let data = self.buffer.take().unwrap().into_inner().unwrap();
        self.file.write(&data).await?;
        self.file.finish().await?;
        Ok(data.len())
    }
}

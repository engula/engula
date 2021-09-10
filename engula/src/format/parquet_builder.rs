use std::sync::Arc;

use async_trait::async_trait;
use parquet::{
    column::writer::ColumnWriter,
    data_type::ByteArray,
    file::{
        properties::WriterProperties,
        writer::{FileWriter, SerializedFileWriter},
    },
    schema::parser::parse_message_type,
    util::cursor::InMemoryWriteableCursor,
};

use super::{table::TableBuilder, TableDesc, Timestamp};
use crate::{
    error::{Error, Result},
    fs::SequentialWriter,
};

pub struct ParquetOptions {
    pub row_group_size: u64,
}

#[allow(dead_code)]
impl ParquetOptions {
    pub fn default() -> ParquetOptions {
        ParquetOptions {
            row_group_size: 8 * 1024 * 1024,
        }
    }
}

pub struct ParquetBuilder {
    options: ParquetOptions,
    file: ParquetFileWriter,
    done: bool,
    error: Option<Error>,
    columns: [Vec<ByteArray>; 3],
    current_group_size: u64,
}

#[allow(dead_code)]
impl ParquetBuilder {
    pub fn new(
        options: ParquetOptions,
        table_writer: Box<dyn SequentialWriter>,
        table_number: u64,
    ) -> ParquetBuilder {
        ParquetBuilder {
            options,
            file: ParquetFileWriter::new(table_writer, table_number),
            done: false,
            error: None,
            columns: [Vec::new(), Vec::new(), Vec::new()],
            current_group_size: 0,
        }
    }
}

#[async_trait]
impl TableBuilder for ParquetBuilder {
    async fn add(&mut self, ts: Timestamp, key: &[u8], value: &[u8]) {
        assert!(!self.done);
        let ts_bytes = ts.to_be_bytes();
        self.columns[0].push(ts_bytes.to_vec().into());
        self.columns[1].push(key.to_vec().into());
        self.columns[2].push(value.to_vec().into());
        self.current_group_size += (ts_bytes.len() + key.len() + value.len()) as u64;
        if self.current_group_size >= self.options.row_group_size {
            if let Err(error) = self.file.write_row_group(&self.columns) {
                self.error = Some(error);
            }
            self.current_group_size = 0;
        }
    }

    async fn finish(&mut self) -> Result<TableDesc> {
        assert!(!self.done);
        self.done = true;
        if let Some(error) = &self.error {
            return Err(error.clone());
        }
        if self.current_group_size > 0 {
            self.file.write_row_group(&self.columns)?;
        }
        self.file.finish().await
    }
}

const SCHEMA_MESSAGE: &str = "
    message schema {
        REQUIRED BYTE_ARRAY ts;
        REQUIRED BYTE_ARRAY key;
        REQUIRED BYTE_ARRAY value;
    }
";

struct ParquetFileWriter {
    file: Box<dyn SequentialWriter>,
    number: u64,
    buffer: Option<InMemoryWriteableCursor>,
    writer: SerializedFileWriter<InMemoryWriteableCursor>,
}

impl ParquetFileWriter {
    fn new(file: Box<dyn SequentialWriter>, number: u64) -> ParquetFileWriter {
        let buffer = InMemoryWriteableCursor::default();
        let schema = parse_message_type(SCHEMA_MESSAGE).unwrap();
        let properties = WriterProperties::builder().build();
        let writer =
            SerializedFileWriter::new(buffer.clone(), Arc::new(schema), Arc::new(properties))
                .unwrap();
        ParquetFileWriter {
            file,
            number,
            buffer: Some(buffer),
            writer,
        }
    }

    fn write_row_group(&mut self, columns: &[Vec<ByteArray>]) -> Result<()> {
        let mut row = self.writer.next_row_group()?;
        for column in columns {
            let mut col = row.next_column()?.unwrap();
            match &mut col {
                ColumnWriter::ByteArrayColumnWriter(w) => {
                    w.write_batch(column, None, None)?;
                }
                _ => unimplemented!(),
            }
            row.close_column(col)?;
        }
        self.writer.close_row_group(row)?;
        Ok(())
    }

    async fn finish(&mut self) -> Result<TableDesc> {
        let data = self.buffer.take().unwrap().into_inner().unwrap();
        self.file.write(&data).await?;
        self.file.finish().await?;
        Ok(TableDesc {
            table_number: self.number,
            table_size: data.len() as u64,
        })
    }
}

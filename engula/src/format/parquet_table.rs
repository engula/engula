use std::{convert::TryInto, io::Cursor, sync::Arc};

use async_trait::async_trait;
use futures::executor::block_on;
use parquet::{
    column::writer::ColumnWriter,
    data_type::ByteArray,
    errors::ParquetError,
    file::{
        properties::WriterProperties,
        reader::{ChunkReader, FileReader, Length},
        serialized_reader::SerializedFileReader,
        writer::{FileWriter, SerializedFileWriter},
    },
    record::Field,
    schema::parser::parse_message_type,
    util::cursor::InMemoryWriteableCursor,
};

use super::{
    iterator::{Entry, Iterator},
    table::{TableBuilder, TableReader},
    TableDesc, Timestamp,
};
use crate::{
    error::{Error, Result},
    fs::{RandomAccessReader, SequentialWriter},
};

#[derive(Clone)]
pub struct ParquetOptions {
    pub row_group_size: u64,
}

impl ParquetOptions {
    #[allow(dead_code)]
    pub fn default() -> ParquetOptions {
        ParquetOptions {
            row_group_size: 8 * 1024 * 1024,
        }
    }
}

pub struct ParquetBuilder {
    options: ParquetOptions,
    done: bool,
    error: Option<Error>,
    writer: RowGroupWriter,
    columns: [Vec<ByteArray>; 3],
    current_group_size: u64,
}

impl ParquetBuilder {
    pub fn new(
        options: ParquetOptions,
        table_writer: Box<dyn SequentialWriter>,
        table_number: u64,
    ) -> ParquetBuilder {
        ParquetBuilder {
            options,
            done: false,
            error: None,
            writer: RowGroupWriter::new(table_writer, table_number),
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
            if let Err(error) = self.writer.write_group(&self.columns) {
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
            self.writer.write_group(&self.columns)?;
        }
        self.writer.finish().await
    }
}

pub struct ParquetReader {
    reader: SerializedFileReader<ColumnChunkReader>,
}

impl ParquetReader {
    pub fn new(file: Box<dyn RandomAccessReader>, desc: TableDesc) -> Result<ParquetReader> {
        let file = ColumnChunkReader::new(file, desc);
        let reader = SerializedFileReader::new(file)?;
        Ok(ParquetReader { reader })
    }
}

#[async_trait]
impl TableReader for ParquetReader {
    async fn get(&self, _: Timestamp, _: &[u8]) -> Result<Option<Vec<u8>>> {
        // Parquet files do not support point gets.
        unimplemented!();
    }

    fn new_iterator(&self) -> Box<dyn Iterator> {
        Box::new(RowIterator::new(&self.reader))
    }
}

macro_rules! next_bytes_column {
    ($iter:ident) => {
        if let Field::Bytes(bytes) = $iter.next().unwrap().1 {
            bytes.clone()
        } else {
            panic!();
        }
    };
}

pub struct RowIterator {
    rows: Vec<[ByteArray; 3]>,
    index: usize,
    error: Option<Error>,
}

impl RowIterator {
    fn new(reader: &SerializedFileReader<ColumnChunkReader>) -> RowIterator {
        match reader.get_row_iter(None) {
            Ok(row_iter) => {
                let mut rows = Vec::new();
                for row in row_iter {
                    let mut col_iter = row.get_column_iter();
                    let columns = [
                        next_bytes_column!(col_iter), // ts
                        next_bytes_column!(col_iter), // key
                        next_bytes_column!(col_iter), // value
                    ];
                    rows.push(columns);
                }
                RowIterator {
                    rows,
                    index: 0,
                    error: None,
                }
            }
            Err(err) => RowIterator {
                rows: Vec::new(),
                index: 0,
                error: Some(err.into()),
            },
        }
    }
}

#[async_trait]
impl Iterator for RowIterator {
    async fn seek_to_first(&mut self) {
        self.index = 0;
    }

    async fn seek(&mut self, _: Timestamp, _: &[u8]) {
        // Parquet files do not support point gets.
        unimplemented!();
    }

    async fn next(&mut self) {
        if self.index < self.rows.len() {
            self.index += 1;
        }
    }

    fn current(&self) -> Result<Option<Entry>> {
        if let Some(error) = &self.error {
            return Err(error.clone());
        }
        if self.index >= self.rows.len() {
            return Ok(None);
        }
        let row = &self.rows[self.index];
        let ts_bytes = row[0].data();
        let ts = u64::from_be_bytes(ts_bytes.try_into().unwrap());
        Ok(Some(Entry(ts, row[1].data(), row[2].data())))
    }
}

const SCHEMA_MESSAGE: &str = "
    message schema {
        REQUIRED BYTE_ARRAY ts;
        REQUIRED BYTE_ARRAY key;
        REQUIRED BYTE_ARRAY value;
    }
";

struct RowGroupWriter {
    file: Box<dyn SequentialWriter>,
    number: u64,
    buffer: Option<InMemoryWriteableCursor>,
    writer: Option<SerializedFileWriter<InMemoryWriteableCursor>>,
}

impl RowGroupWriter {
    fn new(file: Box<dyn SequentialWriter>, number: u64) -> RowGroupWriter {
        let buffer = InMemoryWriteableCursor::default();
        let schema = parse_message_type(SCHEMA_MESSAGE).unwrap();
        let properties = WriterProperties::builder().build();
        let writer =
            SerializedFileWriter::new(buffer.clone(), Arc::new(schema), Arc::new(properties))
                .unwrap();
        RowGroupWriter {
            file,
            number,
            buffer: Some(buffer),
            writer: Some(writer),
        }
    }

    fn write_group(&mut self, columns: &[Vec<ByteArray>]) -> Result<()> {
        let writer = self.writer.as_mut().unwrap();
        let mut row = writer.next_row_group()?;
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
        writer.close_row_group(row)?;
        Ok(())
    }

    fn close_writer(&mut self) -> Result<()> {
        let mut writer = self.writer.take().unwrap();
        writer.close()?;
        Ok(())
    }

    async fn finish(&mut self) -> Result<TableDesc> {
        self.close_writer()?;
        let buffer = self.buffer.take().unwrap();
        let buffer = buffer.into_inner().unwrap();
        let buffer_size = buffer.len() as u64;
        self.file.write(buffer).await;
        self.file.finish().await?;
        Ok(TableDesc {
            table_number: self.number,
            parquet_table_size: buffer_size,
            ..Default::default()
        })
    }
}

type ParquetResult<T> = std::result::Result<T, ParquetError>;

struct ColumnChunkReader {
    file: Box<dyn RandomAccessReader>,
    desc: TableDesc,
}

impl ColumnChunkReader {
    fn new(file: Box<dyn RandomAccessReader>, desc: TableDesc) -> ColumnChunkReader {
        ColumnChunkReader { file, desc }
    }
}

impl Length for ColumnChunkReader {
    fn len(&self) -> u64 {
        self.desc.parquet_table_size
    }
}

impl ChunkReader for ColumnChunkReader {
    type T = Cursor<Vec<u8>>;

    fn get_read(&self, start: u64, length: usize) -> ParquetResult<Self::T> {
        match block_on(self.file.read_at(start, length as u64)) {
            Ok(data) => Ok(Cursor::new(data)),
            Err(err) => Err(ParquetError::General(err.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        format::Entry,
        fs::{Fs, LocalFs},
    };

    #[tokio::test]
    async fn test() {
        const NUM: u64 = 1024;
        let fs = LocalFs::new("/tmp/engula_test").unwrap();
        let options = ParquetOptions::default();
        let desc = {
            let file = fs.new_sequential_writer("test.parquet").await.unwrap();
            let mut builder = ParquetBuilder::new(options.clone(), file, 0);
            for i in 0..NUM {
                let v = i.to_be_bytes();
                builder.add(i, &v, &v).await;
            }
            builder.finish().await.unwrap()
        };
        let file = fs.new_random_access_reader("test.parquet").await.unwrap();
        let reader = ParquetReader::new(file, desc).unwrap();
        let mut iter = reader.new_iterator();
        iter.seek_to_first().await;
        for i in 0..NUM {
            let v = i.to_be_bytes();
            assert_eq!(iter.current().unwrap(), Some(Entry(i, &v, &v)));
            iter.next().await;
        }
        assert_eq!(iter.current().unwrap(), None);
    }
}

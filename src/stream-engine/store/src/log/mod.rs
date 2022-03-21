// Copyright 2022 The Engula Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod format;
pub mod reader;
pub mod writer;

#[cfg(test)]
mod tests {
    use std::{
        fs::File,
        path::{Path, PathBuf},
    };

    use rand::prelude::*;

    use super::{format::MAX_BLOCK_SIZE, reader::Reader as LogReader, writer::Writer as LogWriter};
    use crate::{fs::FileExt, Result};

    const MAX_FILE_SIZE: usize = MAX_BLOCK_SIZE * 1024;

    fn new_tempdir() -> Result<PathBuf> {
        let dir = tempfile::tempdir()?.path().to_owned();
        std::fs::create_dir_all(&dir)?;
        Ok(dir)
    }

    fn new_writer(dir: &Path, log_number: u64) -> Result<LogWriter> {
        new_writer_with_offset(dir, log_number, 0)
    }

    fn new_writer_with_offset(dir: &Path, log_number: u64, offset: usize) -> Result<LogWriter> {
        let mut file = File::options()
            .write(true)
            .create(true)
            .open(dir.join(format!("{}.log", log_number)))?;
        file.preallocate(MAX_FILE_SIZE)?;
        let writer = LogWriter::new(file, log_number, offset, MAX_FILE_SIZE)?;
        Ok(writer)
    }

    fn new_reader(dir: &Path, log_number: u64) -> Result<LogReader> {
        let filename = dir.join(format!("{}.log", log_number));
        assert!(filename.exists());
        let file = File::open(filename).unwrap();
        LogReader::new(file, log_number, true)
    }

    #[test]
    fn log_writer_and_reader_one_record() -> Result<()> {
        let dir = new_tempdir()?;
        let content = vec![0u8, 1, 2, 3, 4, 5, 6];
        let mut writer = new_writer(&dir, 1)?;
        writer.add_record(&content)?;
        drop(writer);

        let mut reader = new_reader(&dir, 1)?;
        let result = reader.read_record()?;
        assert!(result.is_some());
        let got = result.unwrap();
        assert_eq!(content, got);

        Ok(())
    }

    #[test]
    fn log_writer_and_reader_multiple_record() -> Result<()> {
        struct TestCase {
            group: Vec<Vec<u8>>,
        }

        let tests = vec![
            TestCase {
                group: vec![
                    // 1. simple record
                    vec![0u8],
                ],
            },
            TestCase {
                group: vec![
                    // 2. empty record
                    vec![],
                ],
            },
            TestCase {
                group: vec![
                    // 3. group record
                    vec![2u8],
                    vec![2u8, 3u8, 4u8, 5u8],
                ],
            },
        ];

        let dir = new_tempdir()?;
        let mut writer = new_writer(&dir, 1)?;
        let mut expect = vec![];
        for case in &tests {
            for content in &case.group {
                writer.add_record(content)?;
                expect.push(content.clone());
            }
            writer.flush()?;
        }
        drop(writer);

        let mut reader = new_reader(&dir, 1)?;
        let mut got = vec![];
        while let Some(content) = reader.read_record()? {
            got.push(content);
        }

        assert_eq!(expect, got);

        Ok(())
    }

    #[test]
    fn log_writer_and_reader_record_cross_block() -> Result<()> {
        let dir = new_tempdir()?;
        let mut writer = new_writer(&dir, 1)?;

        let mut expects = vec![];

        // 1. full
        let content = vec![0u8, 1, 2, 3, 4, 5, 6];
        writer.add_record(&content)?;
        expects.push(content);

        // 2. head and tail
        let avail = writer.block_avail_space();
        let content = vec![3u8; avail + 1];
        writer.add_record(&content)?;
        expects.push(content);

        // 3. middle
        let avail = writer.block_avail_space();
        let content = vec![4u8; avail + MAX_BLOCK_SIZE + 1];
        writer.add_record(&content)?;
        expects.push(content);

        drop(writer);

        let mut reader = new_reader(&dir, 1)?;
        let mut got = vec![];
        while let Some(content) = reader.read_record()? {
            got.push(content);
        }

        assert_eq!(expects, got);

        Ok(())
    }

    #[test]
    fn log_writer_and_reader_randomly() -> Result<()> {
        let dir = new_tempdir()?;

        let mut log_number = 123;
        let mut writer = new_writer(&dir, log_number)?;

        let random_bytes: Vec<u8> = (0..1024).map(|_| rand::random::<u8>()).collect();

        let mut files = vec![];
        let mut expect_contents = vec![];
        loop {
            let start = rand::thread_rng().gen::<usize>() % 512;
            let end = start + rand::thread_rng().gen::<usize>() % 512;

            let content = &random_bytes[start..end];
            if writer.avail_space() < content.len() {
                writer.flush()?;
                files.push(log_number);
                if files.len() >= 2 {
                    break;
                }

                log_number += 1;
                writer = new_writer(&dir, log_number)?;
            }

            writer.add_record(content)?;
            expect_contents.push(content.to_owned());
        }

        let mut read_contents = vec![];
        for log_number in files {
            let mut reader = new_reader(&dir, log_number)?;
            while let Some(content) = reader.read_record()? {
                read_contents.push(content);
            }
        }

        assert_eq!(read_contents.len(), expect_contents.len());
        for i in 0..read_contents.len() {
            assert_eq!(read_contents[i], expect_contents[i], "index {}", i);
        }

        Ok(())
    }

    #[test]
    fn log_writer_and_reader_recycled_log_file() -> Result<()> {
        let dir = new_tempdir()?;

        let random_bytes: Vec<u8> = (0..1024).map(|_| rand::random::<u8>()).collect();

        let mut writer = new_writer(&dir, 1)?;
        loop {
            let start = rand::thread_rng().gen::<usize>() % 512;
            let end = start + rand::thread_rng().gen::<usize>() % 512;

            let content = &random_bytes[start..end];
            if writer.avail_space() < content.len() {
                writer.flush()?;
                break;
            }
            writer.add_record(content)?;
        }

        drop(writer);

        let mut expect_contents = vec![];
        let mut writer = new_writer(&dir, 2)?;
        for size in 0..1024 {
            let content = vec![size as u8 % u8::MAX; size];
            writer.add_record(&content)?;
            expect_contents.push(content);
        }
        drop(writer);

        let mut read_contents = vec![];
        let mut reader = new_reader(&dir, 2)?;
        while let Some(content) = reader.read_record()? {
            read_contents.push(content);
        }

        assert_eq!(read_contents.len(), expect_contents.len());
        for i in 0..read_contents.len() {
            assert_eq!(read_contents[i], expect_contents[i], "index {}", i);
        }

        Ok(())
    }

    #[test]
    fn log_writer_and_reader_recover_and_reuse() -> Result<()> {
        let dir = new_tempdir()?;
        let mut expect_contents = vec![];
        let mut writer = new_writer(&dir, 1)?;
        for size in 0..1024 {
            let content = vec![size as u8 % u8::MAX; size];
            writer.add_record(&content)?;
            expect_contents.push(content);
        }
        drop(writer);

        let mut read_contents = vec![];
        let mut reader = new_reader(&dir, 1)?;
        while let Some(content) = reader.read_record()? {
            read_contents.push(content);
        }

        assert_eq!(read_contents.len(), expect_contents.len());
        for i in 0..read_contents.len() {
            assert_eq!(read_contents[i], expect_contents[i], "index {}", i);
        }

        let restart_offset = reader.next_record_offset();
        let mut writer = new_writer_with_offset(&dir, 1, restart_offset)?;
        for size in 0..1024 {
            let content = vec![size as u8 % (u8::MAX - 1) + 1; size];
            writer.add_record(&content)?;
            expect_contents.push(content);
        }
        drop(writer);

        let mut read_contents = vec![];
        let mut reader = new_reader(&dir, 1)?;
        while let Some(content) = reader.read_record()? {
            read_contents.push(content);
        }

        Ok(())
    }
}

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

use std::path::{Path, PathBuf};

use crate::{Error, Result};

pub enum FileType {
    Unknown,
    Current,
    Manifest(u64),
    Log(u64),
    Temp,
}

pub fn current<P: AsRef<Path>>(base_dir: P) -> PathBuf {
    base_dir.as_ref().join("CURRENT")
}

pub fn manifest(file_number: u64) -> String {
    format!("MANIFEST-{:06}", file_number)
}

pub fn descriptor<P: AsRef<Path>>(base_dir: P, file_number: u64) -> PathBuf {
    base_dir.as_ref().join(&manifest(file_number))
}

pub fn log<P: AsRef<Path>>(base_dir: P, file_number: u64) -> PathBuf {
    let name = format!("{:09}.log", file_number);
    base_dir.as_ref().join(&name)
}

pub fn temp<P: AsRef<Path>>(base_dir: P, file_number: u64) -> PathBuf {
    let name = format!("{:09}.tmp", file_number);
    base_dir.as_ref().join(&name)
}

pub fn parse_file_name<P: AsRef<Path>>(path: P) -> Result<FileType> {
    let path = path.as_ref();
    if !path.is_file() {
        return Err(Error::InvalidArgument("target isn't a file".to_string()));
    }

    let name = path.file_name().and_then(|s| s.to_str()).unwrap();
    if name == "CURRENT" {
        Ok(FileType::Current)
    } else if name.starts_with("MANIFEST-") {
        let rest = name.strip_prefix("MANIFEST-").unwrap();
        match rest.parse::<u64>() {
            Ok(file_number) => Ok(FileType::Manifest(file_number)),
            Err(_) => Ok(FileType::Unknown),
        }
    } else if name.ends_with(".log") {
        let rest = name.strip_suffix(".log").unwrap();
        match rest.parse::<u64>() {
            Ok(file_number) => Ok(FileType::Log(file_number)),
            Err(_) => Ok(FileType::Unknown),
        }
    } else if name.ends_with(".tmp") {
        Ok(FileType::Temp)
    } else {
        Ok(FileType::Unknown)
    }
}

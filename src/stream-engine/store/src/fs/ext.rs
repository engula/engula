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

use std::{fs::File, io::Error};

pub trait FileExt {
    /// Sync the specifies file range in async.
    ///
    /// [`offset`] is the starting byte of the file range to be synchronized.
    /// [`len`] specifies the length of the range to be synchronized, in bytes.
    /// If then is zero, then all bytes from [`offset`] through to the end of
    /// file are synchronized. Synchronization is in units of the system page
    /// size: [`offset`] is rounded down to a page boundary;
    /// ([`offset`]+[`len`]-1) is rounded up to a page boundary.
    fn sync_range(&mut self, offset: usize, len: usize) -> Result<(), Error>;

    /// Allow caller to directly allocate disk space for the specifies file,
    /// then the range did not contain data will be initialized to zero. After a
    /// successful call, subsequent writes blow file size are guaranteed not
    /// to fail because of lack of disk space.
    fn preallocate(&mut self, len: usize) -> Result<(), Error>;
}

impl FileExt for File {
    fn sync_range(&mut self, offset: usize, len: usize) -> Result<(), Error> {
        #[cfg(target_os = "linux")]
        unsafe {
            use std::os::unix::io::AsRawFd;

            let retval = libc::sync_file_range(
                self.as_raw_fd(),
                offset as i64,
                len as i64,
                libc::SYNC_FILE_RANGE_WRITE,
            );

            if retval == -1 {
                return Err(std::io::Error::last_os_error());
            }
        }

        #[cfg(not(target_os = "linux"))]
        {
            let _ = offset;
            let _ = len;
        }

        Ok(())
    }

    fn preallocate(&mut self, len: usize) -> Result<(), Error> {
        #[cfg(target_os = "linux")]
        unsafe {
            use std::os::unix::io::AsRawFd;

            let retval = libc::fallocate(self.as_raw_fd(), 0, 0, len as i64);
            if retval == -1 {
                return Err(std::io::Error::last_os_error());
            }
        }

        #[cfg(target_os = "macos")]
        unsafe {
            use std::os::unix::io::AsRawFd;

            let retval = libc::ftruncate(self.as_raw_fd(), len as i64);
            if retval != 0 {
                return Err(std::io::Error::from_raw_os_error(retval));
            }
        }

        Ok(())
    }
}

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

use std::{alloc::Layout, ptr};

use libc::{c_int, c_void, off_t, size_t};

/// # Panics
///
/// This method may panic if the align of `layout` is greater than the kernel page align.
/// (Basically, kernel page align is always greater than the align of `layout` that rust
/// generates unless the programer dares to build such a `layout` on purpose.)
#[inline]
pub unsafe fn alloc(layout: Layout) -> *mut u8 {
    const ADDR: *mut c_void = ptr::null_mut::<c_void>();
    let length = layout.size() as size_t;
    const PROT: c_int = libc::PROT_READ | libc::PROT_WRITE;

    // No backend file.
    const FLAGS: c_int = libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_POPULATE;
    const FD: c_int = -1; // Should be -1 if flags includes MAP_ANONYMOUS. See `man 2 mmap`
    const OFFSET: off_t = 0; // Should be 0 if flags includes MAP_ANONYMOUS. See `man 2 mmap`

    match libc::mmap(ADDR, length, PROT, FLAGS, FD, OFFSET) {
        libc::MAP_FAILED => ptr::null_mut::<u8>(),
        ret => {
            let ptr = ret as usize;
            assert_eq!(0, ptr % layout.align());
            ret as *mut u8
        }
    }
}

#[inline]
pub unsafe fn dealloc(ptr: *mut u8, layout: Layout) {
    let addr = ptr as *mut c_void;
    let length = layout.size() as size_t;

    libc::munmap(addr, length);
}

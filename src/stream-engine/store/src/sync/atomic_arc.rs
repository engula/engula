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

use std::sync::{
    atomic::{AtomicPtr, Ordering},
    Arc,
};

#[repr(transparent)]
pub struct AtomicArcPtr<T>(Arc<AtomicPtr<T>>);

impl<T> AtomicArcPtr<T> {
    #[inline(always)]
    pub fn new(t: Box<T>) -> Self {
        AtomicArcPtr(Arc::new(AtomicPtr::new(Box::leak(t))))
    }

    #[inline(always)]
    pub fn try_deref(&self) -> Option<&T> {
        unsafe { self.0.load(Ordering::Acquire).as_ref() }
    }

    /// Store a value into the pointer if the current value is null. Otherwise
    /// return the input argument.
    #[inline(always)]
    pub fn compare_store(&self, t: Box<T>) -> Result<(), Box<T>> {
        self.0
            .compare_exchange(
                std::ptr::null_mut(),
                Box::leak(t),
                Ordering::SeqCst,
                Ordering::Relaxed,
            )
            .map(|_| ())
            .map_err(|raw_ptr| unsafe { Box::from_raw(raw_ptr) })
    }
}

impl<T> From<Box<T>> for AtomicArcPtr<T> {
    fn from(box_ptr: Box<T>) -> Self {
        AtomicArcPtr::new(box_ptr)
    }
}

impl<T> Default for AtomicArcPtr<T> {
    fn default() -> Self {
        AtomicArcPtr(Arc::new(AtomicPtr::default()))
    }
}

impl<T> Clone for AtomicArcPtr<T> {
    fn clone(&self) -> Self {
        AtomicArcPtr(self.0.clone())
    }
}

impl<T> Drop for AtomicArcPtr<T> {
    fn drop(&mut self) {
        let arc = std::mem::replace(&mut self.0, Arc::new(AtomicPtr::new(std::ptr::null_mut())));
        if let Ok(atomic_ptr) = Arc::<AtomicPtr<T>>::try_unwrap(arc) {
            let ptr = atomic_ptr.into_inner();
            if !ptr.is_null() {
                unsafe {
                    Box::from_raw(ptr);
                }
            }
        }
    }
}

// Copyright 2021 The Engula Authors.
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

mod read;
mod read_ext;

pub use self::{
    read::Read,
    read_ext::{ReadExt, ReadFromPosExt},
};

#[cfg(test)]
mod tests {

    use futures::AsyncReadExt;

    use crate::io::random::read_ext::ReadFromPosExt;

    #[tokio::test]
    async fn read() {
        let data = vec![0u8; 4];
        let mut buf = vec![0u8; 3];
        let n = data
            .as_slice()
            .to_async_read(0)
            .read(&mut buf)
            .await
            .unwrap();
        assert_eq!(n, 3);
        let n = data
            .as_slice()
            .to_async_read(2)
            .read(&mut buf)
            .await
            .unwrap();
        assert_eq!(n, 2);
    }
}

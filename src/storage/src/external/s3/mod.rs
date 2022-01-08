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

mod error;
mod storage;

pub use self::storage::Storage;

#[cfg(test)]
mod tests {

    use engula_futures::{io::RandomReadExt, stream::batch::ResultStreamExt};
    use futures::AsyncWriteExt;

    use crate::Storage;

    #[tokio::test]
    async fn test_s3() {
        let shared_config = aws_config::load_from_env().await;
        let s = super::Storage::new("t1", "test-engula-test2", &shared_config)
            .await
            .unwrap();

        s.create_bucket("b1").await.unwrap();
        s.create_bucket("b2").await.unwrap();
        let buckets = s
            .list_buckets()
            .await
            .unwrap()
            .batched(10)
            .collect()
            .await
            .unwrap();
        println!("buckets: {:?}", buckets);

        s.delete_bucket("b1").await.unwrap();
        let buckets = s
            .list_buckets()
            .await
            .unwrap()
            .batched(10)
            .collect()
            .await
            .unwrap();
        println!("buckets: {:?}", buckets);

        let mut w = s.new_sequential_writer("b3", "o3").await.unwrap();
        w.write(b"1234").await.unwrap();
        w.close().await.unwrap();

        let mut w = s.new_sequential_writer("b3", "o5").await.unwrap();
        w.write(b"4321").await.unwrap();
        w.close().await.unwrap();

        let obj = s
            .list_objects("b3")
            .await
            .unwrap()
            .batched(10)
            .collect()
            .await
            .unwrap();
        println!("objects: {:?}", obj);

        s.delete_object("b3", "o3").await.unwrap();

        let obj = s
            .list_objects("b3")
            .await
            .unwrap()
            .batched(10)
            .collect()
            .await
            .unwrap();
        println!("objects: {:?}", obj);

        let r = s.new_random_reader("b3", "o5").await.unwrap();
        let mut buf = vec![0; 2];
        let pos = 1;
        r.read_exact(&mut buf, pos).await.unwrap();
        let ss = String::from_utf8_lossy(&buf);
        println!("data: {}", ss);

        s.delete_bucket("b3").await.unwrap();
    }
}

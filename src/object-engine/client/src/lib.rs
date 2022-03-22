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

mod bucket;
mod bulkload;
mod engine;
mod env;
mod sst_builder;
mod tenant;

pub use object_engine_common::{async_trait, Error, Result};

use self::env::{BucketEnv, Env, TenantEnv};
pub use self::{
    bucket::Bucket,
    bulkload::BulkLoad,
    engine::Engine,
    env::{LocalEnv, RemoteEnv},
    sst_builder::SstBuilder,
    tenant::Tenant,
};

#[cfg(test)]
mod testmode {

    use super::*;

    #[tokio::test]
    async fn test_get_iter_put() -> Result<()> {
        let p = tempfile::tempdir()?;
        println!("{:?}", p.path());
        let env1 = LocalEnv::open(p.path()).await?;

        let eng = Engine::open(env1).await?;

        eng.create_tenant("t1").await?;

        let tenant = eng.tenant("t1").await?;

        tenant.create_bucket("b1").await?;
        tenant.create_bucket("b2").await?;

        let bucket1 = tenant.bucket("b1").await?;
        let bucket2 = tenant.bucket("b2").await?;

        let mut bulk_load = tenant.begin_bulkload().await?;
        let mut t1 = bulk_load.new_sst_builder(&bucket1).await?;
        t1.put(b"k1", 1, b"123").await?;
        t1.put(b"k2", 2, b"456").await?;
        bulk_load.finish_sst_builder(t1).await?;

        let mut t2 = bulk_load.new_sst_builder(&bucket2).await?;
        t2.put(b"k3", 3, b"abc").await?;
        t2.put(b"k4", 4, b"efg").await?;
        bulk_load.finish_sst_builder(t2).await?;

        let mut t3 = bulk_load.new_sst_builder(&bucket1).await?;
        t3.put(b"k1", 5, b"a").await?;
        t3.put(b"k3", 6, b"abc").await?;
        t3.put(b"k4", 7, b"efg").await?;
        t3.put(b"k5", 8, b"cce").await?;
        bulk_load.finish_sst_builder(t3).await?;

        bulk_load.commit().await?;

        let v1 = bucket1.get(b"k1").await?;
        assert_eq!(v1.unwrap(), b"a");
        let v1 = bucket1.get(b"k2").await?;
        assert_eq!(v1.unwrap(), b"456");
        let v2 = bucket2.get(b"k4").await?;
        assert_eq!(v2.unwrap(), b"efg");

        let mut b1_iter = bucket1.iter().await?;
        b1_iter.seek_to_first().await?;
        assert_eq!(b1_iter.value(), b"a");
        b1_iter.next().await?;
        assert_eq!(b1_iter.value(), b"456");
        b1_iter.next().await?;
        assert_eq!(b1_iter.value(), b"abc");
        b1_iter.next().await?;
        assert_eq!(b1_iter.value(), b"efg");
        b1_iter.next().await?;
        assert_eq!(b1_iter.value(), b"cce");
        b1_iter.next().await?;
        assert!(!b1_iter.valid());

        b1_iter.seek(b"k2").await?;
        assert!(b1_iter.valid());
        assert_eq!(b1_iter.value(), b"456");

        Ok(())
    }

    #[tokio::test]
    async fn test_get_iter_delete() -> Result<()> {
        let p = tempfile::tempdir()?;
        println!("{:?}", p.path());
        let env1 = LocalEnv::open(p.path()).await?;
        let eng = Engine::open(env1).await?;

        eng.create_tenant("t1").await?;
        let tenant = eng.tenant("t1").await?;

        tenant.create_bucket("b1").await?;
        let bucket1 = tenant.bucket("b1").await?;

        let mut bulk_load = tenant.begin_bulkload().await?;
        let mut t1 = bulk_load.new_sst_builder(&bucket1).await?;
        t1.put(b"k1", 1, b"123").await?;
        t1.put(b"k2", 1, b"456").await?;
        t1.put(b"k3", 1, b"789").await?;
        t1.put(b"k4", 1, b"000").await?;
        t1.put(b"k5", 1, b"abc").await?;
        bulk_load.finish_sst_builder(t1).await?;
        bulk_load.commit().await?;

        let mut bulk_load = tenant.begin_bulkload().await?;
        let mut t1 = bulk_load.new_sst_builder(&bucket1).await?;
        t1.delete(b"k1", 2).await?;
        t1.put(b"k2", 2, b"457").await?;
        t1.delete(b"k3", 2).await?;
        t1.put(b"k5", 2, b"111").await?;
        bulk_load.finish_sst_builder(t1).await?;
        bulk_load.commit().await?;

        assert_eq!(None, bucket1.get(b"k1").await?);
        let v2 = bucket1.get(b"k2").await?.unwrap();
        assert_eq!(v2, b"457");
        assert_eq!(None, bucket1.get(b"k3").await?);
        let v4 = bucket1.get(b"k4").await?.unwrap();
        assert_eq!(v4, b"000");
        let v5 = bucket1.get(b"k5").await?.unwrap();
        assert_eq!(v5, b"111");

        let mut b1_iter = bucket1.iter().await?;
        b1_iter.seek_to_first().await?;
        assert_eq!(b1_iter.value(), b"457");
        b1_iter.next().await?;
        assert_eq!(b1_iter.value(), b"000");
        b1_iter.next().await?;
        assert_eq!(b1_iter.value(), b"111");
        b1_iter.seek(b"k3").await?;
        assert_eq!(b1_iter.key(), b"k4");

        Ok(())
    }
}

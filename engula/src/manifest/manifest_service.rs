use tonic::{Request, Response, Status};

use super::{
    manifest_server::{self, ManifestServer},
    AddTableRequest, AddTableResponse, CurrentRequest, CurrentResponse, Manifest,
    NextNumberRequest, NextNumberResponse,
};

pub struct ManifestService {
    manifest: Box<dyn Manifest>,
}

impl ManifestService {
    #[allow(dead_code)]
    pub fn new(manifest: Box<dyn Manifest>) -> ManifestService {
        ManifestService { manifest }
    }
}

#[tonic::async_trait]
impl manifest_server::Manifest for ManifestService {
    async fn current(
        &self,
        _: Request<CurrentRequest>,
    ) -> Result<Response<CurrentResponse>, Status> {
        let version = self.manifest.current().await?;
        Ok(Response::new(CurrentResponse {
            version: Some(version),
        }))
    }

    async fn add_table(
        &self,
        request: Request<AddTableRequest>,
    ) -> Result<Response<AddTableResponse>, Status> {
        let input = request.into_inner();
        let table = input.table.unwrap();
        let version = self.manifest.add_table(table).await?;
        Ok(Response::new(AddTableResponse {
            version: Some(version),
        }))
    }

    async fn next_number(
        &self,
        _: Request<NextNumberRequest>,
    ) -> Result<Response<NextNumberResponse>, Status> {
        let number = self.manifest.next_number().await?;
        Ok(Response::new(NextNumberResponse { number }))
    }
}

impl From<ManifestService> for ManifestServer<ManifestService> {
    fn from(s: ManifestService) -> ManifestServer<ManifestService> {
        ManifestServer::new(s)
    }
}

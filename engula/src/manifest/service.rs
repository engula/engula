use tonic::{Request, Response, Status};

use super::{
    manifest_server, GetCurrentRequest, GetCurrentResponse, InstallFlushRequest,
    InstallFlushResponse, Manifest, NextNumberRequest, NextNumberResponse,
};

pub struct Service {
    manifest: Box<dyn Manifest>,
}

impl Service {
    #[allow(dead_code)]
    pub fn new(manifest: Box<dyn Manifest>) -> Service {
        Service { manifest }
    }
}

#[tonic::async_trait]
impl manifest_server::Manifest for Service {
    async fn get_current(
        &self,
        _: Request<GetCurrentRequest>,
    ) -> Result<Response<GetCurrentResponse>, Status> {
        let version = self.manifest.current().await?;
        Ok(Response::new(GetCurrentResponse {
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

    async fn install_flush(
        &self,
        request: Request<InstallFlushRequest>,
    ) -> Result<Response<InstallFlushResponse>, Status> {
        let input = request.into_inner();
        let file = input.file.unwrap();
        let version = self.manifest.install_flush(file).await?;
        Ok(Response::new(InstallFlushResponse {
            version: Some(version),
        }))
    }
}

impl From<Service> for manifest_server::ManifestServer<Service> {
    fn from(s: Service) -> manifest_server::ManifestServer<Service> {
        manifest_server::ManifestServer::new(s)
    }
}

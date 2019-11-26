use crate::runner::RuntimeConfig;

use hyper::server::Server;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Request, Response, Body, Method};
use tokio::runtime::TaskExecutor;

use std::net::SocketAddr;
use std::sync::Arc;

pub(crate) async fn start(executor: TaskExecutor, port: u16, runtime_config: Arc<RuntimeConfig>, serve_metrics: bool, serve_health: bool) {

    let address: SocketAddr = ([0u8; 4], port).into();
    log::info!("Starting server on address: {}, exposing '/metrics': {}, '/health': {}", address, serve_metrics, serve_health);

    let svc = Svc::new(runtime_config.clone(), serve_metrics, serve_health);
    let service = make_service_fn( move |_| {

        let service = svc.clone();
        async move {
            let service = service;
            Ok::<_, hyper::Error>(service_fn(move |request| {
                futures_util::future::ready(service.handle_request(request))
            }))
        }
    });
    if let Err(err) = Server::bind(&address).executor(executor).serve(service).await {
        log::error!("Server failed with error: {:?}", err);
    }
}


type Error = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, Clone)]
struct Svc {
    runtime_config: Arc<RuntimeConfig>,
    serve_metrics: bool,
    serve_health: bool,
}

impl Svc {

    fn new(runtime_config: Arc<RuntimeConfig>, serve_metrics: bool, serve_health: bool) -> Svc {
        Svc {runtime_config, serve_metrics, serve_health }
    }

    fn not_found(&self, _request: &Request<Body>) -> Result<Response<Body>, Error> {
        let resp = Response::builder().status(404).body(Body::empty())?;
        Ok(resp)
    }

    fn health(&self, _request: &Request<Body>) -> Result<Response<Body>, Error> {
        let resp = Response::builder().status(200).body(Body::empty())?;
        Ok(resp)
    }
    fn metrics(&self, _request: &Request<Body>) -> Result<Response<Body>, Error> {
        let body = self.runtime_config.metrics.encode_as_text()?;
        let resp = Response::builder().status(200)
                .header(http::header::CONTENT_TYPE, prometheus::TEXT_FORMAT)
                .body(Body::from(body))?;
        Ok(resp)
    }

    fn handle_request(&self, request: Request<Body>) -> Result<Response<Body>, Error> {
        let req_path = request.uri().path().trim_end_matches('/');
        let req_method = request.method();

        log::debug!("Got http request {} {}", req_method, request.uri());

        let result = match (req_method, req_path) {
            (&Method::GET, "/health") if self.serve_health => {
                self.health(&request)
            }
            (&Method::GET, "/metrics") if self.serve_metrics => {
                self.metrics(&request)
            }
            _ => {
                self.not_found(&request)
            }
        };
        match result.as_ref() {
            Ok(ref resp) => {
                log::debug!("Finished handling {} {} with response status: {}", req_method, request.uri(), resp.status());
            }
            Err(ref err) => {
                log::error!("Error handling {} {} , error: {:?}", req_method, request.uri(), err);
            }
        }
        result
    }
}

mod request;

use crate::config::{CAData, ClientConfig, Credentials};
use crate::k8s_types::K8sType;
use crate::resource::ObjectIdRef;
use crate::runner::metrics::ClientMetrics;

use bytes::Buf;
use http::{Request, Response};
use hyper::client::Client as HyperClient;
use hyper::client::HttpConnector;
use hyper::Body;
use hyper_openssl::HttpsConnector;
use lazy_static::lazy_static;
use openssl::pkey::PKey;
use openssl::ssl::{SslConnector, SslMethod};
use openssl::x509::X509;
use regex::bytes::Regex;
use serde::de::DeserializeOwned;
use serde_json::Value;
use tokio_stream::StreamExt;

use std::fs::File;
use std::io;
use std::io::Read;
use std::sync::Arc;
use std::time::Instant;

pub use self::request::{MergeStrategy, Patch};

lazy_static! {
    static ref NEWLINE_REGEX: Regex = Regex::new("([\\r\\n]+)").unwrap();
}

#[derive(Debug)]
pub enum Error {
    Io(hyper::Error),
    Serde(serde_json::Error),
    Http(http::StatusCode),
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e as &(dyn std::error::Error + 'static)),
            Error::Serde(e) => Some(e as &(dyn std::error::Error + 'static)),
            Error::Http(_) => None,
        }
    }
}

impl Error {
    pub fn http(status: http::StatusCode) -> Error {
        Error::Http(status)
    }

    pub fn is_http_410(&self) -> bool {
        self.is_http_status(410)
    }

    pub fn is_http_status(&self, code: u16) -> bool {
        match self {
            Error::Http(ref status) => status.as_u16() == code,
            _ => false,
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            Error::Io(ref e) => write!(f, "Io Error: {}", e),
            Error::Serde(ref e) => write!(f, "(De)Serialization error: {}", e),
            Error::Http(ref e) => write!(f, "Http Error: {}", e),
        }
    }
}

impl From<hyper::Error> for Error {
    fn from(e: hyper::Error) -> Error {
        Error::Io(e)
    }
}
impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Error {
        Error::Serde(e)
    }
}

#[derive(Debug)]
struct ClientInner {
    http_client: HyperClient<HttpsConnector<HttpConnector>>,
    config: ClientConfig,
    metrics: ClientMetrics,
}

#[derive(Debug, Clone)]
pub struct Client(Arc<ClientInner>);

impl Client {
    pub fn new(mut config: ClientConfig, metrics: ClientMetrics) -> Result<Client, io::Error> {
        let mut http = HttpConnector::new();
        http.enforce_http(false);

        let mut ssl = SslConnector::builder(SslMethod::tls())?;
        // enable http2 using alpn
        ssl.set_alpn_protos(b"\x02h2\x08http/1.1")?;
        match config.ca_data.take() {
            Some(CAData::Contents(certs)) => {
                // if the CA cert contents are provided inline, as they are from a kubeconfig file, then we need to manually
                // parse them and add them to the openssl cert store
                let decoded = base64::decode(&certs).map_err(|err| {
                    io::Error::new(
                        io::ErrorKind::Other,
                        format!(
                            "Invalid base64 content of certificate-authority-data: {}",
                            err
                        ),
                    )
                })?;
                let certs = X509::stack_from_pem(decoded.as_slice())?;
                let cert_store = ssl.cert_store_mut();
                for cert in certs {
                    cert_store.add_cert(cert)?;
                }
            }
            Some(CAData::File(path)) => {
                ssl.set_ca_file(path.as_str())?;
            }
            None => {}
        }

        if let Credentials::PemPath {
            ref certificate_path,
            ref private_key_path,
        } = config.credentials
        {
            let mut file = File::open(certificate_path)?;
            let mut file_content_cert = vec![];
            file.read_to_end(&mut file_content_cert)?;

            let mut file = File::open(private_key_path)?;
            let mut file_content_key = vec![];
            file.read_to_end(&mut file_content_key)?;

            let cert = X509::from_pem(file_content_cert.as_slice())?;
            let pkey = PKey::private_key_from_pem(file_content_key.as_slice())?;
            ssl.set_certificate(&*cert)?; // &* is to convert from X509 to &X509Ref where X509 impls Deref to X509Ref
            ssl.set_private_key(&*pkey)?; // same as above
            ssl.check_private_key()?; // ensures that the provided private key and certificate actually go together
        }

        if let Credentials::Pem {
            ref certificate_base64,
            ref private_key_base64,
        } = config.credentials
        {
            let decoded_cert = base64::decode(certificate_base64).map_err(|err| {
                io::Error::new(
                    io::ErrorKind::Other,
                    format!("Invalid base64 content of client-certificate-data: {}", err),
                )
            })?;
            let decoded_key = base64::decode(private_key_base64).map_err(|err| {
                io::Error::new(
                    io::ErrorKind::Other,
                    format!("Invalid base64 content of client-key-data: {}", err),
                )
            })?;
            let cert = X509::from_pem(decoded_cert.as_slice())?;
            let pkey = PKey::private_key_from_pem(decoded_key.as_slice())?;
            ssl.set_certificate(&*cert)?; // &* is to convert from X509 to &X509Ref where X509 impls Deref to X509Ref
            ssl.set_private_key(&*pkey)?; // same as above
            ssl.check_private_key()?; // ensures that the provided private key and certificate actually go together
        }

        if config.verify_ssl_certs {
            ssl.set_verify(openssl::ssl::SslVerifyMode::PEER);
        } else {
            log::warn!("TLS Certificate verifification has been disabled! All connections to the Kubernetes api server will be insecure!");
            ssl.set_verify(openssl::ssl::SslVerifyMode::NONE);
        }

        let https = HttpsConnector::with_connector(http, ssl)?;

        let client = HyperClient::builder().build(https);

        let inner = ClientInner {
            http_client: client,
            config,
            metrics,
        };
        Ok(Client(Arc::new(inner)))
    }

    pub async fn list_all(
        &self,
        k8s_type: &K8sType,
        namespace: Option<&str>,
        label_selector: Option<&str>,
    ) -> Result<ObjectList<Value>, Error> {
        let req = request::list_request(&self.0.config, k8s_type, label_selector, namespace)?;
        self.get_response_body(req).await
    }

    pub async fn watch(
        &self,
        k8s_type: &K8sType,
        namespace: Option<&str>,
        resource_version: Option<&str>,
        label_selector: Option<&str>,
    ) -> Result<LineDeserializer<WatchEvent>, Error> {
        let req = request::watch_request(
            &self.0.config,
            k8s_type,
            resource_version,
            label_selector,
            None,
            namespace,
        )?;
        self.get_response_lines_deserialized(req).await
    }

    pub async fn update_status(
        &self,
        k8s_type: &K8sType,
        id: &ObjectIdRef<'_>,
        new_status: &Value,
    ) -> Result<(), Error> {
        let req = request::update_status_request(&self.0.config, k8s_type, id, new_status)?;
        self.execute_ensure_success(req).await
    }

    pub async fn delete_resource(
        &self,
        k8s_type: &K8sType,
        id: &ObjectIdRef<'_>,
    ) -> Result<(), Error> {
        log::info!("Deleting resouce '{}' with type: {}", id, k8s_type);
        let req = request::delete_request(&self.0.config, k8s_type, id)?;
        let response = self.get_response(req).await?;

        match response.status().as_u16() {
            200..=299 | 404 | 409 => {
                // 404 means that something else must have already deleted the resource, which is fine by us
                // 409 status is returned when the object is already in the process of being deleted, again fine by us
                Ok(())
            }
            other => {
                log::error!(
                    "Delete request for {} : {} failed with status: {}",
                    k8s_type,
                    id,
                    other
                );
                Err(Error::http(response.status()))
            }
        }
    }

    /// gets the requested resource by name and converts a 404 response into a None value
    #[cfg(feature = "testkit")]
    pub async fn get_resource(
        &self,
        k8s_type: &K8sType,
        id: &ObjectIdRef<'_>,
    ) -> Result<Option<Value>, Error> {
        let req = request::get_request(&self.0.config, k8s_type, id)?;
        match self.get_response_body::<Value>(req).await {
            Ok(body) => Ok(Some(body)),
            Err(ref e) if e.is_http_status(404) => Ok(None),
            Err(e) => Err(e),
        }
    }

    pub async fn create_resource(&self, k8s_type: &K8sType, resource: &Value) -> Result<(), Error> {
        let req = request::create_request(&self.0.config, k8s_type, resource)?;
        self.execute_ensure_success(req).await
    }

    pub async fn replace_resource(
        &self,
        k8s_type: &K8sType,
        id: &ObjectIdRef<'_>,
        resource: &Value,
    ) -> Result<(), Error> {
        let req = request::replace_request(&self.0.config, k8s_type, id, resource)?;
        self.execute_ensure_success(req).await
    }

    pub async fn patch_resource(
        &self,
        k8s_type: &K8sType,
        id: &ObjectIdRef<'_>,
        patch: &Patch,
    ) -> Result<(), Error> {
        let req = request::patch_request(&self.0.config, k8s_type, id, patch)?;
        self.execute_ensure_success(req).await
    }

    pub async fn execute_ensure_success(&self, req: Request<Body>) -> Result<(), Error> {
        let response = self.get_response(req).await?;
        if response.status().is_success() {
            Ok(())
        } else {
            let status = response.status();
            let body = hyper::body::to_bytes(response.into_body()).await?;
            if let Ok(as_str) = std::str::from_utf8(body.as_ref()) {
                log::error!("Response status: {}, body: {}", status, as_str);
            } else {
                log::error!(
                    "Response status: {}, binary body with {} bytes",
                    status,
                    body.len()
                );
            }
            Err(Error::http(status))
        }
    }

    async fn get_response_lines_deserialized<T: DeserializeOwned>(
        &self,
        req: Request<Body>,
    ) -> Result<LineDeserializer<T>, Error> {
        let lines = self.get_response_lines(req).await?;
        Ok(LineDeserializer::<T>::new(lines))
    }

    async fn get_response_lines(&self, req: Request<Body>) -> Result<Lines, Error> {
        let resp = self.get_response(req).await?;
        if !resp.status().is_success() {
            Err(Error::http(resp.status()))
        } else {
            Ok(Lines::from_body(resp.into_body()))
        }
    }

    async fn get_response(&self, req: Request<Body>) -> Result<Response<Body>, Error> {
        let method = req.method().to_string();
        let uri = req.uri().to_string();
        let start_time = Instant::now();

        self.private_execute_request(start_time, method.as_str(), uri.as_str(), req)
            .await
    }

    async fn get_response_body<T: DeserializeOwned>(&self, req: Request<Body>) -> Result<T, Error> {
        let method = req.method().to_string();
        let uri = req.uri().to_string();
        let start_time = Instant::now();

        let response = self
            .private_execute_request(start_time, method.as_str(), uri.as_str(), req)
            .await?;

        let status_code = response.status().as_u16();
        let result = Client::read_body(response).await;
        let success = result.is_ok();
        let duration = start_time.elapsed().as_millis();
        log::debug!(
            "Finished {} request to: {}, status: {}, total_duration: {}ms, success: {}",
            method,
            uri,
            status_code,
            duration,
            success
        );
        result
    }

    async fn private_execute_request(
        &self,
        start_time: Instant,
        method: &str,
        uri: &str,
        req: Request<Body>,
    ) -> Result<Response<Body>, Error> {
        log::debug!("Starting {} request to: {}", method, uri);
        // we measure duration separately for the logs and for the prometheus metrics... should figure out an alternative
        let timer = self.0.metrics.request_started();
        let result = self.0.http_client.request(req).await;
        let duration = start_time.elapsed().as_millis();
        timer.observe_duration();
        match result {
            Ok(resp) => {
                let status_code = resp.status().as_u16();
                log::debug!(
                    "Response status received for {} to: {}, status: {}, duration: {}ms",
                    method,
                    uri,
                    status_code,
                    duration
                );
                Ok(resp)
            }
            Err(err) => {
                log::error!(
                    "Failed to execute {} request to: {}, err: {}",
                    method,
                    uri,
                    err
                );
                Err(err.into())
            }
        }
    }

    async fn read_body<T: DeserializeOwned>(response: Response<Body>) -> Result<T, Error> {
        if !response.status().is_success() {
            return Err(Error::http(response.status()));
        }

        let deserialized = if log::log_enabled!(log::Level::Trace) {
            // if we're logging the response body, then we'll need to read it all into a
            // single buffer, since we can't read a response body twice without copying
            let body = hyper::body::to_bytes(response.into_body()).await?;
            let as_str = String::from_utf8_lossy(body.as_ref());
            log::trace!("Got response body: {}", as_str);
            serde_json::from_slice(body.as_ref())?
        } else {
            // in the common case, we'll just parse the response body directly
            let body = hyper::body::aggregate(response.into_body()).await?;
            serde_json::from_reader(body.reader())?
        };
        Ok(deserialized)
    }
}

pub struct Lines {
    body: Body,
    remaining: Option<bytes::Bytes>,
    current_line: Vec<bytes::Bytes>,
}

impl Lines {
    pub fn from_body(body: Body) -> Lines {
        Lines {
            body,
            remaining: None,
            current_line: Vec::with_capacity(2),
        }
    }

    pub async fn next(&mut self) -> Option<Result<Line<'_>, Error>> {
        self.current_line.clear();

        loop {
            if let Some(mut remaining) = self.remaining.take() {
                let res = {
                    let buf = &remaining[..];
                    Lines::index_of_newline(buf)
                };
                if let Some((start, end)) = res {
                    // found a line in the currrent buffer, so we'll break off a slice
                    let mut line = remaining.split_to(end);
                    if !remaining.is_empty() {
                        self.remaining = Some(remaining);
                    }
                    if start > 0 {
                        line.truncate(start);
                        self.current_line.push(line);
                        return Some(Ok(self.make_line()));
                    }
                } else {
                    // no newlines in here, add this slice to the current line and keep looking
                    self.current_line.push(remaining);
                }
            } else {
                // fine then, we'll try to read some more from the body
                let next = self.body.next().await;
                match next {
                    Some(Ok(bytes)) => self.remaining = Some(bytes),
                    Some(Err(e)) => {
                        log::error!("Error reading response lines: {}", e);
                        return Some(Err(e.into()));
                    }
                    None => {
                        if !self.current_line.is_empty() {
                            return Some(Ok(self.make_line()));
                        } else {
                            return None;
                        }
                    }
                }
            }
        }
    }

    fn make_line(&mut self) -> Line {
        Line {
            buffer: self.current_line.as_mut_slice(),
        }
    }

    fn index_of_newline(bytes: &[u8]) -> Option<(usize, usize)> {
        NEWLINE_REGEX.find(bytes).map(|m| (m.start(), m.end()))
    }
}

pub struct Line<'a> {
    buffer: &'a mut [bytes::Bytes],
}

impl<'a> Line<'a> {
    fn is_empty(&self) -> bool {
        self.buffer.iter().map(bytes::Bytes::len).sum::<usize>() == 0usize
    }
}

impl<'a> std::io::Read for Line<'a> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.buffer.is_empty() {
            return Ok(0);
        }

        let mut bytes_written = 0;
        let mut dest = buf;

        while !self.buffer.is_empty() && !dest.is_empty() {
            let byte_count = dest.len().min(self.buffer[0].len());

            {
                let src = &(self.buffer[0])[0..byte_count];
                let tmp_dst = &mut dest[0..byte_count];
                tmp_dst.copy_from_slice(src);
                bytes_written += byte_count;
            }
            dest = &mut dest[byte_count..];
            let _ = self.buffer[0].split_to(byte_count);
            if self.buffer[0].is_empty() {
                let tmp: &mut [bytes::Bytes] = std::mem::replace(&mut self.buffer, &mut []);
                self.buffer = &mut tmp[1..];
            }
        }
        Ok(bytes_written)
    }
}

pub struct LineDeserializer<T: DeserializeOwned> {
    lines: Lines,
    _phantom: std::marker::PhantomData<T>,
}
impl<T: DeserializeOwned> LineDeserializer<T> {
    pub fn new(lines: Lines) -> Self {
        Self {
            lines,
            _phantom: std::marker::PhantomData,
        }
    }

    pub async fn next(&mut self) -> Option<Result<T, Error>> {
        loop {
            let line = self.lines.next().await?;
            match line {
                Ok(reader) if !reader.is_empty() => {
                    return Some(serde_json::from_reader(reader).map_err(Into::into))
                }
                Err(err) => return Some(Err(err)),
                _ => { /* empty line, so we'll loop again */ }
            }
        }
    }
}

#[derive(Deserialize, Serialize, Clone)]
#[serde(tag = "type", content = "object", rename_all = "UPPERCASE")]
pub enum WatchEvent {
    Added(Value),
    Modified(Value),
    Deleted(Value),
    Error(ApiError),
}

#[derive(Deserialize, Serialize, Debug, Clone, Eq, PartialEq)]
pub struct ApiError {
    pub status: String,
    #[serde(default)]
    pub message: String,
    #[serde(default)]
    pub reason: String,
    pub code: u16,
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "Api Error: status: '{}', code: {}, reason: '{}', message: '{}'",
            self.status, self.code, self.reason, self.message
        )
    }
}
impl std::error::Error for ApiError {}

#[derive(Debug, Deserialize, PartialEq, Clone)]
pub struct ListMeta {
    #[serde(rename = "resourceVersion")]
    pub resource_version: Option<String>,
}

#[derive(Debug, Deserialize, PartialEq, Clone)]
pub struct ObjectList<T> {
    pub metadata: ListMeta,
    pub items: Vec<T>,
}

#[cfg(test)]
mod test {
    use super::*;
    use bytes::Bytes;
    use futures_util::StreamExt;
    use hyper::Body;
    use std::io::Read;
    use tokio::runtime;

    #[test]
    fn lines_iterates_lines() {
        let input1 = &b"line1\nline2\r\nline3\r\n\r\n\r\n\rlong"[..];
        let input2 = &b"line4\r\r"[..];
        let input3 = &b"\r\nline5"[..];
        let stream = tokio_stream::iter(vec![input1, input2, input3]).map(|b| {
            let res: Result<Bytes, std::io::Error> = Ok(Bytes::from_static(b));
            res
        });
        let body = Body::wrap_stream(stream);
        let mut lines = Lines::from_body(body);

        let runtime = runtime::Builder::new_current_thread().build().unwrap();

        let expected = ["line1", "line2", "line3", "longline4", "line5"];

        runtime.block_on(async move {
            for expected_line in expected.iter() {
                let mut line = lines
                    .next()
                    .await
                    .expect("line returned none")
                    .expect("line returned error");
                let mut string = String::new();
                line.read_to_string(&mut string)
                    .expect("failed to read to string");
                assert_eq!(*expected_line, string.as_str());
                assert!(line.is_empty());
            }
        });
    }
}

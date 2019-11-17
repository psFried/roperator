use crate::config::{ClientConfig, K8sType};
use crate::runner::request;
use crate::resource::ObjectIdRef;

use http::{Request, Response, header};
use hyper::client::Client as HyperClient;
use hyper::client::HttpConnector;
use hyper::Body;
use hyper_openssl::HttpsConnector;
use openssl::ssl::{SslConnector, SslMethod};
use futures_util::TryStreamExt;
use serde::{Serialize, Deserialize};
use serde::de::DeserializeOwned;
use serde_json::Value;
use futures::stream::Stream;
use regex::bytes::Regex;
use lazy_static::lazy_static;

use std::time::{Instant, Duration};
use std::sync::Arc;
use std::collections::VecDeque;


lazy_static!{
    static ref NEWLINE_REGEX: Regex = Regex::new("([\\r\\n]+)").unwrap();
}

#[derive(Debug)]
pub enum Error {
    Io(hyper::error::Error),
    Serde(serde_json::Error),
    Http(http::StatusCode),
}

impl std::error::Error for Error { }

impl Error {
    pub fn http(status: http::StatusCode) -> Error {
        Error::Http(status)
    }

    pub fn is_http_410(&self) -> bool {
        match self {
            &Error::Http(ref status) => status.as_u16() == 410,
            _ => false
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

impl From<hyper::error::Error> for Error {
    fn from(e: hyper::error::Error) -> Error {
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
    auth_header_value: header::HeaderValue,
}

#[derive(Debug, Clone)]
pub struct Client(Arc<ClientInner>);


impl Client {

    pub fn new(config: ClientConfig) -> Result<Client, failure::Error> {
        let mut http = HttpConnector::new();
        http.enforce_http(false);

        let mut ssl = SslConnector::builder(SslMethod::tls())?;
        // enable http2 using alpn
        ssl.set_alpn_protos(b"\x02h2\x08http/1.1")?;
        if let Some(ca_path) = config.ca_file_path.as_ref() {
            ssl.set_ca_file(ca_path.as_str())?;
        }

        let https = HttpsConnector::with_connector(http, ssl)?;

        let client = HyperClient::builder().build(https);

        let bearer_auth = format!("Bearer {}", config.service_account_token);
        let auth_header_value = header::HeaderValue::from_str(bearer_auth.as_str())?;

        let inner = ClientInner {
            http_client: client,
            config,
            auth_header_value,
        };
        Ok(Client(Arc::new(inner)))
    }

    pub async fn delete_resource(&self, k8s_type: &K8sType, id: &ObjectIdRef<'_>) -> Result<(), Error> {
        log::info!("Deleting resouce '{}' with type: {}", id, k8s_type);
        let req = request::delete_request(&self.0.config, k8s_type, id)?;
        self.execute_ensure_success(req).await
    }

    pub async fn create_resource(&self, k8s_type: &K8sType, resource: &Value) -> Result<(), Error> {
        let req = request::create_request(&self.0.config, k8s_type, resource)?;
        self.execute_ensure_success(req).await
    }

    pub async fn replace_resource(&self, k8s_type: &K8sType, id: &ObjectIdRef<'_>, resource: &Value) -> Result<(), Error> {
        let req = request::replace_request(&self.0.config, k8s_type, id, resource)?;
        self.execute_ensure_success(req).await
    }

    pub async fn execute_ensure_success(&self, req: Request<Body>) -> Result<(), Error> {
        let response = self.get_response(req).await?;
        if response.status().is_success() {
            Ok(())
        } else {
            Err(Error::http(response.status().clone()))
        }
    }

    pub async fn get_response_lines_deserialized<T: DeserializeOwned>(&self, req: Request<Body>) -> Result<LineDeserializer<T>, Error> {
        let lines = self.get_response_lines(req).await?;
        Ok(LineDeserializer::<T>::new(lines))
    }

    pub async fn get_response_lines(&self, req: Request<Body>) -> Result<Lines, Error> {
        let resp = self.get_response(req).await?;
        if !resp.status().is_success() {
            Err(Error::http(resp.status().clone()))
        } else {
            Ok(Lines::from_body(resp.into_body()))
        }
    }

    pub async fn get_response(&self, mut req: Request<Body>) -> Result<Response<Body>, Error> {
        let method = req.method().to_string();
        let uri = req.uri().to_string();
        let start_time = Instant::now();

        self.private_execute_request(start_time, method.as_str(), uri.as_str(), req).await
    }

    pub async fn get_response_body<T: DeserializeOwned>(&self, mut req: Request<Body>) -> Result<T, Error> {
        let method = req.method().to_string();
        let uri = req.uri().to_string();
        let start_time = Instant::now();

        let response = self.private_execute_request(start_time, method.as_str(), uri.as_str(), req).await?;

        let status_code = response.status().as_u16();
        let result = Client::read_body(response).await;
        let success = result.is_ok();
        let duration = start_time.elapsed().as_millis();
        log::debug!("Finished {} request to: {}, status: {}, total_duration: {}ms, success: {}", method, uri, status_code, duration, success);
        result
    }

    async fn private_execute_request(&self, start_time: Instant, method: &str, uri: &str, mut req: Request<Body>) -> Result<Response<Body>, Error> {
        log::debug!("Starting {} request to: {}", method, uri);

        {
            let headers = req.headers_mut();
            headers.insert(header::AUTHORIZATION, self.0.auth_header_value.clone());
        }

        let result = self.0.http_client.request(req).await;
        let duration = start_time.elapsed().as_millis();
        match result {
            Ok(resp) => {
                let status_code = resp.status().as_u16();
                log::debug!("Response status received for {} to: {}, status: {}, duration: {}ms", method, uri, status_code, duration);
                Ok(resp)
            }
            Err(err) => {
                log::error!("Failed to execute {} request to: {}, err: {}", method, uri, err);
                Err(err.into())
            }
        }
    }

    async fn read_body<T: DeserializeOwned>(response: Response<Body>) -> Result<T, Error> {
        if !response.status().is_success() {
            return Err(Error::http(response.status().clone()));
        }
        let body = response.into_body().try_concat().await?;
        let deserialized = serde_json::from_slice(&body)?;
        Ok(deserialized)
    }
}

// pub struct MultiValueResponse<T> {
//     body: Body,
//     _phantom: std::marker::PhantomData<T>,
// }

// impl <T: DeserializeOwned> MultiValueResponse<T> {

//     pub async fn next(&mut self) -> Option<Result<T, Error>> {

//     }
// }

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

    pub async fn next<'a>(&'a mut self) -> Option<Result<Line<'a>, Error>> {
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
                    Some(Ok(bytes)) => self.remaining = Some(bytes.into_bytes()),
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

    fn make_line<'a>(&'a mut self) -> Line<'a> {
        Line {
            buffer: self.current_line.as_mut_slice(),
        }
    }

    fn index_of_newline(bytes: &[u8]) -> Option<(usize, usize)> {
        NEWLINE_REGEX.find(bytes).map(|m| {
            (m.start(), m.end())
        })
    }
}

pub struct Line<'a> {
    buffer: &'a mut [bytes::Bytes],
}

impl <'a> Line<'a> {
    pub fn is_empty(&self) -> bool {
        self.buffer.iter().map(bytes::Bytes::len).sum::<usize>() == 0usize
    }

    fn pop_first_element(&mut self) {
        let tmp: &mut [bytes::Bytes] = std::mem::replace(&mut self.buffer, &mut []);
        self.buffer = &mut tmp[1..];
    }
}


impl <'a> std::io::Read for Line<'a> {
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
impl <T: DeserializeOwned> LineDeserializer<T> {

    pub fn new(lines: Lines) -> Self {
        Self {
            lines,
            _phantom: std::marker::PhantomData,
        }
    }

    pub async fn next(&mut self) -> Option<Result<T, Error>> {
        let line = self.lines.next().await?;
        match line {
            Ok(reader) => Some(serde_json::from_reader(reader).map_err(Into::into)),
            Err(err) => Some(Err(err))
        }
    }
}


#[cfg(test)]
mod test {
    use super::*;
    use hyper::Body;
    use tokio::runtime::current_thread::Runtime;
    use std::collections::VecDeque;
    use std::io::Read;

    #[test]
    fn lines_iterates_lines() {

        let input1 = &b"line1\nline2\r\nline3\r\n\r\n\r\n\rlong"[..];
        let input2 = &b"line4\r\r"[..];
        let input3 = &b"\r\nline5"[..];
        let stream: VecDeque<Result<&[u8], String>> = VecDeque::from(vec![Ok(input1), Ok(input2), Ok(input3)]);
        let body = Body::wrap_stream(stream);
        let mut lines = Lines::from_body(body);

        let mut runtime = Runtime::new().unwrap();

        let expected = [
            "line1",
            "line2",
            "line3",
            "longline4",
            "line5"
        ];

        runtime.block_on(async move {

            for i in 0usize..5usize {
                let mut line = lines.next().await.expect("line returned none").expect("line returned error");
                let mut string = String::new();
                line.read_to_string(&mut string).expect("failed to read to string");
                assert_eq!(expected[i], string.as_str());
                assert!(line.is_empty());
            }
        });
    }

}

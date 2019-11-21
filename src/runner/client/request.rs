use crate::config::{K8sType, ClientConfig};
use crate::runner::client::Error;
use crate::resource::{ObjectIdRef, K8sResource};

use url::Url;
use hyper::Body;
use http::{Request, header, Method};
use serde_json::Value;

#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum MergeStrategy {
    Json,
    JsonMerge,
    StrategicMerge,
}

impl MergeStrategy {
    fn content_type(&self) -> &'static str {
        match *self {
            MergeStrategy::Json => "application/json-patch+json",
            MergeStrategy::JsonMerge => "application/json-merge+json",
            MergeStrategy::StrategicMerge => "application/strategic-merge-patch+json",
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct Patch {
    merge_strategy: MergeStrategy,
    value: Value,
}

impl Patch {
    pub fn remove_finalizer(resource: &K8sResource, finalizer: &str) -> Patch {
        let finalizers = resource.as_ref().pointer("/metadata/finalizers")
                .and_then(Value::as_array)
                .map(|finalizers| finalizers.iter().filter(|f| f.as_str() != Some(finalizer)).collect::<Vec<_>>())
                .unwrap_or(Vec::new());
        let patch = serde_json::json!({
            "metadata": {
                "namespace": resource.get_object_id().namespace(),
                "name": resource.get_object_id().name(),
                "resourceVersion": resource.get_resource_version(),
                "finalizers": finalizers,
            }
        });
        Patch {
            value: patch,
            merge_strategy: MergeStrategy::JsonMerge,
        }
    }
}

pub fn patch_request(client_config: &ClientConfig, k8s_type: &K8sType, id: &ObjectIdRef<'_>, patch: &Patch) -> Result<Request<Body>, Error> {
    let url = make_url(client_config, k8s_type, id.namespace(), Some(id.name()));
    let mut builder = make_req(url, Method::PATCH, client_config);
    let header_value = patch.merge_strategy.content_type();
    builder.header(header::CONTENT_TYPE, header_value);
    let body = serde_json::to_vec(&patch.value)?;
    let req = builder.body(Body::from(body)).unwrap();
    Ok(req)
}

pub fn create_request(client_config: &ClientConfig, k8s_type: &K8sType, resource: &Value) -> Result<Request<Body>, Error> {
    let url = make_url(client_config, k8s_type, get_namespace(resource), None);

    let mut builder = make_req(url, Method::POST, client_config);
    let as_vec = serde_json::to_vec(resource)?;
    let req = builder.body(Body::from(as_vec)).unwrap();
    Ok(req)
}

pub fn replace_request(client_config: &ClientConfig, k8s_type: &K8sType, id: &ObjectIdRef<'_>, resource: &Value) -> Result<Request<Body>, Error> {
    let url = make_url(client_config, k8s_type, id.namespace(), Some(id.name()));
    let as_vec = serde_json::to_vec(resource)?;
    let req = make_req(url, Method::PUT, client_config).body(Body::from(as_vec)).unwrap();
    Ok(req)
}

pub fn update_status_request(client_config: &ClientConfig, k8s_type: &K8sType, id: &ObjectIdRef<'_>, new_status: &Value) -> Result<Request<Body>, Error> {

    let mut url = make_url(client_config, k8s_type, id.namespace(), Some(id.name()));
    {
        let mut path = url.path_segments_mut().unwrap();
        path.push("status");
    }
    let as_vec = serde_json::to_vec(new_status)?;
    let req = Request::put(url.into_string())
            .header(header::AUTHORIZATION, client_config.service_account_token.as_str())
            .body(Body::from(as_vec))
            .unwrap();
    Ok(req)
}

pub fn delete_request(client_config: &ClientConfig, k8s_type: &K8sType, id: &ObjectIdRef<'_>) -> Result<Request<Body>, Error> {
    let url = make_url(client_config, k8s_type, id.namespace(), Some(id.name()));
    let req = Request::delete(url.into_string())
            .header(header::AUTHORIZATION, client_config.service_account_token.as_str())
            .body(Body::empty())
            .unwrap();
    Ok(req)
}

pub fn watch_request(client_config: &ClientConfig, k8s_type: &K8sType, resource_version: Option<&str>, label_selector: Option<&str>, timeout_seconds: Option<u32>, namespace: Option<&str>) -> Result<Request<Body>, Error> {
    let mut url = make_url(client_config, k8s_type, namespace, None);
    {
        let mut query = url.query_pairs_mut();
        query.append_pair("watch", "true");
        if let Some(vers) = resource_version {
            query.append_pair("resourceVersion", vers);
        }
        if let Some(selector) = label_selector {
            query.append_pair("labelSelector", selector);
        }
        if let Some(timeout) = timeout_seconds {
            let as_str = format!("{}", timeout);
            query.append_pair("timeoutSeconds", &as_str);
        }
    }

    let req = Request::get(url.into_string())
            .header(header::AUTHORIZATION, client_config.service_account_token.as_str())
            .body(Body::empty())
            .unwrap();
    Ok(req)
}

pub fn list_request(client_config: &ClientConfig, k8s_type: &K8sType, label_selector: Option<&str>, namespace: Option<&str>) -> Result<Request<Body>, Error> {
    let mut url = make_url(client_config, k8s_type, namespace, None);
    if let Some(selector) = label_selector {
        let mut query = url.query_pairs_mut();
        query.append_pair("labelSelector", selector);
    }
    let req = Request::get(url.into_string())
            .header(header::AUTHORIZATION, client_config.service_account_token.as_str())
            .body(Body::empty())
            .unwrap();
    Ok(req)
}

fn make_req(url: Url, method: http::Method, client_config: &ClientConfig) -> http::request::Builder {
    let mut builder = Request::builder();
    builder.method(method)
            .uri(url.into_string())
            .header(header::AUTHORIZATION, client_config.service_account_token.as_str())
            .header(header::USER_AGENT, client_config.user_agent.as_str());
    builder
}

fn get_namespace(resource: &Value) -> Option<&str> {
    resource.pointer("/metadata/namespace").and_then(Value::as_str)
}

fn make_url(client_config: &ClientConfig, k8s_type: &K8sType, namespace: Option<&str>, name: Option<&str>) -> Url {
    let mut url = url::Url::parse(client_config.api_server_endpoint.as_str()).unwrap();
    {
        let mut segments = url.path_segments_mut().unwrap();

        let prefix = if k8s_type.group.len() > 0 {
            "apis"
        } else {
            "api"
        };
        segments.push(prefix);
        if !k8s_type.group.is_empty() {
            segments.push(k8s_type.group.as_str());
        }
        segments.push(k8s_type.version.as_str());
        if let Some(ns) = namespace {
            segments.push("namespaces");
            segments.push(ns);
        }
        segments.push(k8s_type.plural_kind.as_str());

        if let Some(n) = name {
            segments.push(n);
        }
    }
    url
}

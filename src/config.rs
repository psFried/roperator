//! Types for creating `OperatorConfig` and `ClientConfig`.
//! Most users will use `OperatorConfig::new()`.
//! `ClientConfig` can be created automatically in most cases, but you can also create that manually.
mod kubeconfig;

use crate::k8s_types::K8sType;

use std::collections::HashMap;
use std::io;
use std::{path::Path, time::Duration};

/// Default label that's added to all child resources, so that roperator can track the ownership of resources.
/// The value is the `metadata.uid` of the parent.
pub const DEFAULT_TRACKING_LABEL_NAME: &str = "app.kubernetes.io/instance";

/// Default label that's added to all child resources to identify this operator as the manager.
/// The value will always be the `operator_name` from the `OperatorConfig`.
pub const DEFAULT_OWNERSHIP_LABEL_NAME: &str = "app.kubernetes.io/managed-by";

const SERVICE_ACCOUNT_TOKEN_PATH: &str = "/var/run/secrets/kubernetes.io/serviceaccount/token";
const SERVICE_ACCOUNT_CA_PATH: &str = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt";
const API_SERVER_HOSTNAME: &str = "kubernetes.default.svc";

pub use self::kubeconfig::{KubeConfig, KubeConfigError};

/// What to do when there's a difference between the "desired" state of a given resource and the
/// actual state of that resource in the cluster. The three options are:
/// - Update the resource in place using an HTTP PUT request
/// - First delete the resource, then try to re-create it later
/// - Don't update it automatically, and instead wait for something else to delete the resource and then re-create it with the new state
///
/// There's not yet any option for using PATCH requests.
#[derive(Debug, Copy, Clone, PartialEq)]
pub enum UpdateStrategy {
    /// Means that the resource will be updated in place using an HTTP PUT request
    Replace,

    /// The resource will first get deleted, and then re-created with the new state
    Recreate,

    /// The resource will not be modified in any way. Instead, we'll wait until the resource is deleted by some other means and then re-create it
    OnDelete,
}

/// Configuration object that's specific to each type of child
#[derive(Debug, Clone, PartialEq)]
pub struct ChildConfig {
    /// The update strategy for this child type, which determines what roperator should do when a
    /// desired from a `SyncResponse` doesn't match the actual state of the cluster.
    pub update_strategy: UpdateStrategy,
}

impl ChildConfig {
    pub fn new(update_strategy: UpdateStrategy) -> ChildConfig {
        ChildConfig { update_strategy }
    }

    /// returns a `ChildConfig` with the `update_strategy` set to `UpdateStrategy::Recreate`
    pub fn recreate() -> ChildConfig {
        ChildConfig::new(UpdateStrategy::Recreate)
    }

    /// returns a `ChildConfig` with the `update_strategy` set to `UpdateStrategy::Replace`
    pub fn replace() -> ChildConfig {
        ChildConfig::new(UpdateStrategy::Replace)
    }

    /// returns a `ChildConfig` with the `update_strategy` set to `UpdateStrategy::OnDelete`
    pub fn on_delete() -> ChildConfig {
        ChildConfig::new(UpdateStrategy::OnDelete)
    }
}

/// This is the main configuration of your operator. It is where you'll specify the type of your
/// parent and child resources, among other things. `OperatorConfig::new()` returns sensible
/// defaults for everything except for the child types.
#[derive(Debug, Clone, PartialEq)]
pub struct OperatorConfig {
    /// The type of the parent resource. This should match the type information from the CRD
    pub parent: &'static K8sType,
    /// The type of each child resource that the operator will deal with.
    pub child_types: HashMap<&'static K8sType, ChildConfig>,
    /// Optional namespace to constrain the operator to. If None, then the operator will monitor
    /// and act on any instance of the parent resource in any namespace. If Some, then the operator
    /// will only ever watch and modify resources in the given namespace.
    pub namespace: Option<String>,

    /// The name of the operator, which must consist of only ascii alphabetic characters and numerals.
    /// This value will be used to add a label to every child resource being managed by this operator,
    /// which will have the `operator_name` as its value.
    pub operator_name: String,
    /// The name of the label to use for tracking the relationships between parent and child resources.
    /// Roperator will add the `metadata.uid` of the parent resource to the labels of each child resource it manages.
    pub tracking_label_name: String,

    /// The label to use for marking the `operator_name`. Defaults to `"kubernetes.io/managed-by"`
    pub ownership_label_name: String,

    /// The HTTP port to listen on for exposing health checks and metrics. No server will be started
    /// if both `expose_metrics` and `expose_health` are `false`
    pub server_port: u16,

    //// If true, then prometheus metrics will be exposed by HTTP at `/metrics`. This is enabled by default
    /// when you use `OperatorConfig::new()`
    pub expose_metrics: bool,

    //// If true, then a health check will be exposed by HTTP at `/health`. This is enabled by default
    /// when you use `OperatorConfig::new()`
    pub expose_health: bool,

    //// This is used to space out the time between `Handler::sync()` calls on the same parent resource in a uniform way. If `None`, no exponential backoff is performed.
    /// maximum period between requested resyncs
    pub resync_period: Option<Duration>,
}

impl OperatorConfig {
    pub fn new(operator_name: impl Into<String>, parent: &'static K8sType) -> OperatorConfig {
        let operator_name = operator_name.into();
        OperatorConfig {
            parent,
            operator_name,
            child_types: HashMap::new(),
            namespace: None,
            tracking_label_name: DEFAULT_TRACKING_LABEL_NAME.to_owned(),
            ownership_label_name: DEFAULT_OWNERSHIP_LABEL_NAME.to_owned(),
            server_port: 8080,
            expose_metrics: true,
            expose_health: true,
            resync_period: Some(Duration::from_secs(60)),
        }
    }

    /// Set the namespace for this operator. If set, then the operator will only ever watch or manage
    /// resources within the given namespace
    pub fn within_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    /// Adds a new child type to this configuration. Every type of resource that the operator may manage
    /// must be included in the `OperatorConfig`.
    pub fn with_child(mut self, child_type: &'static K8sType, config: ChildConfig) -> Self {
        self.child_types.insert(child_type, config);
        self
    }

    /// Sets whether to expose a health check HTTP endpoint
    pub fn expose_health(mut self, expose_health: bool) -> Self {
        self.expose_health = expose_health;
        self
    }

    /// Sets whether to expose prometheus metrics over HTTP
    pub fn expose_metrics(mut self, expose_metrics: bool) -> Self {
        self.expose_metrics = expose_metrics;
        self
    }

    /// Sets the port to listen on for HTTP. This will be ignored if both `expose_metrics` and `expose_health` are `false`
    pub fn server_port(mut self, port: u16) -> Self {
        self.server_port = port;
        self
    }

    pub fn resync_period(mut self, period: Option<Duration>) -> Self {
        self.resync_period = period;
        self
    }
}

/// Certificate Authority data for verifying Kubernetes TLS certificates. This typically comes from either a
/// mounted service account Secret at `SERVICE_ACCOUNT_CA_PATH`, or else a "cluster" entry in a kubeconfig file.
#[derive(Debug, Clone, PartialEq)]
pub enum CAData {
    /// The path to a file on disk that contains the CA certificate
    File(String),
    /// The base64 encoded certificate from a kubeconfig file. This value is already base64 encoded in the
    /// kubeconfig file, so you don't need to modify that value at all.
    Contents(String),
}

/// Represents how to authenticate to the cluster. Roperator supports either using an Authorization header
/// or a public/private key pair. The `Header` value can support either username/password or token based
/// authentication.
#[derive(Debug, Clone, PartialEq)]
pub enum Credentials {
    /// Represents the value to set for an Authorization header. This value must include the prefix (e.g. `Basic `
    /// or `Bearer `) as well as the properly encoded and formatted value.
    Header(String),

    /// Values for authenticating using a certificate. This is frequently used in kubeconfig files
    Pem {
        /// The public certificate, which will be presented to the server during the TLS handshake. This value
        /// is already base64 encoded in the kubeconfig file, so if you take if from there, then no further
        /// modification is necessary
        certificate_base64: String,
        /// The private key that corresponds to the certificate above
        private_key_base64: String,
    },

    PemPath {
        certificate_path: String,
        private_key_path: String,
    },
}

impl Credentials {
    /// Creates a `Credentials` from a raw (_not_ base64 encoded) token
    pub fn raw_bearer_token(raw_token: impl AsRef<str>) -> Credentials {
        let encoded = base64::encode(raw_token.as_ref());
        Credentials::Header(format!("Bearer {}", encoded))
    }

    /// Creates a `Credentials` from a token that is already base64 encoded
    pub fn base64_bearer_token(base64_token: impl AsRef<str>) -> Credentials {
        Credentials::Header(format!("Bearer {}", base64_token.as_ref()))
    }

    /// Creates a `Credentials` from a raw (_not_ base64 encoded) username and password
    pub fn basic(raw_username: impl AsRef<str>, raw_password: impl AsRef<str>) -> Credentials {
        let formatted = format!("{}:{}", raw_username.as_ref(), raw_password.as_ref());
        let encoded = base64::encode(formatted.as_str());
        Credentials::Header(format!("Basic {}", encoded))
    }
}

/// Configuration for how to connect to the Kubernetes API server and authenticate. This configuration
/// can typically be created from either a service account or a kubeconfig file using one of the provided
/// functions, but you may also create configurations manually.
#[derive(Debug, Clone, PartialEq)]
pub struct ClientConfig {
    /// The http(s) endpoint of the api server, including the scheme and port
    pub api_server_endpoint: String,
    /// The credentials for authenticating with the api server
    pub credentials: Credentials,
    // Certificate for the Certificate Authority that signed the Kubernetes API server certificate
    pub ca_data: Option<CAData>,
    /// The user-agent string to include in requests to the api server. This typically doesn't affect
    /// the fuctioning of the operator, but it can be useful when looking through api server logs
    pub user_agent: String,
    /// Escape hatch for turning off ssl certificate validation **in test environments only**. Don't
    /// set to `true` in production. Don't be _that_ person.
    pub verify_ssl_certs: bool,
    /// Optional user to impersonate
    pub impersonate: Option<String>,
    /// optional list of groups to add when impersonating a user. Ignored if `impersonate` is empty.
    pub impersonate_groups: Vec<String>,
}

impl ClientConfig {
    /// Attempts to build a `ClientConfig` from a service account that's been mounted in the usual path
    /// (`SERVICE_ACCOUNT_TOKEN_PATH`). Returns an error if either the "token" or "ca.crt" files are
    /// missing.
    ///
    /// The returned `ClientConfig` will use the default `api_server_endpoint` of `"kubernetes.default.svc"`,
    /// so you'll need to change that if your cluster uses something different.
    ///
    /// The `user_agent` is typically the same value as the `operator_name` from the `OperatorConfig`.
    pub fn from_service_account(user_agent: impl Into<String>) -> Result<ClientConfig, io::Error> {
        use std::fs::File;
        use std::io::Read;

        let mut token_file = File::open(SERVICE_ACCOUNT_TOKEN_PATH)?;
        let mut service_account_token = String::new();
        token_file.read_to_string(&mut service_account_token)?;

        let ca_file_path = Path::new(SERVICE_ACCOUNT_CA_PATH);
        let ca_data = if ca_file_path.exists() {
            Some(CAData::File(SERVICE_ACCOUNT_CA_PATH.to_owned()))
        } else {
            None
        };

        let api_server_endpoint = format!("https://{}", API_SERVER_HOSTNAME);
        Ok(ClientConfig {
            api_server_endpoint,
            ca_data,
            credentials: Credentials::Header(format!("Bearer {}", service_account_token)),
            user_agent: user_agent.into(),
            verify_ssl_certs: true,
            impersonate: None,
            impersonate_groups: Vec::new(),
        })
    }

    /// Attempts to build a `ClientConfig` from a kubeconfig file. This respects the value of the `KUBECONFIG`
    /// environment variable. Most, but not all, of the authentication methods are supported, including
    /// certificates, username/password, token, and exec. Roperator does not currently support rotation of
    /// credentials, so any credentials taken from the kubeconfig will need to be valid for the lifetime of
    /// the application.
    pub fn from_kubeconfig(user_agent: impl Into<String>) -> Result<ClientConfig, KubeConfigError> {
        self::kubeconfig::load_from_kubeconfig(user_agent.into())
    }
}

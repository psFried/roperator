mod kubeconfig;

use crate::k8s_types::K8sType;

use std::collections::HashMap;
use std::io;
use std::path::Path;

pub const DEFAULT_TRACKING_LABEL_NAME: &str = "app.kubernetes.io/instance";
pub const DEFAULT_OWNERSHIP_LABEL_NAME: &str = "app.kubernetes.io/managed-by";

const SERVICE_ACCOUNT_TOKEN_PATH: &str = "/var/run/secrets/kubernetes.io/serviceaccount/token";
const SERVICE_ACCOUNT_CA_PATH: &str = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt";
const API_SERVER_HOSTNAME: &str = "kubernetes.default.svc";

pub use self::kubeconfig::KubeConfigError;

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum UpdateStrategy {
    Recreate,
    Replace,
    OnDelete,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ChildConfig {
    pub update_strategy: UpdateStrategy,
}

impl ChildConfig {
    pub fn new(update_strategy: UpdateStrategy) -> ChildConfig {
        ChildConfig { update_strategy }
    }

    pub fn recreate() -> ChildConfig {
        ChildConfig::new(UpdateStrategy::Recreate)
    }
    pub fn replace() -> ChildConfig {
        ChildConfig::new(UpdateStrategy::Replace)
    }
    pub fn on_delete() -> ChildConfig {
        ChildConfig::new(UpdateStrategy::OnDelete)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct OperatorConfig {
    pub parent: &'static K8sType,
    pub child_types: HashMap<&'static K8sType, ChildConfig>,
    pub namespace: Option<String>,
    pub operator_name: String,
    pub tracking_label_name: String,
    pub ownership_label_name: String,
    pub server_port: u16,
    pub expose_metrics: bool,
    pub expose_health: bool,
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
        }
    }

    pub fn within_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    pub fn with_child(mut self, child_type: &'static K8sType, config: ChildConfig) -> Self {
        self.child_types.insert(child_type, config);
        self
    }

    pub fn expose_health(mut self, expose_health: bool) -> Self {
        self.expose_health = expose_health;
        self
    }

    pub fn expose_metrics(mut self, expose_metrics: bool) -> Self {
        self.expose_metrics = expose_metrics;
        self
    }

    pub fn server_port(mut self, port: u16) -> Self {
        self.server_port = port;
        self
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum CAData {
    File(String),
    Contents(String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Credentials {
    Header(String),
    Pem {
        certificate_base64: String,
        private_key_base64: String,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct ClientConfig {
    pub api_server_endpoint: String,
    pub credentials: Credentials,
    pub ca_data: Option<CAData>,
    pub user_agent: String,
    pub verify_ssl_certs: bool,
    pub impersonate: Option<String>,
    pub impersonate_groups: Vec<String>,
}

impl ClientConfig {
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

    pub fn from_kubeconfig(user_agent: impl Into<String>) -> Result<ClientConfig, KubeConfigError> {
        self::kubeconfig::load_from_kubeconfig(user_agent.into())
    }
}

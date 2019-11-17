use crate::resource::K8sTypeRef;

use failure::Error;

use std::fmt::{self, Display};
use std::collections::HashMap;

pub const DEFAULT_TRACKING_LABEL_NAME: &str = "app.kubernetes.io/instance";
pub const DEFAULT_OWNERSHIP_LABEL_NAME: &str = "app.kubernetes.io/managed-by";

const SERVICE_ACCOUNT_TOKEN_PATH: &str = "/var/run/secrets/kubernetes.io/serviceaccount/token";
const SERVICE_ACCOUNT_CA_PATH: &str = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt";
const API_SERVER_HOSTNAME: &str = "kubernetes.default.svc";

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
        ChildConfig {
            update_strategy,
        }
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
    pub parent: K8sType,
    pub child_types: HashMap<K8sType, ChildConfig>,
    pub namespace: Option<String>,
    pub operator_name: String,
    pub tracking_label_name: String,
    pub ownership_label_name: String,
}

impl OperatorConfig {
    pub fn new(operator_name: impl Into<String>, parent: K8sType) -> OperatorConfig {
        let operator_name = operator_name.into();
        OperatorConfig {
            parent,
            operator_name,
            child_types: HashMap::new(),
            namespace: None,
            tracking_label_name: DEFAULT_TRACKING_LABEL_NAME.to_owned(),
            ownership_label_name: DEFAULT_OWNERSHIP_LABEL_NAME.to_owned(),
        }
    }

    pub fn within_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    pub fn with_child(mut self, child_type: K8sType, config: ChildConfig) -> Self {
        self.child_types.insert(child_type, config);
        self
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ClientConfig {
    pub api_server_endpoint: String,
    pub service_account_token: String,
    pub ca_file_path: Option<String>,
}

impl ClientConfig {
    pub fn from_service_account() -> Result<ClientConfig, Error> {
        use std::io::Read;
        use std::fs::File;

        let mut token_file = File::open(SERVICE_ACCOUNT_TOKEN_PATH)?;
        let mut service_account_token = String::new();
        token_file.read_to_string(&mut service_account_token)?;

        let ca_file_path = if std::path::Path::new(SERVICE_ACCOUNT_CA_PATH).exists() {
            Some(SERVICE_ACCOUNT_CA_PATH.to_owned())
        } else {
            None
        };

        let api_server_endpoint = format!("https://{}", API_SERVER_HOSTNAME);
        Ok(ClientConfig {
            api_server_endpoint,
            service_account_token,
            ca_file_path,
        })
    }
}


#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct K8sType {
    pub group: String,
    pub version: String,
    pub plural_kind: String,
    pub kind: String,
}

impl K8sType {
    pub fn new(group: &str, version: &str, kind: &str, plural_kind: &str) -> K8sType {
        K8sType {
            group: group.to_owned(),
            version: version.to_owned(),
            kind: kind.to_owned(),
            plural_kind: plural_kind.to_owned(),
        }
    }

    pub fn to_type_ref(&self) -> K8sTypeRef<'static> {
        K8sTypeRef::new(self.format_api_version(), self.kind.clone())
    }

    pub fn format_api_version(&self) -> String {
        if self.group.is_empty() {
            self.version.clone()
        } else {
            format!("{}/{}", self.group, self.version)
        }
    }


    pub fn pod() -> K8sType {
        K8sType {
            group: String::new(),
            version: v1(),
            plural_kind: "pods".to_owned(),
            kind: "Pod".to_owned(),
        }
    }

    pub fn service() -> K8sType {
        K8sType {
            group: String::new(),
            version: v1(),
            plural_kind: "services".to_owned(),
            kind: "Service".to_owned(),
        }
    }
}

impl Display for K8sType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.group.is_empty() {
            write!(f, "{}/{}", self.version, self.plural_kind)
        } else {
            write!(f, "{}/{}/{}", self.group, self.version, self.plural_kind)
        }
    }
}

fn v1() -> String {
    "v1".to_owned()
}

use crate::resource::K8sTypeRef;

use std::fmt::{self, Display};
use std::collections::HashMap;

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

#[derive(Debug, Clone, PartialEq)]
pub struct OperatorConfig {
    pub parent: K8sType,
    pub child_types: HashMap<K8sType, ChildConfig>,
    pub namespace: Option<String>,
    pub operator_name: String,
    pub tracking_label_name: String,
    pub ownership_label_name: String,
}


#[derive(Debug, Clone, PartialEq)]
pub struct ClientConfig {
    pub api_server_endpoint: String,
    pub service_account_token: String,
    pub ca_file_path: Option<String>,
}


#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct K8sType {
    pub group: String,
    pub version: String,
    pub plural_kind: String,
    pub kind: String,
}

impl K8sType {

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

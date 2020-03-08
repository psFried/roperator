use serde_json::{Value};
use crate::resource::{K8sTypeRef, ObjectIdRef};

pub static API_VERSION_POINTER: &str = "/apiVersion";
pub static KIND_POINTER: &str = "/kind";
pub static NAMESPACE_POINTER: &str = "/metadata/namespace";
pub static NAME_POINTER: &str = "/metadata/name";

pub trait ResourceJson: std::fmt::Display {
    fn get_api_version(&self) -> Option<&str>;
    fn get_kind(&self) -> Option<&str>;
    fn get_namespace(&self) -> Option<&str>;
    fn get_name(&self) -> Option<&str>;

    fn get_type_ref(&self) -> Option<K8sTypeRef> {
        let api_version = self.get_api_version()?;
        let kind = self.get_kind()?;
        Some(K8sTypeRef::new(api_version, kind))
    }

    fn get_id_ref(&self) -> Option<ObjectIdRef> {
        let namespace = self.get_namespace().unwrap_or("");
        let name = self.get_name()?;
        Some(ObjectIdRef::new(namespace, name))
    }
}

fn str_value<'a, 'b>(value: &'a Value, pointer: &'b str) -> Option<&'a str> {
    value.pointer(pointer).and_then(Value::as_str)
}

impl ResourceJson for Value {
    fn get_api_version(&self) -> Option<&str> {
        str_value(self, API_VERSION_POINTER)
    }

    fn get_kind(&self) -> Option<&str> {
        str_value(self, KIND_POINTER)
    }

    fn get_namespace(&self) -> Option<&str> {
        str_value(self, NAMESPACE_POINTER)
    }

    fn get_name(&self) -> Option<&str> {
        str_value(self, NAME_POINTER)
    }
}

pub(crate) mod object_id;
mod json_ext;

use crate::k8s_types::K8sType;

use serde_json::Value;

use std::fmt::{self, Debug};

pub use self::json_ext::ResourceJson;
pub use self::object_id::{ObjectId, ObjectIdRef};

pub type JsonObject = serde_json::Map<String, Value>;

#[derive(PartialEq, Clone)]
pub struct InvalidResourceError {
    pub message: &'static str,
    pub value: Value,
}

impl InvalidResourceError {
    pub fn new(message: &'static str, value: Value) -> Self {
        InvalidResourceError { message, value }
    }
}

impl Debug for InvalidResourceError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "InvalidResourceError('{}', {})",
            self.message, self.value
        )
    }
}

impl fmt::Display for InvalidResourceError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Invalid Resource: {}", self.message)
    }
}

impl std::error::Error for InvalidResourceError {}

/// A `K8sResource` represents a Kubernetes resource that actually exists in the cluster.
/// It is a transparent wrapper around a `serde_json::Value`, with some validation and some
/// accessors for common properties. This type can represent a Kubernetes resource of any
/// apiVersion/kind. If you want to deserialize this to a specific struct type (e.g. a Pod),
/// you can use `into_type::<Pod>()` to deserialize it.
/// This type is _not_ used to represent resources that haven't been created yet, since
/// the fields that are populated by the api server would be missing. Resources that don't actually
/// exist in the cluster are instead represented by a plain `serde_json::Value`, since we make almost
/// no assumptions about those.
#[derive(PartialEq, Clone, Serialize, Deserialize)]
#[serde(transparent)]
pub struct K8sResource(Value);

impl Debug for K8sResource {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "K8sResource({})", self.0)
    }
}

impl std::fmt::Display for K8sResource {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl K8sResource {
    /// Attemnpts to create a `K8sResource` from a raw json value. Returns an error if the json
    /// is missing any required fields
    pub fn from_value(value: Value) -> Result<K8sResource, InvalidResourceError> {
        if let Err(msg) = K8sResource::validate(&value) {
            Err(InvalidResourceError {
                message: msg,
                value,
            })
        } else {
            Ok(K8sResource(value))
        }
    }

    /// Convenience function that attempts to deserialize this resource as the given struct type.
    pub fn into_type<T: serde::de::DeserializeOwned>(self) -> Result<T, serde_json::Error> {
        serde_json::from_value(self.into_value())
    }

    /// unwrap the resource into the raw json Value
    pub fn into_value(self) -> Value {
        self.0
    }

    pub fn is_id(&self, id: &ObjectIdRef) -> bool {
        self.get_object_id() == *id
    }

    /// Returns `true` if this resource is of the given type (matches the apiVersion and kind)
    pub fn is_type(&self, k8s_type: &K8sType) -> bool {
        self.api_version() == k8s_type.api_version && self.kind() == k8s_type.kind
    }

    /// returns the `metadata.resourceVersion`, which is guaranteed to exist
    pub fn resource_version(&self) -> &str {
        self.str_value("/metadata/resourceVersion").unwrap()
    }

    /// returns the value of the given label, if it exists
    pub fn get_label_value(&self, label: &str) -> Option<&str> {
        let labels = self.0.pointer("/metadata/labels")?.as_object()?;
        labels.get(label).and_then(Value::as_str)
    }

    /// returns the `metadata.uid`, which is guaranteed to exist
    pub fn uid(&self) -> &str {
        self.str_value("/metadata/uid").unwrap()
    }

    /// returns the `metadata.name`, which is guaranteed to exist
    pub fn name(&self) -> &str {
        self.str_value("/metadata/name").unwrap()
    }

    /// returns the `metadata.namespace`, which will only exist for namespaced resources
    pub fn namespace(&self) -> Option<&str> {
        self.str_value("/metadata/namespace")
    }

    /// returns the `apiVersion` of the resource, which is guaranteed to exist
    pub fn api_version(&self) -> &str {
        self.str_value("/apiVersion").unwrap()
    }

    /// returns the `kind` of the resource, which is guaranteed to exist
    pub fn kind(&self) -> &str {
        self.str_value("/kind").unwrap()
    }

    /// returns the status of the resource, which may be missing. No validation is done on the
    /// status, so callers must not make any assumptions about the `Value`, which may not be an
    /// object, or may even be `Value::Null`
    pub fn status(&self) -> Option<&Value> {
        self.0.pointer("/status")
    }

    /// Returns the id of this object as a pair of namespace/name
    pub fn get_object_id(&self) -> ObjectIdRef {
        let ns = self.namespace().unwrap_or("");
        let name = self.name();
        ObjectIdRef::new(ns, name)
    }

    /// returns the type of the resource as a pair of apiVersion/kind
    pub fn get_type_ref(&self) -> K8sTypeRef {
        let api_version = self.api_version();
        let kind = self.kind();
        K8sTypeRef(api_version, kind)
    }

    /// returns the current value of `metadata.generation`
    pub fn generation(&self) -> i64 {
        self.0
            .pointer("/metadata/generation")
            .and_then(Value::as_i64)
            .unwrap_or(-1)
    }

    /// returns true if `metadata.deletionTimestamp` is set, which would indicate
    /// that the resource is in the process of being deleted
    pub fn is_deletion_timestamp_set(&self) -> bool {
        self.0.pointer("/metadata/deletionTimestamp").is_some()
    }

    fn validate(value: &Value) -> Result<(), &'static str> {
        value
            .pointer("/metadata/resourceVersion")
            .ok_or("missing metadata.resourceVersion")?;
        value
            .pointer("/metadata/name")
            .ok_or("missing metadata.name")?;
        value
            .pointer("/metadata/uid")
            .ok_or("missing metadata.uid")?;
        value.pointer("/apiVersion").ok_or("missing apiVersion")?;
        value.pointer("/kind").ok_or("missing kind")?;
        Ok(())
    }

    /// retrieves a `&str` value from one of the fields within the json.
    /// The `pointer` is an [RFC6901](https://tools.ietf.org/html/rfc6901) json pointer.
    /// Returns None if the given field is missing or of a different type
    pub fn str_value(&self, pointer: &str) -> Option<&str> {
        self.0.pointer(pointer).and_then(Value::as_str)
    }
}

impl json_ext::ResourceJson for K8sResource {
    fn get_api_version(&self) -> Option<&str> {
        Some(self.api_version())
    }

    fn get_kind(&self) -> Option<&str> {
        Some(self.kind())
    }

    fn get_namespace(&self) -> Option<&str> {
        self.namespace()
    }

    fn get_name(&self) -> Option<&str> {
        Some(self.name())
    }
}

impl std::convert::AsRef<Value> for K8sResource {
    fn as_ref(&self) -> &Value {
        &self.0
    }
}

impl std::ops::Deref for K8sResource {
    type Target = Value;

    fn deref(&self) -> &Value {
        &self.0
    }
}

impl std::borrow::Borrow<Value> for K8sResource {
    fn borrow(&self) -> &Value {
        &self.0
    }
}

impl Into<Value> for K8sResource {
    fn into(self) -> Value {
        self.into_value()
    }
}

pub fn type_ref(json: &Value) -> Option<K8sTypeRef<'_>> {
    str_value(json, "/apiVersion")
        .and_then(|api_version| str_value(json, "/kind").map(|kind| K8sTypeRef(api_version, kind)))
}

pub fn str_value<'a, 'b>(json: &'a Value, pointer: &'b str) -> Option<&'a str> {
    json.pointer(pointer).and_then(Value::as_str)
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Copy)]
pub struct K8sTypeRef<'a>(pub &'a str, pub &'a str);
impl<'a> K8sTypeRef<'a> {
    pub fn new(api_version: &'a str, kind: &'a str) -> Self {
        K8sTypeRef(api_version, kind)
    }

    pub fn as_parts(&self) -> (&str, &str) {
        (self.0, self.1)
    }

    pub fn api_version(&self) -> &str {
        self.as_parts().0
    }

    pub fn kind(&self) -> &str {
        self.as_parts().1
    }
}

impl<'a> std::fmt::Display for K8sTypeRef<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}/{}", self.0, self.1)
    }
}

impl<'a> std::cmp::PartialEq<K8sType> for K8sTypeRef<'a> {
    fn eq(&self, rhs: &K8sType) -> bool {
        self.api_version() == rhs.api_version && self.kind() == rhs.kind
    }
}

impl<'a> From<&'_ K8sType> for K8sTypeRef<'a> {
    fn from(k8s_type: &K8sType) -> K8sTypeRef<'static> {
        K8sTypeRef(k8s_type.api_version, k8s_type.kind)
    }
}

impl<'a> From<(&'a str, &'a str)> for K8sTypeRef<'a> {
    fn from((api_version, kind): (&'a str, &'a str)) -> K8sTypeRef<'a> {
        K8sTypeRef(api_version, kind)
    }
}


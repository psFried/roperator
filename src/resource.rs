use crate::config::K8sType;

use serde_json::Value;

use std::borrow::Cow;

pub type JsonObject = serde_json::Map<String, Value>;

#[derive(Debug, PartialEq, Clone)]
pub struct InvalidResourceError {
    pub message: &'static str,
    pub value: Value,
}

impl InvalidResourceError {
    pub fn new(message: &'static str, value: Value) -> Self {
        InvalidResourceError { message, value }
    }
}

impl std::fmt::Display for InvalidResourceError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Invalid Resource: {}", self.message)
    }
}

impl std::error::Error for InvalidResourceError { }


#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
#[serde(transparent)]
pub struct K8sResource(Value);

impl K8sResource {

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

    pub fn into_value(self) -> Value {
        self.0
    }

    pub fn get_resource_version(&self) -> &str {
        self.str_value("/metadata/resourceVersion").unwrap()
    }

    pub fn get_label_value(&self, label: &str) -> Option<&str> {
        let labels = self.0.pointer("/metadata/labels")?.as_object()?;
        labels.get(label).and_then(Value::as_str)
    }

    pub fn uid(&self) -> &str {
        self.str_value("/metadata/uid").unwrap()
    }

    pub fn name(&self) -> &str {
        self.str_value("/metadata/name").unwrap()
    }

    pub fn namespace(&self) -> Option<&str> {
        self.str_value("/metadata/namespace")
    }

    pub fn api_version(&self) -> &str {
        self.str_value("/apiVersion").unwrap()
    }

    pub fn kind(&self) -> &str {
        self.str_value("/kind").unwrap()
    }

    pub fn status(&self) -> Option<&Value> {
        self.0.pointer("/status")
    }

    pub fn get_object_id(&self) -> ObjectIdRef {
        let ns = self.namespace().unwrap_or("");
        let name = self.name();
        ObjectIdRef::new(ns, name)
    }

    pub fn get_type_ref(&self) -> K8sTypeRef {
        let api_version = self.api_version();
        let kind = self.kind();
        K8sTypeRef::new(api_version, kind)
    }

    pub fn get_resource_ref(&self) -> ResourceRef {
        ResourceRef::new(self.get_type_ref(), self.get_object_id())
    }

    pub fn generation(&self) -> i64 {
        self.0.pointer("/metadata/generation").and_then(Value::as_i64).unwrap_or(-1)
    }

    fn validate_self(&self) -> Result<(), InvalidResourceError> {
        K8sResource::validate(&self.0).map_err(|e| {
            InvalidResourceError {
                message: e,
                value: self.0.clone(),
            }
        })
    }

    fn validate(value: &Value) -> Result<(), &'static str> {
        value.pointer("/metadata/resourceVersion").ok_or("missing metadata.resourceVersion")?;
        value.pointer("/metadata/name").ok_or("missing metadata.name")?;
        value.pointer("/metadata/uid").ok_or("missing metadata.uid")?;
        value.pointer("/apiVersion").ok_or("missing apiVersion")?;
        value.pointer("/kind").ok_or("missing kind")?;
        Ok(())
    }

    pub fn str_value(&self, pointer: &str) -> Option<&str> {
        self.0.pointer(pointer).and_then(Value::as_str)
    }
}

impl std::convert::AsRef<Value> for K8sResource {
    fn as_ref(&self) -> &Value {
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

impl std::fmt::Display for K8sResource {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

pub fn object_id(json: &Value) -> Option<ObjectIdRef> {
    let namespace = str_value(json, "/metadata/namspace").unwrap_or("");
    str_value(json, "/metadata/name").map(|name| {
        ObjectIdRef::new(namespace, name)
    })
}

pub fn type_ref(json: &Value) -> Option<K8sTypeRef> {
    str_value(json, "apiVersion").and_then(|api_version| {
        str_value(json, "kind").map(|kind| {
            K8sTypeRef::new(api_version, kind)
        })
    })
}

pub fn str_value<'a, 'b>(json: &'a Value, pointer: &'b str) -> Option<&'a str> {
    json.pointer(pointer).and_then(Value::as_str)
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PairRef<'a>(Cow<'a, str>, Cow<'a, str>);

impl <'a> PairRef<'a> {
    pub fn into_owned(self) -> PairRef<'static> {
        let PairRef(a, b) = self;
        let a: Cow<'static, str> = Cow::Owned(a.into_owned());
        let b: Cow<'static, str> = Cow::Owned(b.into_owned());
        PairRef(a, b)
    }

    pub fn new(a: impl Into<Cow<'a, str>>, b: impl Into<Cow<'a, str>>) -> Self {
        PairRef(a.into(), b.into())
    }

    pub fn as_parts(&self) -> (&str, &str) {
        (self.0.as_ref(), self.1.as_ref())
    }
}

impl <'a> std::fmt::Display for PairRef<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}/{}", self.0, self.1)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct K8sTypeRef<'a>(PairRef<'a>);
impl <'a> K8sTypeRef<'a> {
    pub fn into_owned(self) -> K8sTypeRef<'static> {
        K8sTypeRef(self.0.into_owned())
    }

    pub fn new(api_version: impl Into<Cow<'a, str>>, kind: impl Into<Cow<'a, str>>) -> Self {
        K8sTypeRef(PairRef::new(api_version, kind))
    }

    pub fn as_parts(&self) -> (&str, &str) {
        self.0.as_parts()
    }

    pub const fn v1_pod() -> K8sTypeRef<'static> {
        K8sTypeRef(PairRef(Cow::Borrowed("v1"), Cow::Borrowed("Pod")))
    }
}

impl <'a> std::fmt::Display for K8sTypeRef<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl <'a> std::cmp::PartialEq<K8sType> for K8sTypeRef<'a> {
    fn eq(&self, rhs: &K8sType) -> bool {
        let K8sTypeRef(PairRef(ref api_version, ref kind)) = *self;

        let rhs_group_len = rhs.group.len();
        if kind == rhs.kind.as_str() && api_version.starts_with(rhs.group.as_str()) && api_version.ends_with(rhs.version.as_str()) {
            if !rhs.group.is_empty()  {
                // if it has a group, then the apiVersion should be <group>/<version>, so we need to ensure that
                // the slash is present and nothing else
                if api_version.len() == (rhs_group_len + rhs.version.len() + 1) {
                    let expected_slash = &api_version[rhs_group_len..];
                    return expected_slash.starts_with("/");
                } else {
                    return false;
                }
            } else {
                // if the group is NOT specified in rhs, then we check the length of api_version in order to
                // ensure that it does not contain a '/' at all
                return api_version.len() == rhs_group_len + rhs.version.len();
            }
        }
        false
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ObjectIdRef<'a>(PairRef<'a>);
impl <'a> ObjectIdRef<'a> {
    pub fn into_owned(self) -> ObjectId {
        ObjectIdRef(self.0.into_owned())
    }

    pub fn new(namespace: impl Into<Cow<'a, str>>, name: impl Into<Cow<'a, str>>) -> Self {
        ObjectIdRef(PairRef::new(namespace, name))
    }

    pub fn as_parts(&self) -> (&str, &str) {
        self.0.as_parts()
    }

    pub fn empty() -> ObjectId {
        ObjectIdRef::new("", "")
    }

    pub fn name(&self) -> &str {
        (self.0).1.as_ref()
    }

    pub fn namespace(&self) -> Option<&str> {
        if (self.0).0.is_empty() {
            None
        } else {
            Some((self.0).0.as_ref())
        }
    }

}

pub type ObjectId = ObjectIdRef<'static>;

impl <'a> std::fmt::Display for ObjectIdRef<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ResourceRef<'a>(K8sTypeRef<'a>, ObjectIdRef<'a>);

impl <'a> ResourceRef<'a> {
    pub fn new(type_ref: K8sTypeRef<'a>, id: ObjectIdRef<'a>) -> Self {
        ResourceRef(type_ref, id)
    }

    pub fn into_owned(self) -> ResourceRef<'static> {
        ResourceRef(self.0.into_owned(), self.1.into_owned())
    }
}

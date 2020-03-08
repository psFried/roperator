//! All Kubernetes resources have an id, which is the combination of the
//! `namespace` and `name` fields from the `metadata`. Although the `metadata`
//! also includes a `uid` field, we typically only use that in order to
//! differentiate different resources in the case where one is deleted and
//! another is subsequently created with the same namespace and name.
//!
//! Both of the structs defined here are just holders of a namespace and name.
//! `ObjectId` represents an _owned_ object id, which `ObjectIdRef` represents
//! one that _borrows_ its fields (typically from a json `Value`).
//!
//! Many functions will accept `impl Into<ObjectIdRef<'_>>`, which allows passing
//! either an owned or borrowed version, as well as `(&str, &str)`.
//!
//! ### Note on optional namespaces
//!
//! Namespaces are optional on many Kuberentes resources. Some resources are simply
//! non-namspaced. For others, a missing namespace just communicates a desire to use
//! the _default_ namespace of the current user. In this case, an empty namespace is
//! treated the same as one where it is undefined. Both representations of object ids
//! in roperator represent missing namespaces as empty strings. The `namespace()`
//! function will return an `Option<&str>` to help with cases where you need to determine
//! if a namespace is present or not.
use std::fmt::{self, Display};

/// An owned Object Id
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct ObjectId {
    pub namespace: String,
    pub name: String,
}

impl ObjectId {
    /// create a new `ObjectId` from owned Strings
    pub fn new(namespace: String, name: String) -> ObjectId {
        ObjectId {
            namespace,
            name,
        }
    }

    /// return an `ObjectIdRef` that borrows its fields from this id
    pub fn as_id_ref(&self) -> ObjectIdRef {
        ObjectIdRef {
            namespace: &self.namespace,
            name: &self.name,
        }
    }

    /// Returns an option containing a non-empty namespace. Will return None
    /// if the namespace is an empty string.
    pub fn namespace(&self) -> Option<&str> {
        self.as_id_ref().namespace()
    }

    /// returns the name
    pub fn name(&self) -> &str {
        self.name.as_str()
    }
}


impl Display for ObjectId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.as_id_ref().fmt(f)
    }
}

/// An id that borrows its fields
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct ObjectIdRef<'a> {
    pub namespace: &'a str,
    pub name: &'a str,
}

impl<'a> ObjectIdRef<'a> {
    /// Returns a new id with the given namespace and name fields
    pub fn new(namespace: &'a str, name: &'a str) -> ObjectIdRef<'a> {
        ObjectIdRef {
            namespace,
            name,
        }
    }

    /// Create an owned `ObjectId` by copying the borrowed fields to
    /// newly allocated ones
    pub fn to_owned(&self) -> ObjectId {
        ObjectId {
            namespace: self.namespace.to_owned(),
            name: self.name.to_owned(),
        }
    }

    /// Returns an option containing a non-empty namespace. Will return None
    /// if the namespace is an empty string.
    pub fn namespace(&self) -> Option<&'a str> {
        if self.namespace.is_empty() {
            None
        } else {
            Some(self.namespace)
        }
    }

    /// returns the name field
    pub fn name(&self) -> &'a str {
        self.name
    }
}

impl<'a> Display for ObjectIdRef<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}/{}", self.namespace, self.name)
    }
}

impl<'a> PartialEq<ObjectIdRef<'a>> for ObjectId {
    fn eq(&self, other: &ObjectIdRef<'a>) -> bool {
        self.namespace == other.namespace &&
            self.name == other.name
    }
}

impl<'a> PartialEq<ObjectId> for ObjectIdRef<'a> {
    fn eq(&self, other: &ObjectId) -> bool {
        self.namespace == other.namespace &&
            self.name == other.name
    }
}

impl<'a> From<&'a ObjectId> for ObjectIdRef<'a> {
    fn from(id: &'a ObjectId) -> ObjectIdRef<'a> {
        id.as_id_ref()
    }
}

impl<'a> From<(&'a str, &'a str)> for ObjectIdRef<'a> {
    fn from((namespace, name): (&'a str, &'a str)) -> ObjectIdRef<'a> {
        ObjectIdRef { namespace, name }
    }
}

impl<'a, 'b> From<&'b ObjectIdRef<'a>> for ObjectIdRef<'a> {
    fn from(other: &'b ObjectIdRef<'a>) -> ObjectIdRef<'a> {
        other.clone()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    #[test]
    fn object_id_has_same_hash_as_ref() {
        let id = ObjectId {
            namespace: "foo".to_string(),
            name: "bar".to_string(),
        };
        let id_ref = id.as_id_ref();

        assert_eq!(hash(&id), hash(&id_ref));
        assert_eq!(&id, &id_ref);
    }

    fn hash<T: Hash>(obj: &T) -> u64 {
        let mut hasher = DefaultHasher::new();
        obj.hash(&mut hasher);
        hasher.finish()
    }
}

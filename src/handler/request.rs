//! Contains the `SyncRequest`, which is passed to the `Handler` function, as well as some
//! helpers for accessing and deserializing resources from the request.
//!
use crate::k8s_types::K8sType;
use crate::resource::{K8sResource, K8sTypeRef, ObjectIdRef, ResourceJson};

use serde::de::DeserializeOwned;
use std::fmt::{self, Debug};
use std::marker::PhantomData;

/// The type passed to the Handler that provides a snapshot view of the parent Custom Resource and all of the children
/// as they exist in the Kubernetes cluster. The handler will be passed an immutable reference to this struct.
#[derive(Deserialize, Serialize, Clone, PartialEq)]
pub struct SyncRequest {
    /// The parent custom resource instance
    pub parent: K8sResource,
    /// The entire set of children related to this parent instance, as they exist in the cluster at the time.
    /// In the happy path, this will include all of the children that have been returned in a previous `SyncResponse`
    pub children: Vec<K8sResource>,
}

impl Debug for SyncRequest {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("SyncRequst: ")?;
        let as_string = if f.alternate() {
            serde_json::to_string_pretty(self)
        } else {
            serde_json::to_string(self)
        }
        .map_err(|_| fmt::Error)?;

        f.write_str(as_string.as_str())
    }
}

impl SyncRequest {
    /// Deserialize the parent resource as the given type. It's common to have a struct representation of your CRD, so you
    /// don't have to work with the json directly. This function allows you to easily do just that.
    pub fn deserialize_parent<T: DeserializeOwned>(&self) -> Result<T, serde_json::Error> {
        self.parent.clone().into_type()
    }

    /// Returns a view of just the children of this request, which is useful for passing to a function that determines the current
    /// status. The returned view has a variety of functions for accessing individual children and groups of children.
    pub fn children(&self) -> RequestChildren {
        RequestChildren(self)
    }
}

/// A view of a subset of child resouces that share a given apiVersion and kind. This view has accessors
/// for retrieving deserialized child resources. These accessors all accept `impl Into<ObjectIdRef<'_>>`
/// as their input, which allows passing a variety of types, including `&ObjectId` and `(&str, &str)`.
#[derive(Debug, Clone)]
pub struct TypedView<'a, 'b, T: DeserializeOwned> {
    raw: RawView<'a, 'b>,
    _phantom: PhantomData<T>,
}

impl<'a, 'b, T: DeserializeOwned> TypedView<'a, 'b, T> {
    /// Returns true if the given child exists in this `SyncRequest`, otherwise false. Namespace
    /// can be an empty str for resources that are not namespaced.
    pub fn exists<'c>(&self, id: impl Into<ObjectIdRef<'c>>) -> bool {
        self.raw.exists(id)
    }

    /// Attempts to deserialize the resource with the given namespace and name
    pub fn get<'c>(&self, id: impl Into<ObjectIdRef<'c>>) -> Option<Result<T, serde_json::Error>> {
        self.raw.get(id).map(|c| c.clone().into_type())
    }

    /// Returns the first item with this apiVersion and kind, attempting to deserialize it
    pub fn first(&self) -> Option<Result<T, serde_json::Error>> {
        let mut iter = self.iter();
        iter.next()
    }

    /// Returns an iterator over the child resources that deserializes each of the child resources as the given type
    pub fn iter(&self) -> TypedIter<'a, 'b, T> {
        let inner = self.as_raw().iter();
        TypedIter {
            inner,
            _phantom: PhantomData,
        }
    }

    /// Returns a reference to the inner `RawView`, which allows access to references to the children as `K8sResource`s
    pub fn as_raw(&self) -> &RawView<'a, 'b> {
        &self.raw
    }

    /// Returns true if there are no children of this type
    pub fn is_empty(&self) -> bool {
        self.as_raw().is_empty()
    }

    /// Returns the number of resources that match this apiVersion and kind
    pub fn count(&self) -> usize {
        self.as_raw().count()
    }
}

impl<'a, 'b, T: DeserializeOwned> IntoIterator for TypedView<'a, 'b, T> {
    type Item = Result<T, serde_json::Error>;
    type IntoIter = TypedIter<'a, 'b, T>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// A view of raw `K8sResource` children that share the same apiVersion and kind, which provides
/// convenient accessor functions. These accessors all accept `impl Into<ObjectIdRef<'_>>`
/// as their input, which allows passing a variety of types, including `&ObjectId` and `(&str, &str)`.
#[derive(Debug, Clone)]
pub struct RawView<'a, 'b> {
    // children: &'a RequestChildren<'a>,
    req: &'a SyncRequest,
    type_ref: K8sTypeRef<'b>,
}

impl<'a, 'b> RawView<'a, 'b> {
    /// Returns true if the given child exists in this `SyncRequest`, otherwise false.
    pub fn exists<'c>(&self, id: impl Into<ObjectIdRef<'c>>) -> bool {
        self.get(id).is_some()
    }

    /// Returns a reference to the raw `K8sResource` of this type if it exists.
    /// Namespace can be an empty str for resources that are not namespaced
    pub fn get<'c>(&self, id: impl Into<ObjectIdRef<'c>>) -> Option<&'a K8sResource> {
        let id = id.into();
        self.iter().find(|res| res.is_id(&id))
    }

    /// Returns an iterator over all of the resources of this type
    pub fn iter(&self) -> RawIter<'a, 'b> {
        // self.children.0.children.iter().filter(move |c| {
        //     c.get_type_ref() == self.type_ref
        // })
        RawIter {
            inner: self.req.children.iter(),
            type_ref: self.type_ref,
        }
    }

    /// Returns a reference to the first resource of this type, if one exists
    pub fn first(&self) -> Option<&'a K8sResource> {
        let mut iter = self.iter();
        iter.next()
    }

    /// Returns true if there are no resources that match this apiVersion and kind
    pub fn is_empty(&self) -> bool {
        self.first().is_none()
    }

    /// Returns the number of resources that match this apiVersion and kind
    pub fn count(&self) -> usize {
        self.iter().count()
    }

    pub fn as_type<T: DeserializeOwned>(&self) -> TypedView<'a, 'b, T> {
        TypedView {
            raw: self.clone(),
            _phantom: PhantomData,
        }
    }

    /// Returns the type (apiVersion and kind) for this collection of resources
    pub fn type_ref(&self) -> K8sTypeRef<'b> {
        self.type_ref
    }
}

impl<'a, 'b> IntoIterator for RawView<'a, 'b> {
    type Item = &'a K8sResource;
    type IntoIter = RawIter<'a, 'b>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// An iterator over references to child resources with a specific apiVersion and kind.
pub struct RawIter<'a, 'b> {
    inner: std::slice::Iter<'a, K8sResource>,
    type_ref: K8sTypeRef<'b>,
}

impl<'a, 'b> Iterator for RawIter<'a, 'b> {
    type Item = &'a K8sResource;

    fn next(&mut self) -> Option<&'a K8sResource> {
        while let Some(res) = self.inner.next() {
            if res.get_type_ref() == self.type_ref {
                return Some(res);
            }
        }
        None
    }
}

/// An Iterator over deserialized resources that all share the same
/// apiVersion and kind.
pub struct TypedIter<'a, 'b, T: DeserializeOwned> {
    inner: RawIter<'a, 'b>,
    _phantom: PhantomData<T>,
}

impl<'a, 'b, T: DeserializeOwned> Iterator for TypedIter<'a, 'b, T> {
    type Item = Result<T, serde_json::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|res| res.clone().into_type())
    }
}

/// A view of all of the children from a `SyncRequest`, which has convenient accessors for getting (and optionally deserializing)
/// child resources. This view represents a snapshot of the known state of all the children that are related to a specific parent.
/// The parent status should be computed from this view, NOT from the _desired_ children returned from your handler.
///
/// This view provides a number of functions that return other views of particular types of resources. For example,
/// you could get a view of just all the Pods in the request. These view functions all accept any type that implements
/// `Into<K8sTypeRef<'_>>`, which includes `&K8sType` and `(&str, &str)`.
///
/// ## Examples
///
/// Loop through all the Pods in the request:
/// ```rust
/// use roperator::k8s_types::core::v1::Pod;
///
/// # let request = roperator::handler::request::test_request();
/// for pod in request.children().of_type(Pod) {
///     println!("Example Pod with id: {}", pod.get_object_id());
/// }
/// ```
///
/// Check if a Service exists:
/// ```
/// # let request = roperator::handler::request::test_request();
/// let service_exists: bool = request.children()
///     .of_type(("v1", "Service"))
///     .exists(("foo", "bar"));
/// assert!(service_exists);
/// ```
///
/// Get a typed view, which will automatically deserialize the child resources as the provided type:
/// ```
/// extern crate roperator;
/// extern crate k8s_openapi;
///
/// use k8s_openapi::api::core::v1::Service;
/// # let request = roperator::handler::request::test_request();
/// let service: Service = request.children().of_type(("v1", "Service"))
///     .as_type::<Service>()
///     .first()
///     .expect("no service found in request")
///     .expect("failed to deserialize Service");
/// ```
///
#[derive(Debug)]
pub struct RequestChildren<'a>(&'a SyncRequest);
impl<'a> RequestChildren<'a> {
    /// Provides a view of all the children with the given apiVersion/kind. The returned view provides a variety of functions
    /// to provide access to the matching subset of child resources
    pub fn of_type<'b>(&self, type_ref: impl Into<K8sTypeRef<'b>>) -> RawView<'a, 'b> {
        RawView {
            req: self.0,
            type_ref: type_ref.into(),
        }
    }

    /// Provides a typed view of all the children with the given apiVersion/kind. This view
    /// provides typed accessor functions to return deserailized children
    pub fn with_type<'b, T: DeserializeOwned>(
        &self,
        k8s_type: &'static K8sType,
    ) -> TypedView<'a, 'b, T> {
        self.of_type(k8s_type).as_type::<T>()
    }

    /// Returns an iterator over all of the children in the `SyncRequest`
    pub fn iter(&self) -> impl Iterator<Item = &K8sResource> {
        self.0.children.iter()
    }

    /// Returns true if the request contains a resource with the same `apiVersion`, `kind`,
    /// `metadata.namespace`, and `metadata.name` as the given `resource`.
    ///
    pub fn get_child_with_id<'b>(
        &self,
        resource: &'b serde_json::Value,
    ) -> Option<&'a K8sResource> {
        let type_id = resource
            .get_type_ref()
            .and_then(|v| resource.get_id_ref().map(move |id| (v, id)));

        if let Some((k8s_type, id)) = type_id {
            self.of_type(k8s_type).get(id)
        } else {
            panic!("contains_child_with_id called with argument that does not specify one of apiVersion, kind, metadata.name, resource: '{}'", resource);
        }
    }
}

#[cfg(any(feature = "test", test))]
macro_rules! resource {
    ($toks:tt) => {
        K8sResource::from_value(json!($toks)).expect("invalid test resource")
    };
}

#[cfg(any(feature = "test", test))]
pub fn test_request() -> SyncRequest {
    use serde_json::json;

    SyncRequest {
        parent: resource! {{
            "apiVersion": "foo.com/v1",
            "kind": "MyThing",
            "metadata": {
                "namespace": "foo",
                "name": "bar",
                "resourceVersion": "1234455",
                "uid": "abc123"
            },
            "spec": {
                "a": 1,
                "b": "two"
            }
        }},
        children: vec![
            resource!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "foo",
                    "name": "bar",
                    "resourceVersion": "1234455",
                    "uid": "abc123"
                },
                "spec": {
                    "containers": [ ]
                }
            }),
            resource!({
                "apiVersion": "v1",
                "kind": "Service",
                "metadata": {
                    "namespace": "foo",
                    "name": "bar",
                    "resourceVersion": "234",
                    "uid": "sldfkj"
                },
                "spec": {
                    "selector": {
                        "oooohhhh": "weeee"
                    }
                }
            }),
            resource!({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "namespace": "foo",
                    "name": "baz",
                    "resourceVersion": "543231",
                    "uid": "def456"
                },
                "spec": {
                    "containers": [ ]
                }
            }),
        ],
    }
}

#[cfg(test)]
pub mod test {
    use super::*;

    #[test]
    fn request_children_allows_retrieving_first_resource_with_type() {
        let request = test_request();

        let first_service = request
            .children()
            .with_type::<AnyResource>(crate::k8s_types::core::v1::Service)
            .first()
            .expect("no pod found")
            .expect("failed to deserialize child");
        assert_eq!("sldfkj", first_service.metadata.uid.as_str());
    }

    #[test]
    fn request_children_allows_retrieving_all_children_with_type() {
        let request = test_request();

        let all_pods = request
            .children()
            .with_type::<AnyResource>(crate::k8s_types::core::v1::Pod)
            .iter()
            .collect::<Result<Vec<AnyResource>, serde_json::Error>>()
            .expect("failed to deserialize child");

        assert_eq!(2, all_pods.len());
        for pod in all_pods {
            assert_eq!("Pod", pod.kind);
        }
    }

    #[test]
    fn request_children_allows_retrieving_a_specific_typed_child() {
        let request = test_request();

        let result = request
            .children()
            .with_type::<AnyResource>(crate::k8s_types::core::v1::Pod)
            .get(("foo", "baz"))
            .expect("failed to retrieve pod")
            .expect("failed to deserialize pod");

        assert_eq!("def456", result.metadata.uid);
    }

    #[test]
    fn request_children_allows_retrieving_first_raw_resource_matching_type() {
        let request = test_request();

        let first_service = request
            .children()
            .of_type(crate::k8s_types::core::v1::Service)
            .first()
            .expect("no pod found");
        assert_eq!("sldfkj", first_service.uid());
    }

    #[test]
    fn request_children_allows_retrieving_a_specific_raw_child() {
        let request = test_request();
        let result = request
            .children()
            .of_type(crate::k8s_types::core::v1::Pod)
            .get(("foo", "baz"))
            .expect("failed to retrieve pod");

        assert_eq!("def456", result.uid());
    }

    #[test]
    fn count_returns_number_of_matching_children() {
        let request = test_request();
        let pods = request
            .children()
            .with_type::<AnyResource>(crate::k8s_types::core::v1::Pod);

        assert_eq!(2, pods.count());

        let services = request.children().of_type(("v1", "Service"));
        assert_eq!(1, services.count());

        let foos = request.children().of_type(("v0", "NeverGonnaExist"));
        assert_eq!(0, foos.count());
    }

    #[test]
    fn views_can_be_cloned_without_copying_the_request() {
        let request = test_request();

        let pods = request.children().of_type(crate::k8s_types::core::v1::Pod);
        let more_pods = pods.clone();

        let a = pods.first().expect("failed to retrieve pod a");
        let b = more_pods.first().expect("failed to retrieve pod b");
        assert_eq!(a, b);
    }

    #[test]
    fn all_view_functions_return_none_when_no_resources_exist_with_type() {
        let request = test_request();

        let non_existant_type = crate::k8s_types::node_k8s_io::v1beta1::RuntimeClass;
        let kids = request.children();
        assert_eq!(0, kids.of_type(non_existant_type).count());
        assert!(kids.of_type(non_existant_type).is_empty());
        assert!(kids.of_type(non_existant_type).first().is_none());

        assert_eq!(0, kids.with_type::<AnyResource>(non_existant_type).count());
        assert!(kids.with_type::<AnyResource>(non_existant_type).is_empty());
        assert!(kids
            .with_type::<AnyResource>(non_existant_type)
            .first()
            .is_none());
    }

    #[derive(Debug, Deserialize, PartialEq)]
    #[serde(rename_all = "camelCase")]
    struct MyMeta {
        name: String,
        namespace: String,
        resource_version: String,
        uid: String,
    }

    #[derive(Debug, Deserialize, PartialEq)]
    #[serde(rename_all = "camelCase")]
    struct AnyResource {
        api_version: String,
        kind: String,
        metadata: MyMeta,
    }
}

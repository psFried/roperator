# Parent Custom Resource Definition

At this point, you'll need to have a Custom Resource Definition to work with. If you haven't created a CRD before, check out the [Kuberntes docs](https://kubernetes.io/docs/tasks/access-kubernetes-api/custom-resources/custom-resource-definitions/#) on how to do that.

**Note:** Kubernetes `CustomResourceDefinition` resources have recently (as of Kubernetes 1.16) moved from beta to stable. The most recent Kubernetes documentation may use the `apiVersion` `apiextensions.k8s.io/v1`, but for Kubernetes versions 1.15 and earlier, you'll need to use `apiextensions.k8s.io/v1beta1` instead.

## Enable the status subresource

Roperator requires that your CRD have the `status` subresource enabled in order to update the status of the parent. Enabling the status subresource is as simple as adding the following to your CRD yaml:

```yaml
spec:
  subresources:
    status: {}
```

## Define the parent `K8sType`


Now we need to tell Roperator about your Custom Resource. Roperator already has type definitions for resources that are builtin to Kubernetes, but since our parent is a custom resource, we'll have to describe the type ourselves. We'll use the following CRD for this example:

```yaml
apiVersion: apiextensions.k8s.io/v1beta1
kind: CustomResourceDefinition
metadata:
  name: foos.example.com
spec:
  group: example.com
  versions:
    - name: v1
      served: true
      storage: true
  scope: Namespaced
  names:
    plural: foos
    singular: foo
    kind: Foo
  subresources:
    status: {}
```

For the above CRD, we would define the following `K8sType` in our code:

```rust
// in main.rs
use roperator::prelude::K8sType;

// A static reference to the type of our parent resource
pub PARENT_TYPE: &K8sType = &K8sType {
    api_version: "example.com/v1",
    kind: "Foo",
    plural_kind: "foos",
};
```

Roperator uses static references for defining `K8sType`s. This makes it very convenient to safely access the type information from anywhere in your code, since these definitions are immutable.

### Look Ma, no schema!

Roperator only needs to know this minimal amount of information about your types because it treats all resources the same, and only stores them as plain JSON values. So there's no need to define structs for your resources if you don't want to. You _may_ still define structs if you want, but they only need to define the fields that you care about. They don't need to include fields for the extra stuff that's added by the Kubernetes api server. See the [Serialization chapter](../reference/serialization.md) for more details on that.


# Next

Finishing [Operator Configuration](operator-config.md)

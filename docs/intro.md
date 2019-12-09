# Introduction

A Kubernetes Operator is a custom _controller_ that manages application instances running in a Kuberentes cluster. Applications can be rather complex, and can involve quite a few different resources. Operators offer a place to encode all of the specialized knowlege and logic that's required to run an arbitrarily complex application in Kubernetes. There are a few different components that help with all that:

### Custom Resource Definition

A [Custom Resource Definition](https://kubernetes.io/docs/concepts/extend-kubernetes/api-extension/custom-resources/), also known as a CRD. To quote from the Kubernetes documentation:

> A resource is an endpoint in the Kubernetes API that stores a collection of API objects of a certain kind. For example, the built-in pods resource contains a collection of Pod objects.
>
> A custom resource is an extension of the Kubernetes API that is not necessarily available in a default Kubernetes installation. It represents a customization of a particular Kubernetes installation. However, many core Kubernetes functions are now built using custom resources, making Kubernetes more modular.

To get started creating an operator, you'll first need a CRD. For example, if we want our operator to manage an application called `FooApp`, then we'd define a `FooApp` CRD. Once we've created the Custom Resource _Definition_, we can then start creating resources in the cluster with `kind: FooApp`. The `spec` of these resources should contain fields that are relevant specifically to `FooApp`.

### The Controller

Next we need a _controller_. The job of a controller is to watch all of the instances of your custom resource, and to do whatever is needed to run the FooApp instance. For example, whenever a `FooApp` resource is created in Kuberentes, the controller will create all of the other resources that it takes to run a FooApp instance, like Pods, Services, ConfigMaps, Secrets, Volumes, or even other custom resources. We call these _child_ resources, and the FooApp resource is the _parent_. If the FooApp resource is modified, then the controller should recognize that and update the chidren as appropriate for that application. If the FooApp is deleted, then the controller should clean up all of the child resources for that parent.

While controllers are conceptually simple, they actually have quite a bit to do. For example, they should also watch all the child resources so that they can recreate them if they fail or accidentally get deleted. They should also keep the status up to date on the parent, so that we can easily tell if a FooApp is running correctly. Controllers also need to ensure that they themselves are fault tolerant, and that they won't miss observing changes to any of the resources they control. It's a lot to manage, but that's why Roperator is here to help!


## Declarative Operator

Roperator helps you tackle the complexity by providing a simple, declarative API. Whenever one of your parent or child resources is modified, Roperator will gether up all the state related to that application instance (the parent and all child resources), and it will pass a snapshot of that state to a function that you've defined. Your function looks at that state and it returns an object that represents the _desired state_ of all of the children and the `status` of the parent. Roperator will then take care of updating any resources that differ from the desired state! This simple model makes it easy to write correct and fault tolerant operators, while still retaining enough flexibility to handle all but the most outlandish scenarios. Operators written with Roperator can even manage external resources.

# Next Steps

- [The guide](guide/index.md) is a great place to start
- Check out [the examples](https://github.com/psFried/roperator/tree/master/examples) over at Github
- The [API documentation](https://docs.rs/roperator) is also available over at docs.rs

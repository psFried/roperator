# Temp Namespace example Operator

This example uses a CRD to represent a temporary namespace that will be deleted automatically after an expiration period. This shows off how to use time-based regular re-syncs, and also how to use the `FailableHandler` trait.

To run this example:

- Ensure that your kubeconfig file is updated and that the current context is pointed to a Kubernetes cluster that you have access to
- Create the CustomResourceDefinition in your cluster using `kubectl apply -f examples/temp-namespace/crd.yaml`
- Next, run the operator using `cargo run --example examples/temp-namespace`
- Now use kubectl to create an instance of your EchoServer using `kubectl apply -f examples/temp-namespace/example.yaml`
- The operator will then create the actual namespace called `my-temp-namespace`. Run `kubectl get tempns my-temp-namespace -o yaml` and you should see the `createdAt` timestamp.

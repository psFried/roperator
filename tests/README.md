# Integration Tests

These tests use the `TestKit` to run the operator against a real Kubernetes cluster and assert that the behavior is correct.

Prior to running the tests, you must create the CRDs by running `kubectl apply -f tests/resources`.

The tests rely primarily on CRDs as _both_ the parent and child types. We try to be careful about using things like Pods in the tests, since the state of a Pod can be affected by many factors of the environment. For example, if you run the tests in a cluster that doesn't have access to the main docker hub, then many tests may fail. Or if a cluster has admission webhooks configured that mutate the state of a Pod or some other resource, that could also cause things to fail. So, we just define the CRDs that we'll use in the tests, and primarily stick to those.


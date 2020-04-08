use roperator::prelude::*;
use roperator::resource::{K8sResource, ObjectIdRef};
use roperator::runner::testkit::{HandlerErrors, TestKit};

use roperator::serde_json::{json, Value};

use std::fmt::{self, Display};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

fn make_client_config(operator_name: &str) -> ClientConfig {
    if let Ok(conf) = ClientConfig::from_service_account(operator_name) {
        conf
    } else {
        ClientConfig::from_kubeconfig(operator_name).expect("Failed to create client configuration")
    }
}

fn unique_namespace(prefix: &str) -> String {
    let ts = std::time::SystemTime::now();
    let epoch_time = ts
        .duration_since(std::time::UNIX_EPOCH)
        .expect("System time is set to before the unix epoch")
        .as_secs();

    format!("{}-{}", prefix, epoch_time)
}

static PARENT_TYPE: &K8sType = &K8sType {
    api_version: "roperator.com/v1alpha1",
    kind: "TestParent",
    plural_kind: "testparents",
};

static CHILD_ONE_TYPE: &K8sType = &K8sType {
    api_version: "roperator.com/v1alpha1",
    kind: "TestChildOne",
    plural_kind: "testchildones",
};

fn setup(name: &str, handler: impl Handler) -> TestKit {
    let operator_config = OperatorConfig::new(name, PARENT_TYPE)
        .within_namespace(name)
        .with_child(CHILD_ONE_TYPE, ChildConfig::recreate())
        .expose_health(false)
        .expose_metrics(false);

    setup_with(name, handler, operator_config)
}

fn setup_with(name: &str, handler: impl Handler, operator_config: OperatorConfig) -> TestKit {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "roperator=INFO");
    }
    let _ = env_logger::builder().is_test(true).try_init();
    let conf = operator_config
        .within_namespace(name)
        .expose_health(false)
        .expose_metrics(false);

    let client_config = make_client_config(name);
    TestKit::with_test_namespace(name, conf, client_config, handler)
        .expect("Failed to create test kit")
}

fn parent(namespace: &str, name: &str) -> Value {
    json!({
        "apiVersion": PARENT_TYPE.api_version,
        "kind": PARENT_TYPE.kind,
        "metadata": {
            "namespace": namespace,
            "name": name,
        },
        "spec": {
            "foo": "bar",
        }
    })
}

#[test]
fn operator_reconciles_a_parent_with_a_child() {
    let namespace = unique_namespace("stable-state");
    let mut testkit = setup(namespace.as_str(), create_child_handler);

    let parent_name = "parent-one";
    let parent = parent(&namespace, parent_name);
    testkit
        .create_resource(PARENT_TYPE, &parent)
        .expect("failed to create parent resource");

    let expected_child = json!({
        "apiVersion": "roperator.com/v1alpha1",
        "kind": "TestChildOne",
        "metadata": {
            "namespace": &namespace,
            "name": parent_name,
            "labels": {
                "app.kubernetes.io/name": parent_name
            }
        },
        "spec": {
            "parentSpec": {
                "foo": "bar",
            },
        }
    });

    let id = ObjectIdRef::new(&namespace, parent_name);
    testkit.assert_resource_eq_eventually(
        CHILD_ONE_TYPE,
        &id,
        expected_child,
        Duration::from_secs(15),
    );
    testkit.delete_parent(&id);
    testkit.assert_resource_deleted_eventually(CHILD_ONE_TYPE, &id, Duration::from_secs(30));
}

#[test]
fn operator_retries_finalize_when_response_retry_is_some() {
    use std::sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    };

    const REQUIRED_FINALIZE_CALLS: u64 = 3;
    // Handler that will increment the counter for each finalize call, and return `finalized: true` once it reaches 3
    struct FinalizeHandler(Arc<AtomicU64>);
    impl Handler for FinalizeHandler {
        fn sync(&self, request: &SyncRequest) -> SyncResponse {
            create_child_handler(request)
        }

        fn finalize(&self, _request: &SyncRequest) -> Result<FinalizeResponse, Error> {
            let num_calls = self.0.fetch_add(1, Ordering::SeqCst);
            let status = json!({
                "finalizeCalls": num_calls,
            });
            let retry = if num_calls >= REQUIRED_FINALIZE_CALLS {
                None
            } else {
                Some(Duration::from_millis(5))
            };
            Ok(FinalizeResponse { status, retry })
        }
    }

    let finalize_call_count = Arc::new(AtomicU64::new(0));

    let namespace = unique_namespace("retry-finalize");
    let mut testkit = setup(
        namespace.as_str(),
        FinalizeHandler(finalize_call_count.clone()),
    );

    let parent_name = "test-parent";
    let parent = parent(namespace.as_str(), parent_name);
    testkit
        .create_resource(PARENT_TYPE, &parent)
        .expect("failed to create parent resource");

    // parent and child just happen to have the same name, but different types
    let id = ObjectIdRef::new(namespace.as_str(), parent_name);
    testkit.assert_resource_exists_eventually(CHILD_ONE_TYPE, &id, Duration::from_secs(10));

    let expected_parent_fields = json!({
        "metadata": {
            "finalizers": [namespace.as_str()],
        }
    });
    testkit.assert_resource_eq_eventually(
        PARENT_TYPE,
        &id,
        expected_parent_fields,
        Duration::from_secs(5),
    );
    testkit.delete_parent(&id);

    testkit.assert_resource_deleted_eventually(PARENT_TYPE, &id, Duration::from_secs(30));
    testkit.assert_resource_deleted_eventually(CHILD_ONE_TYPE, &id, Duration::from_secs(20));

    let actual_finalize_calls = finalize_call_count.load(Ordering::SeqCst);
    assert!(actual_finalize_calls >= REQUIRED_FINALIZE_CALLS);
}

#[test]
fn child_is_recreated_after_being_deleted() {
    let namespace = unique_namespace("recreate-after-del");
    let mut testkit = setup(namespace.as_str(), create_child_handler);

    let parent_name = "parent";
    let parent = parent(&namespace, parent_name);
    testkit
        .create_resource(PARENT_TYPE, &parent)
        .expect("failed to create parent resource");

    let id = ObjectIdRef::new(&namespace, parent_name);
    testkit.assert_resource_exists_eventually(CHILD_ONE_TYPE, &id, Duration::from_secs(15));

    let old_child = testkit
        .get_resource_from_api_server(CHILD_ONE_TYPE, &id)
        .expect("Failed to fetch resource")
        .expect("child did not exist");
    let old_child = K8sResource::from_value(old_child).expect("old child was invalid");

    testkit
        .delete_resource(CHILD_ONE_TYPE, &id)
        .expect("failed to delete child");
    testkit.reconcile_and_assert_success(Duration::from_secs(10));

    let new_child = testkit
        .get_resource_from_api_server(CHILD_ONE_TYPE, &id)
        .expect("Failed to fetch resource")
        .expect("child did not exist");
    let new_child = K8sResource::from_value(new_child).expect("new_child was invalid");
    assert_ne!(old_child.uid(), new_child.uid()); // assert that they're different instances
}

#[test]
fn child_is_not_updated_until_deleted_when_update_strategy_is_on_delete() {
    let namespace = unique_namespace("on-delete");
    let operator_config = OperatorConfig::new(namespace.as_str(), PARENT_TYPE)
        .with_child(CHILD_ONE_TYPE, ChildConfig::on_delete());
    let mut testkit = setup_with(namespace.as_str(), create_child_handler, operator_config);

    let parent_name = "parent";
    let parent = parent(&namespace, parent_name);
    testkit
        .create_resource(PARENT_TYPE, &parent)
        .expect("failed to create parent resource");

    let id = ObjectIdRef::new(&namespace, parent_name);
    testkit.assert_resource_exists_eventually(CHILD_ONE_TYPE, &id, Duration::from_secs(15));

    let old_child = testkit
        .get_resource_from_api_server(CHILD_ONE_TYPE, &id)
        .expect("Failed to fetch resource")
        .expect("child did not exist");
    let old_child = K8sResource::from_value(old_child).expect("old child was invalid");
    assert_eq!(
        Some("bar"),
        old_child
            .pointer("/spec/parentSpec/foo")
            .and_then(Value::as_str)
    );
    let prev_generation = old_child.generation();

    // now update the parent
    let mut new_parent = testkit
        .get_resource_from_api_server(PARENT_TYPE, &id)
        .expect("failed to fetch parent")
        .expect("parent was not found");
    new_parent
        .pointer_mut("/spec")
        .unwrap()
        .as_object_mut()
        .unwrap()
        .insert("foo".to_string(), Value::String("CANARY".to_string()));
    testkit
        .replace_resource(PARENT_TYPE, &id, &new_parent)
        .expect("failed to update parent");

    testkit.reconcile_and_assert_success(Duration::from_secs(5));

    let old_child = testkit
        .get_resource_from_api_server(CHILD_ONE_TYPE, &id)
        .expect("Failed to fetch resource")
        .expect("child did not exist");
    let old_child = K8sResource::from_value(old_child).expect("old child was invalid");
    assert_eq!(
        Some("bar"),
        old_child
            .pointer("/spec/parentSpec/foo")
            .and_then(Value::as_str)
    );
    assert_eq!(prev_generation, old_child.generation());

    testkit
        .delete_resource(CHILD_ONE_TYPE, &id)
        .expect("failed to delete child");
    testkit.reconcile_and_assert_success(Duration::from_secs(10));

    let new_child = testkit
        .get_resource_from_api_server(CHILD_ONE_TYPE, &id)
        .expect("Failed to fetch resource")
        .expect("child did not exist");
    let new_child = K8sResource::from_value(new_child).expect("new_child was invalid");
    assert_ne!(old_child.uid(), new_child.uid()); // assert that they're different instances
}

#[test]
fn handler_is_invoked_after_waiting_when_resync_is_some() {
    let namespace = unique_namespace("resync");

    // 2 syncs would be normal if nothing changed, since we re-sync so that the handler
    // can observe the updated status. But for this test we want to ensure that we'll
    // re-sync anyway if the resync field is Some
    const EXPECTED_SYNCS: u64 = 5;

    let counter = Arc::new(AtomicU64::new(0));
    struct ResyncHandler(Arc<AtomicU64>);
    impl Handler for ResyncHandler {
        fn sync(&self, _request: &SyncRequest) -> SyncResponse {
            let i = self.0.fetch_add(1, Ordering::SeqCst);
            let resync = if i > EXPECTED_SYNCS {
                None
            } else {
                Some(Duration::from_millis(5))
            };
            SyncResponse {
                status: json!({
                    "foo": "bar"
                }),
                children: Vec::new(),
                resync,
            }
        }
    }
    let handler = ResyncHandler(counter.clone());
    let mut testkit = setup(namespace.as_str(), handler);

    let parent_name = "test-parent";
    let parent = parent(&namespace, parent_name);
    testkit
        .create_resource(PARENT_TYPE, &parent)
        .expect("failed to create parent resource");

    let start = std::time::Instant::now();
    while counter.load(Ordering::SeqCst) < EXPECTED_SYNCS
        && start.elapsed() < Duration::from_secs(5)
    {
        testkit
            .reconcile(Duration::from_secs(1))
            .expect("failed to reconcile");
    }

    let actual_syncs = counter.load(Ordering::SeqCst);
    assert!(actual_syncs >= EXPECTED_SYNCS);
}

#[derive(Debug, PartialEq, Clone)]
struct MockHandlerError(u64);
impl Display for MockHandlerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}
impl std::error::Error for MockHandlerError {}

struct ReturnErrorHandler<T: Handler> {
    counter: std::sync::atomic::AtomicU64,
    match_namespace: String,
    match_name: String,
    delegate: T,
}

impl<T: Handler> ReturnErrorHandler<T> {
    fn new(delegate: T, match_namespace: &str, match_name: &str) -> ReturnErrorHandler<T> {
        ReturnErrorHandler {
            counter: std::sync::atomic::AtomicU64::new(0),
            match_namespace: match_namespace.to_string(),
            match_name: match_name.to_string(),
            delegate,
        }
    }
    fn should_return_error(&self, req: &SyncRequest) -> bool {
        let ns = req.parent.namespace().unwrap_or("");
        let name = req.parent.name();
        self.match_namespace == ns && self.match_name == name
    }
}

impl<T: Handler> Handler for ReturnErrorHandler<T> {
    fn sync(&self, req: &SyncRequest) -> SyncResponse {
        self.delegate.sync(req)
    }

    fn finalize(&self, req: &SyncRequest) -> Result<FinalizeResponse, Error> {
        if self.should_return_error(req) {
            let index = self
                .counter
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Err(Box::new(MockHandlerError(index)))
        } else {
            self.delegate.finalize(req)
        }
    }
}

fn create_child_handler(req: &SyncRequest) -> SyncResponse {
    let namespace = req.parent.namespace();
    let name = req.parent.name();

    let child = json!({
        "apiVersion": "roperator.com/v1alpha1",
        "kind": "TestChildOne",
        "metadata": {
            "namespace": namespace,
            "name": name,
            "labels": {
                "app.kubernetes.io/name": name,
            }
        },
        "spec": {
            "parentSpec": req.parent.pointer("/spec"),
        }
    });

    let children = req.children();
    let test_child_ones = children.of_type(CHILD_ONE_TYPE);
    let count = test_child_ones.iter().count();

    let message = if count == 0 {
        "Waiting for children to be created".to_string()
    } else {
        format!("Has {} children", count)
    };

    SyncResponse {
        status: json!({
            "message": message,
            "childCount": count,
        }),
        children: vec![child],
        resync: None,
    }
}

use crate::{
    runner::{
        OperatorState, create_operator_state, HandlerRef,
        client::Client,
        reconcile::compare,
        metrics::Metrics,
    },
    config::{OperatorConfig, ClientConfig, K8sType},
    handler::{self, Handler, SyncRequest, SyncResponse, FinalizeResponse},
    resource::{K8sResource, ObjectIdRef, ObjectId}
};

use tokio::runtime::current_thread::{Runtime, TaskExecutor};
use serde_json::Value;
use serde::Serialize;

use std::fmt::{self, Debug, Display};
use std::sync::{Arc, RwLock};
use std::sync::atomic::AtomicBool;
use std::time::{Instant, Duration};
use std::collections::{HashSet, HashMap};

/// Trait for errors that can be returned from a Handler function. This just enforces all of the
/// bounds we need in order to convert the errors into trait objects and to send them between threads.
pub trait TestError: std::error::Error + Send + 'static {}
impl <T> TestError for T where T: std::error::Error + Send + 'static {}

pub type Error = Box<dyn TestError>;

impl <T> From<T> for Error where T: TestError {
    fn from(e: T) -> Error {
        Box::new(e)
    }
}

pub struct TestKit {
    state: OperatorState,
    handler: HandlerRef,
    instrumented_handler: InstrumentedHandler,
    runtime: Runtime,
    client: Client,
    parents_needing_sync: HashSet<String>,
    delete_namespace_on_drop: bool,
    namespace: Option<String>,
}

impl Debug for TestKit {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("TestKit")
                .field("operator_state", &self.state)
                .field("instrumented_handler", &self.instrumented_handler)
                .field("client", &self.client)
                .field("parents_needing_sync", &self.parents_needing_sync)
                .finish()
    }
}

pub trait ToJson {
    fn to_json(&self) -> Value;
    fn into_json(self) -> Value;
}

impl <T: Serialize + Clone> ToJson for T {
    fn to_json(&self) -> Value {
        self.clone().into_json()
    }
    fn into_json(self) -> Value {
        serde_json::to_value(self).expect("failed to serialize value")
    }
}

impl Drop for TestKit {
    fn drop(&mut self) {
        if self.delete_namespace_on_drop {
            let TestKit {ref namespace, ref client, ref mut runtime, ..} = self;
            if let Some(ns) = namespace.as_ref() {
                log::info!("Deleting test namespace: '{}'", ns);
                let id = ObjectIdRef::new("", ns);
                let k8s_type = K8sType::namespaces();
                let result = runtime.block_on(async {
                    client.delete_resource(&k8s_type, &id).await
                });
                if let Err(err) = result {
                    log::error!("Failed to delete test namespace: '{}': {:?}", ns, err);
                }
            }
        }
    }
}

impl TestKit {

    pub fn with_test_namespace(namespace: impl Into<String>, operator_config: OperatorConfig, client_config: ClientConfig, handler: impl Handler) -> Result<TestKit, Error> {
        let ns = namespace.into();
        let mut testkit = TestKit::create(operator_config.within_namespace(ns.as_str()), client_config, handler)?;

        let ns_type = K8sType::namespaces();
        let namespace_id = ObjectIdRef::new("", ns.as_str());

        if testkit.get_resource(&ns_type, &namespace_id)?.is_none() {
            let namespace_json = serde_json::json!({
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": {
                    "name": ns.as_str(),
                    "labels": {
                        "roperator.com/test-namespace": "true",
                    }
                }
            });
            testkit.create_resource(&ns_type, &namespace_json)?;
        }

        Ok(testkit.delete_namespace_on_drop())
    }

    pub fn delete_namespace_on_drop(mut self) -> Self {
        if self.namespace.is_none() {
            panic!("cannot delete namespace on drop because no namespace has been configured for testkit");
        }
        self.delete_namespace_on_drop = true;
        self
    }

    pub fn create(operator_config: OperatorConfig, client_config: ClientConfig, handler: impl Handler) -> Result<TestKit, Error> {
        let metrics = Metrics::new();
        let client = Client::new(client_config, metrics.client_metrics())?;
        let mut runtime = Runtime::new()?;
        let namespace = operator_config.namespace.clone();

        let state = runtime.block_on( async {
            let mut executor = TaskExecutor::current();
            create_operator_state(&mut executor, metrics, Arc::new(AtomicBool::new(true)), operator_config, client.clone()).await
        });

        let (instrumented_handler, handler) = InstrumentedHandler::wrap(handler);

        Ok(TestKit {
            state,
            handler,
            instrumented_handler,
            runtime,
            client,
            parents_needing_sync: HashSet::new(),
            delete_namespace_on_drop: false,
            namespace,
        })
    }

    pub fn assert_child_resource_eq(&mut self, k8s_type: &K8sType, id: &ObjectIdRef<'_>, expected: impl ToJson) {
        let expected = expected.into_json();
        let TestKit {ref state, ref mut runtime, ..} = *self;

        let maybe_diff = runtime.block_on(async {
            compare_child_objects(state, k8s_type, id, expected).await
        }).unwrap();
        if let Some(diff) = maybe_diff {
            panic!("Found diff in {}: {}\n{}", k8s_type, id, diff);
        }
    }

    pub fn get_resource(&mut self, k8s_type: &K8sType, id: &ObjectIdRef<'_>) -> Result<Option<Value>, Error> {
        let TestKit {ref client, ref mut runtime, ..} = *self;
        let maybe_resource = runtime.block_on(async {
            client.get_resource(k8s_type, id).await
        })?;
        Ok(maybe_resource)
    }

    pub fn replace_resource(&mut self, k8s_type: &K8sType, id: &ObjectIdRef<'_>, new_resource: impl ToJson) -> Result<(), Error> {
        let TestKit {ref client, ref mut runtime, ..} = *self;
        runtime.block_on(async {
            client.replace_resource(k8s_type, id, &new_resource.into_json()).await
        })?;
        Ok(())
    }

    pub fn create_resource(&mut self, k8s_type: &K8sType, new_resource: &Value) -> Result<(), Error> {
        let TestKit {ref client, ref mut runtime, ..} = *self;
        runtime.block_on(async {
            client.create_resource(k8s_type, new_resource).await
        })?;
        Ok(())
    }

    pub fn delete_resource(&mut self, k8s_type: &K8sType, id: &ObjectIdRef<'_>) -> Result<(), Error> {
        let TestKit {ref client, ref mut runtime, ..} = *self;
        runtime.block_on(async {
            client.delete_resource(k8s_type, id).await
        })?;
        Ok(())
    }

    pub fn reconcile_and_assert_success(&mut self, max_timeout: Duration) {
        self.run_reconciliation(true, max_timeout).expect("Reconciliation failed");
    }

    pub fn run_reconciliation(&mut self, fail_on_handler_error: bool, max_timeout: Duration) -> Result<(), Error> {
        let TestKit {ref mut state, ref mut parents_needing_sync, ref handler, ref instrumented_handler, ref mut runtime, ..} = *self;
        // TODO: check the running flag and return an error if it's false
        let result = runtime.block_on(async {
            do_reconciliation_run(state, parents_needing_sync, handler, instrumented_handler, max_timeout).await
        });

        if fail_on_handler_error {
            // Return any handler errors first, before we return any timeout errors
            instrumented_handler.take_errors()?;
        } else {
            instrumented_handler.reset();
        }

        result
    }
}


#[derive(Debug, Default, Clone)]
struct SyncRecord {
    sync_count: usize,
    sync_errors: usize,
    finalize_count: usize,
    finalize_errors: usize,
    last_sync_request: Option<SyncRequest>,
    last_sync_response: Option<Result<SyncResponse, String>>,
    last_finalize_request: Option<SyncRequest>,
    last_finalize_response: Option<Result<FinalizeResponse, String>>,
}

impl SyncRecord {
    fn sync_started(&mut self, req: &SyncRequest) {
        self.sync_count += 1;
        self.last_sync_request = Some(req.clone());
    }

    fn sync_finished(&mut self, resp: &Result<SyncResponse, handler::Error>) {
        match resp.as_ref() {
            Ok(response) => {
                self.last_sync_response = Some(Ok(response.clone()));
            }
            Err(e) => {
                self.sync_errors += 1;
                self.last_sync_response = Some(Err(format!("Handler error: {}", e)));
            }
        }
    }

    fn finalize_started(&mut self, req: &SyncRequest) {
        self.finalize_count += 1;
        self.last_finalize_request = Some(req.clone());
    }

    fn finalize_finished(&mut self, resp: &Result<FinalizeResponse, handler::Error>) {
        match resp.as_ref() {
            Ok(response) => {
                self.last_finalize_response = Some(Ok(response.clone()));
            }
            Err(e) => {
                self.finalize_errors += 1;
                self.last_finalize_response = Some(Err(format!("Handler error: {}", e)));
            }
        }
    }

    fn has_error(&self) -> bool {
        self.sync_errors > 0 || self.finalize_errors > 0
    }
}

#[derive(Clone)]
struct InstrumentedHandler {
    wrapped: HandlerRef,
    records: Arc<RwLock<HashMap<ObjectId, SyncRecord>>>,
}

impl Debug for InstrumentedHandler {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("InstrumentedHandler")
    }
}

impl InstrumentedHandler {
    fn wrap(wrapped: impl Handler) -> (InstrumentedHandler, HandlerRef) {
        let handler_ref = Arc::new(wrapped);
        let handler = InstrumentedHandler {
            wrapped: handler_ref.clone(),
            records: Arc::new(RwLock::new(HashMap::new())),
        };

        (handler, handler_ref)
    }

    fn take_errors(&self) -> Result<(), HandlerErrors> {
        let mut records = self.take_records();
        records.retain(|_, record| record.has_error());
        if records.is_empty() {
            Ok(())
        } else {
            Err(HandlerErrors(records))
        }
    }

    fn take_records(&self) -> HashMap<ObjectId, SyncRecord> {
        let mut lock = self.records.write().unwrap();
        std::mem::replace(&mut *lock, HashMap::new())
    }

    fn reset(&self) {
        self.take_records();
    }

    fn total_invocation_count(&self) -> usize {
        let lock = self.records.read().unwrap();
        lock.values().map(|record| record.sync_count + record.finalize_count).sum()
    }
}

impl Handler for InstrumentedHandler {
    fn sync(&self, req: &SyncRequest) -> Result<SyncResponse, handler::Error> {
        let InstrumentedHandler { ref wrapped, ref records } = self;

        let parent_id = req.parent.get_object_id().into_owned();
        let mut records_lock = records.write().unwrap();
        let record = records_lock.entry(parent_id).or_default();
        record.sync_started(req);

        let result = wrapped.sync(req);
        record.sync_finished(&result);
        result
    }

    fn finalize(&self, req: &SyncRequest) -> Result<FinalizeResponse, handler::Error> {
        let InstrumentedHandler { ref wrapped, ref records } = self;

        let parent_id = req.parent.get_object_id().into_owned();
        let mut records_lock = records.write().unwrap();
        let record = records_lock.entry(parent_id).or_default();
        record.finalize_started(req);

        let result = wrapped.finalize(req);
        record.finalize_finished(&result);
        result
    }

}

async fn do_reconciliation_run(state: &mut OperatorState, parents_needing_sync: &mut HashSet<String>, handler: &HandlerRef, instrumented_handler: &InstrumentedHandler, max_timeout: Duration) -> Result<(), Error> {
    let mut timeout = max_timeout.min(Duration::from_millis(20));
    let start = Instant::now();
    let starting_invocation_count = instrumented_handler.total_invocation_count();
    let mut last_invocation_count = starting_invocation_count;
    while start.elapsed() < max_timeout && state.is_running() {
        state.run_once(parents_needing_sync, handler, timeout).await;
        let current_invocation_count = instrumented_handler.total_invocation_count();
        if state.is_any_update_in_progress() || current_invocation_count > last_invocation_count {
            last_invocation_count = current_invocation_count;
            timeout = max_timeout.checked_sub(start.elapsed()).unwrap_or(Duration::from_secs(0));
        } else {
            return Ok(());
        }
    }
    let records = instrumented_handler.take_records();
    Err(Box::new(ReconciliationIncompleteError {
        records,
        timeout: max_timeout,
    }) as Error)
}

#[derive(Debug)]
pub struct HandlerErrors(HashMap<ObjectId, SyncRecord>);

impl std::error::Error for HandlerErrors {}

impl Display for HandlerErrors {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("Handler Errors:")?;
        for (id, record) in self.0.iter() {
            write!(f, "\nParent id: {},\n SyncRecord:\n{:?}", id, record)?;
        }
        Ok(())
    }
}


/// Error representing an operator that never "settled" after repeated attempts to sync or finalize. This condition would manifest
/// as a "hot loop" in a production environment, and is typically caused by repeatedly returning a different value in a SyncResponse.
/// For example, if you always return a new timestamp in one of the values, then the resource would be continuously updated.
#[derive(Debug)]
pub struct ReconciliationIncompleteError {
    records: HashMap<ObjectId, SyncRecord>,
    timeout: Duration,
}

impl std::error::Error for ReconciliationIncompleteError {}

impl Display for ReconciliationIncompleteError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let total_invocations = self.records.values().map(|record| record.sync_count + record.finalize_count).sum::<usize>();
        let duration_ms = crate::runner::duration_to_millis(self.timeout);
        write!(f, "Operator reconciliation loop never stabilized after {} handler invocations in {}ms", total_invocations, duration_ms)
    }
}


#[derive(Serialize, Debug, Clone, PartialEq)]
pub struct Diff {
    expected: Value,
    actual: Value,
    different_paths: Vec<String>,
}

impl Display for Diff {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Diff: \n\texpected:\t {},\n\tactual:\t{},\n\tdifferent_paths:\t{:?}", self.expected, self.actual, self.different_paths)
    }
}

#[derive(Debug)]
pub struct MissingResource {
    pub k8s_type: K8sType,
    pub id: ObjectId,
}

impl Display for MissingResource {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Missing Resource: type: {}, id: {}", self.k8s_type, self.id)
    }
}
impl std::error::Error for MissingResource { }

async fn compare_child_objects(state: &OperatorState, k8s_type: &K8sType, id: &ObjectIdRef<'_>, expected: Value) -> Result<Option<Diff>, MissingResource> {
    let actual = get_object(state, k8s_type, id).await?;
    Ok(compare_resources(actual.into_value(), expected))
}

async fn get_object(state: &OperatorState, k8s_type: &K8sType, id: &ObjectIdRef<'_>) -> Result<K8sResource, MissingResource> {
    let children = state.children.get(k8s_type).expect("no configuration exists for the given K8sType");
    let lock = children.lock_state().await.expect("failed to acquire lock on cach state");
    match lock.get_by_id(id) {
        Some(res) => {
            Ok(res)
        }
        None => {
            Err(MissingResource {
                k8s_type: k8s_type.clone(),
                id: id.to_owned(),
            })
        }
    }
}

fn compare_resources(actual: Value, expected: Value) -> Option<Diff> {
    let diffs = compare::compare_values(&actual, &expected).into_vec();
    let paths: Vec<String> = diffs.into_iter().map(|diff| diff.path).collect();
    if paths.is_empty() {
        None
    } else {
        Some(Diff {
            expected: expected,
            actual: actual,
            different_paths: paths,
        })
    }
}

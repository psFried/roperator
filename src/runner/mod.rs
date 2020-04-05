mod client;
mod informer;
mod metrics;
pub(crate) mod reconcile;
pub(crate) mod resource_map;
mod server;

#[cfg(feature = "testkit")]
pub mod testkit;

#[cfg(feature = "testkit")]
use crate::resource::ObjectIdRef;

use crate::config::{ClientConfig, OperatorConfig, UpdateStrategy};
use crate::error::Error;
use crate::handler::{Handler, SyncRequest};
use crate::k8s_types::K8sType;
use crate::resource::{K8sResource, K8sTypeRef, ObjectId};
use crate::runner::informer::{
    EventType, LabelToIdIndex, ResourceMessage, ResourceMonitor, UidToIdIndex,
};
use crate::runner::reconcile::SyncHandler;
use backoff::{backoff::Backoff, ExponentialBackoff};
use client::Client;
use metrics::Metrics;

use tokio::executor::Executor;
use tokio::prelude::*;
use tokio::runtime::{Runtime, TaskExecutor};
use tokio::sync::mpsc::{Receiver, Sender};

use std::collections::{HashMap, HashSet};
use std::fmt::{self, Display};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// A handle to a potentially running operator, which allows for shutting it down
pub struct OperatorHandle {
    running: Arc<AtomicBool>,
}

impl std::ops::Drop for OperatorHandle {
    fn drop(&mut self) {
        self.running.store(false, Ordering::Relaxed);
    }
}

impl OperatorHandle {
    pub fn shutdown_now(self) {
        self.running.store(false, Ordering::Relaxed);
    }

    pub fn is_active(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }
}

#[derive(Debug)]
pub struct UnexpectedShutdownError;
impl Display for UnexpectedShutdownError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("Operator shutdown due to an unexpected error")
    }
}
impl std::error::Error for UnexpectedShutdownError {}

/// Starts the operator and blocks the current thread indefinitely until the operator shuts down due to an error.
pub fn run_operator(config: OperatorConfig, handler: impl Handler) -> Error {
    let client_config = {
        let user_agent = config.operator_name.as_str();
        let result = ClientConfig::from_service_account(user_agent).or_else(|_| {
            log::debug!("Failed to load ClientConfig from service account, so trying to load from kubeconfig");
            ClientConfig::from_kubeconfig(user_agent)
        });
        match result {
            Ok(conf) => conf,
            Err(err) => return err.into(),
        }
    };
    run_operator_with_client_config(config, client_config, handler)
}

/// Starts the operator and blocks the current thread indefinitely until the operator shuts down due to an error.
pub fn run_operator_with_client_config(
    config: OperatorConfig,
    client_config: ClientConfig,
    handler: impl Handler,
) -> Error {
    let handler = Arc::new(handler);
    let metrics = Metrics::new();
    let client = match Client::new(client_config, metrics.client_metrics()) {
        Ok(c) => c,
        Err(err) => return err.into(),
    };
    let runtime = match Runtime::new() {
        Ok(rt) => rt,
        Err(err) => return err.into(),
    };
    let running = Arc::new(AtomicBool::new(true));
    let executor = runtime.executor();
    runtime.block_on(async move {
        run_with_client(executor, metrics, running, config, client, handler).await;
    });
    log::warn!("Operator stopped, shutting down runtime");
    runtime.shutdown_now();
    // return an error here, since the operator will never exit under normal circumstances
    Box::new(UnexpectedShutdownError)
}

/// Starts the operator asynchronously using the provided runtime. This function will return immediately with a
/// handle that can be used to shutdown the operator at a later point. Will return an error if it fails to create
/// the http client due to invalid configuration.
pub fn start_operator_with_runtime(
    runtime: &Runtime,
    config: OperatorConfig,
    client_config: ClientConfig,
    handler: impl Handler,
) -> Result<OperatorHandle, Error> {
    let handler = Arc::new(handler);
    let metrics = Metrics::new();
    let client = Client::new(client_config, metrics.client_metrics())?;
    let running = Arc::new(AtomicBool::new(true));
    let handle = OperatorHandle {
        running: running.clone(),
    };
    let executor = runtime.executor();
    runtime.spawn(async move {
        run_with_client(executor, metrics, running.clone(), config, client, handler).await;
    });
    Ok(handle)
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ChildRuntimeConfig {
    update_strategy: UpdateStrategy,
    child_type: &'static K8sType,
}

#[derive(Debug)]
pub(crate) struct RuntimeConfig {
    pub metrics: Metrics,
    pub child_types: HashMap<&'static K8sType, ChildRuntimeConfig>,
    pub parent_type: &'static K8sType,
    pub correlation_label_name: String,
    pub controller_label_name: String,
    pub operator_name: String,
    pub max_error_backoff: Duration,
}

impl RuntimeConfig {
    pub(crate) fn type_for(&self, type_ref: &K8sTypeRef<'_>) -> Option<&'static K8sType> {
        self.child_types
            .values()
            .find(|conf| type_ref == conf.child_type)
            .map(|conf| conf.child_type)
    }

    pub(crate) fn get_child_config<'a>(
        &'a self,
        type_ref: &'_ K8sTypeRef<'_>,
    ) -> Option<&'a ChildRuntimeConfig> {
        self.type_for(type_ref)
            .and_then(|child_type| self.child_types.get(child_type))
    }
}

async fn run_with_client(
    executor: TaskExecutor,
    metrics: Metrics,
    running: Arc<AtomicBool>,
    config: OperatorConfig,
    client: Client,
    handler: Arc<dyn Handler>,
) {
    log::debug!("Starting operator with configuration: {:?}", config);
    let server_port = config.server_port;
    let expose_metrics = config.expose_metrics;
    let expose_health = config.expose_health;
    let mut state = create_operator_state(executor.clone(), metrics, running, config, client).await;
    if expose_metrics || expose_health {
        let server_future = server::start(
            executor,
            server_port,
            state.runtime_config.clone(),
            expose_metrics,
            expose_health,
        );
        let operator_future = state.run(handler);
        futures_util::future::join(server_future, operator_future).await;
    } else {
        state.run(handler).await;
    }
}

async fn create_operator_state<T: Executor>(
    mut executor: T,
    metrics: Metrics,
    running: Arc<AtomicBool>,
    config: OperatorConfig,
    client: Client,
) -> OperatorState<T> {
    let OperatorConfig {
        parent,
        child_types,
        namespace,
        operator_name,
        tracking_label_name,
        ownership_label_name,
        max_error_backoff,
        ..
    } = config;

    let (tx, rx) = tokio::sync::mpsc::channel::<ResourceMessage>(1024);

    let parent_metrics = metrics.watcher_metrics(parent);
    let parent_monitor = informer::start_parent_monitor(
        &mut executor,
        namespace.clone(),
        parent,
        client.clone(),
        tx.clone(),
        parent_metrics,
    );

    let mut child_runtime_config = HashMap::with_capacity(4);
    let mut children = HashMap::with_capacity(4);

    for (child_type, child_conf) in child_types {
        let child_metrics = metrics.watcher_metrics(&child_type);
        let runtime_conf = ChildRuntimeConfig {
            child_type,
            update_strategy: child_conf.update_strategy,
        };
        child_runtime_config.insert(child_type, runtime_conf);
        let child_monitor = informer::start_child_monitor(
            &mut executor,
            tracking_label_name.clone(),
            namespace.clone(),
            child_type,
            client.clone(),
            tx.clone(),
            child_metrics,
        );
        children.insert(child_type, child_monitor);
    }
    let runtime_config = Arc::new(RuntimeConfig {
        metrics,
        child_types: child_runtime_config,
        parent_type: parent,
        correlation_label_name: tracking_label_name,
        controller_label_name: ownership_label_name,
        operator_name,
        max_error_backoff,
    });

    OperatorState {
        running,
        parents: parent_monitor,
        children,
        sender: tx,
        receiver: rx,
        parent_states: HashMap::new(),
        client,
        runtime_config,
        executor,
    }
}

type HandlerRef = Arc<dyn Handler>;

#[derive(Debug)]
struct InProgressUpdate {
    start_time: Instant,
}

#[derive(Debug)]
struct CappedBackoff(ExponentialBackoff);

impl Backoff for CappedBackoff {
    fn next_backoff(&mut self) -> Option<Duration> {
        let CappedBackoff(exp) = self;
        exp.next_backoff()
    }
    fn reset(&mut self) {
        let CappedBackoff(exp) = self;
        exp.reset()
    }
}

impl Default for CappedBackoff {
    fn default() -> Self {
        let mut sync_timer = ExponentialBackoff {
            initial_interval: Duration::from_millis(100),
            max_elapsed_time: None,
            ..Default::default()
        };
        sync_timer.reset();

        CappedBackoff(sync_timer)
    }
}

impl CappedBackoff {
    fn new(max_backoff: Duration) -> CappedBackoff {
        CappedBackoff(ExponentialBackoff {
            initial_interval: Duration::from_millis(100),
            max_elapsed_time: None,
            max_interval: max_backoff,
            ..Default::default()
        })
    }
}

struct Resync(Duration, u32);

#[derive(Debug, Default)]
struct ParentState {
    in_progress: Option<InProgressUpdate>,
    sync_counter: u32,
    sync_timer: CappedBackoff,
}

impl ParentState {

    fn new(backoff: CappedBackoff) -> ParentState {
        ParentState {
            in_progress: None,
            sync_counter: 0,
            sync_timer: backoff,
        }

    }

    fn start_sync(&mut self) {
        self.sync_counter += 1;
        self.in_progress = Some(InProgressUpdate {
            start_time: Instant::now(),
        })
    }

    fn sync_finished(&mut self, parent_id: &ObjectId, parent_uid: &str, sync_result: Result<Option<Duration>, ()>) -> Option<Resync> {
        if let Some(in_progress) = self.in_progress.take() {
            let duration_millis = duration_to_millis(in_progress.start_time.elapsed());
            let needs_resync = sync_result.as_ref().map(Option::is_some).unwrap_or(true);
            log::info!(
                "Completed sync of parent: {} with uid: {} in {}ms, needs retry: {}",
                parent_id,
                parent_uid,
                duration_millis,
                needs_resync,
            );
            let sync_count = self.sync_counter;

            match sync_result {
                Ok(resync) => {
                    self.sync_timer.reset();
                    resync.map(|duration| Resync(duration, sync_count))
                }
                Err(()) => {
                    self.sync_timer.next_backoff().map(|duration| Resync(duration, sync_count))
                }
            }

        } else {
            log::error!("Got updateOperationComplete when there was no in-progress operation for uid: {}", parent_uid);
            None
        }
    }

    fn is_update_in_progress(&self) -> bool {
        self.in_progress.is_some()
    }
}

#[derive(Debug)]
struct OperatorState<T: Executor> {
    running: Arc<AtomicBool>,
    parents: ResourceMonitor<UidToIdIndex>,
    children: HashMap<&'static K8sType, ResourceMonitor<LabelToIdIndex>>,
    sender: Sender<ResourceMessage>,
    receiver: Receiver<ResourceMessage>,
    parent_states: HashMap<String, ParentState>,
    client: Client,
    runtime_config: Arc<RuntimeConfig>,
    executor: T,
}

impl<T: Executor> OperatorState<T> {
    async fn run(&mut self, handler: HandlerRef) {
        let mut parent_ids_to_sync = HashSet::with_capacity(16);
        while self.running.load(Ordering::Relaxed) {
            let timeout = if parent_ids_to_sync.is_empty() {
                Duration::from_secs(3600)
            } else {
                Duration::from_secs(1)
            };
            self.run_once(&mut parent_ids_to_sync, &handler, timeout)
                .await;
        }
        log::info!("Shutting down operator");
    }

    async fn run_once(
        &mut self,
        parent_ids_to_sync: &mut HashSet<String>,
        handler: &HandlerRef,
        timeout: Duration,
    ) {
        log::debug!(
            "Starting sync loop with {} existing parents needing to sync",
            parent_ids_to_sync.len()
        );
        self.get_parent_uids_to_update(parent_ids_to_sync, timeout)
            .await;
        if !self.running.load(Ordering::Relaxed) {
            // getting the uids to update can take quite a while, so we'll do an extra check to see
            // if the operator has been shutdown in the meantime
            return;
        }

        let mut synced_parents = Vec::new();
        for parent_uid in parent_ids_to_sync.iter() {
            if !self.is_update_in_progress(parent_uid) {
                let result = self.sync_parent(parent_uid.as_str(), handler.clone()).await;
                if let Err(err) = result {
                    log::error!(
                        "Cannot sync parent with uid: {} due to error: {:?}",
                        parent_uid,
                        err
                    );
                } else {
                    synced_parents.push(parent_uid.clone());
                }
            }
        }

        for id in synced_parents {
            parent_ids_to_sync.remove(&id);
        }
    }

    #[cfg(feature = "testkit")]
    fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    #[cfg(feature = "testkit")]
    fn is_any_update_in_progress(&self) -> bool {
        self.parent_states
            .values()
            .any(ParentState::is_update_in_progress)
    }

    fn is_update_in_progress(&self, parent_uid: &str) -> bool {
        self.parent_states
            .get(parent_uid)
            .map(ParentState::is_update_in_progress)
            .unwrap_or(false)
    }

    async fn sync_parent(&mut self, parent_uid: &str, handler: HandlerRef) -> Result<(), Error> {
        let parent = match self.get_parent(parent_uid).await? {
            Some(p) => p,
            None => {
                log::info!("Cannot sync parent with uid: '{}' because resource has been subsequently deleted", parent_uid);
                return Ok(());
            }
        };

        log::info!(
            "Starting sync request for parent: '{}' with uid: '{}'",
            parent.get_object_id(),
            parent.uid()
        );

        let request = self.create_sync_request(parent).await?;

        let parent_state = self.get_or_create_parent_state(parent_uid);
        parent_state.start_sync();

        let handler = SyncHandler {
            sender: self.sender.clone(),
            request,
            handler: handler.clone(),
            client: self.client.clone(),
            runtime_config: self.runtime_config.clone(),
            parent_index_key: parent_uid.to_owned(),
        };
        handler.start_sync();
        Ok(())
    }

    fn get_or_create_parent_state<'a, 'b>(&'a mut self, parent_uid: &'b str) -> &'a mut ParentState {
        if !self.parent_states.contains_key(parent_uid) {
            let parent_state = ParentState::new(CappedBackoff::new(self.runtime_config.max_error_backoff));
            self.parent_states.insert(parent_uid.to_owned(), parent_state);
        }
        self.parent_states.get_mut(parent_uid).unwrap()
    }

    async fn create_sync_request(&self, parent: K8sResource) -> Result<SyncRequest, Error> {
        let children = self.get_all_children(parent.uid()).await?;
        Ok(SyncRequest { parent, children })
    }

    #[cfg(feature = "testkit")]
    async fn get_parent_by_id(
        &self,
        parent_id: &ObjectIdRef<'_>,
    ) -> Result<Option<K8sResource>, Error> {
        let parent_lock = self.parents.lock_state().await?;
        Ok(parent_lock.get_by_id(parent_id))
    }

    async fn get_parent(&self, parent_uid: &str) -> Result<Option<K8sResource>, Error> {
        let parent_lock = self.parents.lock_state().await?;
        Ok(parent_lock.get_by_uid(parent_uid))
    }

    async fn get_all_children(&self, parent_uid: &str) -> Result<Vec<K8sResource>, Error> {
        let mut request_children = Vec::with_capacity(8);

        for children_monitor in self.children.values() {
            let lock = children_monitor.lock_state().await?;
            let kids_of_type = lock.get_all_resources_by_index_key(parent_uid);
            request_children.extend(kids_of_type);
        }

        Ok(request_children)
    }

    /// Tries to receive a whole batch of messages, so that we can consolidate them by parent id.
    /// The `max_timeout` is treated as a soft limit, which may be exceeded by a bit in case there are
    /// tons of messages to process.
    async fn get_parent_uids_to_update(
        &mut self,
        to_sync: &mut HashSet<String>,
        max_timeout: Duration,
    ) {
        let starting_to_sync_len = to_sync.len();
        let start_time = Instant::now();
        let mut first_receive_time = start_time;
        // the initial timeout will be pretty long, but as soon as we receive the first message
        // we'll start to use a much shorter timeout for receiving subsequent messages
        let mut timeout = max_timeout;
        let mut total_messages: usize = 0;

        while let Some(message) = self.recv_next(timeout).await {
            if total_messages == 0 {
                first_receive_time = Instant::now();
            }
            total_messages += 1;
            log::trace!("Received: {:?}", message);
            self.handle_received_message(message, to_sync);

            // if we've been receiving messages for a while, then we'll use a super short timeout so that
            // we can start syncing as soon as possible
            let elapsed_since_first_recv = first_receive_time.elapsed();
            let new_timeout = if (total_messages > 0)
                && (elapsed_since_first_recv > Duration::from_millis(500))
            {
                Duration::from_millis(1)
            } else {
                max_timeout
                    .checked_sub(start_time.elapsed())
                    .unwrap_or(Duration::from_millis(1))
                    .min(Duration::from_millis(500)) // clamp to 500ms since we've already started receiving
                    .min(max_timeout) // clamp to max_timeout just in case that value was already very short
            };
            timeout = new_timeout;
        }
        let elapsed_millis = duration_to_millis(start_time.elapsed());
        let new_to_sync = to_sync.len() - starting_to_sync_len;
        log::debug!(
            "Received: {} messages to sync {} new parents in {}ms",
            total_messages,
            new_to_sync,
            elapsed_millis
        );
    }

    fn handle_received_message(&mut self, message: ResourceMessage, to_sync: &mut HashSet<String>) {
        self.runtime_config.metrics.watch_event_received();
        if message.index_key.is_none() {
            // TODO: change resourceMessage so that index_key is not an Option
            log::error!("Received a message with no index_key: {:?}", message);
            return;
        }
        let ResourceMessage {
            index_key,
            event_type,
            resource_type,
            resource_id,
        } = message;
        let uid = index_key.unwrap();
        match event_type {
            EventType::UpdateOperationComplete { result } => {
                // sanity check to ensure that there was actually an update in progress
                // if not, then we'll log the error and ignore this message, since this indicates
                // that there's a bug in roperator
                let maybe_resync = if let Some(parent_state) = self.parent_states.get_mut(&uid) {
                    parent_state.sync_finished(&resource_id, &uid, result)
                } else {
                    None
                };
                if let Some(Resync(duration, sync_counter)) = maybe_resync {
                    self.schedule_resync(&uid, resource_id, duration, sync_counter);
                }
            }
            EventType::Deleted if resource_type == self.runtime_config.parent_type => {
                log::debug!("Parent resource '{}' has been deleted", resource_id);
                self.runtime_config
                    .metrics
                    .parent_deleted(&resource_id.as_id_ref());
                let _ = self.parent_states.remove(&uid);
            }
            EventType::TriggerResync { resync_round } => {
                let current = self
                    .parent_states
                    .get(&uid)
                    .map(|ps| ps.sync_counter)
                    .unwrap_or(0);
                if resync_round == current {
                    if to_sync.insert(uid) {
                        log::debug!("triggering scheduled resync for parent: {}", resource_id);
                    } else {
                        log::debug!("skipping scheduled resync for parent: {} because it was already triggered by something else", resource_id);
                    }
                } else {
                    log::debug!("Skipping scheduled resync for parent: {} because a sync was already completed since this was scheduled", resource_id);
                }
            }
            _ => {
                if to_sync.insert(uid) {
                    log::info!(
                        "Triggering sync due to event: {:?}, on resource: {} {} ",
                        event_type,
                        resource_type,
                        resource_id
                    );
                }
            }
        }
    }

    fn schedule_resync(&mut self, uid: &str, parent_id: ObjectId, duration: Duration, sync_counter: u32) {
        let mut sender = self.sender.clone();
        log::trace!(
            "scheduling resync for parent: {} with sync_counter: {} for {}ms in the future",
            parent_id,
            sync_counter,
            duration_to_millis(duration)
        );
        let resource_type = self.runtime_config.parent_type;
        let index_key = Some(uid.to_owned());

        self.executor
            .spawn(Box::pin(async move {
                tokio::timer::delay_for(duration).await;
                log::trace!(
                    "sending resync message for parent: {} with sync_counter: {}",
                    parent_id,
                    sync_counter
                );
                let message = ResourceMessage {
                    event_type: EventType::TriggerResync {
                        resync_round: sync_counter,
                    },
                    resource_type,
                    resource_id: parent_id,
                    index_key,
                };
                if sender.send(message).await.is_err() {
                    log::warn!("Unable to send resync message");
                }
            }))
            .expect("fatal error: failed to spawn a task to schedule a resync");
    }

    async fn recv_next(&mut self, timeout: Duration) -> Option<ResourceMessage> {
        match self.receiver.recv().timeout(timeout).await {
            Err(_) => None,
            Ok(Some(val)) => Some(val),
            Ok(None) => {
                log::warn!("All informers have stopped, stopping operator");
                self.running.store(false, Ordering::Relaxed);
                None
            }
        }
    }
}

pub(crate) fn duration_to_millis(duration: Duration) -> u64 {
    let mut millis = duration.as_secs() * 1000;
    let nanos = duration.subsec_nanos() as u64;
    if nanos > 1_000_000 {
        millis = millis.saturating_add(nanos / 1_000_000)
    }
    millis
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn parent_state_backoff_increases_exponentially() {
        let parent_id = ObjectId::new("foo".to_owned(), "bar".to_owned());
        let parent_uid = "test-uid";
        let max_backoff = Duration::from_secs(10);

        // we set this up to disable the jitter for the test, so we can reliably assert that
        // it gets bigger after each error
        let backoff = CappedBackoff(ExponentialBackoff {
            initial_interval: Duration::from_millis(100),
            randomization_factor: 0.0,
            max_interval: max_backoff,
            max_elapsed_time: None,
            ..Default::default()
        });
        let mut subject = ParentState::new(backoff);

        let mut last_duration = Duration::from_secs(0);
        for i in 1..20 {
            subject.start_sync();
            let result = subject.sync_finished(&parent_id, parent_uid, Err(()));
            let Resync(duration, counter) = result.expect("expected result to be Some but it was None");

            assert_eq!(i, counter);
            assert!(duration >= last_duration);
            assert!(duration <= max_backoff);
            last_duration = duration;
        }
        assert_eq!(last_duration, max_backoff);
    }

    #[test]
    fn parent_state_backoff_is_reset_after_successful_sync() {
        let parent_id = ObjectId::new("foo".to_owned(), "bar".to_owned());
        let parent_uid = "test-uid";
        let max_backoff = Duration::from_secs(10);

        let mut subject = ParentState::new(CappedBackoff::new(max_backoff));
        let mut last_duration = Duration::from_secs(0);
        for _ in 0..10 {
            subject.start_sync();
            let result = subject.sync_finished(&parent_id, parent_uid, Err(()));
            let Resync(duration, _) = result.expect("expected result to be Some but it was None");
            last_duration = duration;
        }

        subject.start_sync();
        let result = subject.sync_finished(&parent_id, parent_uid, Ok(None));
        assert!(result.is_none());

        subject.start_sync();
        let Resync(duration, counter) = subject.sync_finished(&parent_id, parent_uid, Err(()))
                .expect("expected result to be Some but it was None");

        // the duration should have started incrementing from the beginning
        assert!(duration < last_duration);
        assert_eq!(12, counter);
    }

    #[test]
    fn parent_state_returns_resync_when_duration_is_present_in_sync_result() {
        let parent_id = ObjectId::new("foo".to_owned(), "bar".to_owned());
        let parent_uid = "test-uid";

        let mut subject = ParentState::new(CappedBackoff::new(Duration::from_secs(10)));

        let desired_period = Duration::from_secs(42);
        subject.start_sync();
        let Resync(duration, _) = subject.sync_finished(&parent_id, parent_uid, Ok(Some(desired_period)))
                .expect("expected result to be a Resync but was None");

        assert_eq!(desired_period, duration);
    }

}

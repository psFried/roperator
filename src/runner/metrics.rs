use crate::resource::ObjectIdRef;
use crate::k8s_types::K8sType;

use prometheus::{
    exponential_buckets,
    Registry,
    Histogram,
    HistogramOpts,
    IntCounterVec,
    IntGaugeVec,
    IntCounter,
    Opts,
    IntGauge,
};

use std::fmt::{self, Debug};

pub struct Metrics {
    registry: Registry,
    api_server_request_times: Histogram,
    total_watch_events_received: IntCounter,
    sync_count_by_parent: IntCounterVec,
    sync_errors_by_parent: IntCounterVec,
    resources_by_type: IntGaugeVec,
    watcher_requests_by_type: IntCounterVec,
    watcher_errors_by_type: IntCounterVec,
    watch_events_by_type: IntCounterVec,
}

impl Debug for Metrics {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("Metrics")
    }
}


fn id_labels<'a, 'b>(id: &'a ObjectIdRef<'b>) -> [&'a str; 2] {
    [id.as_parts().0.as_ref(), id.as_parts().1.as_ref()]
}

// 5, 10, 20, 40, 80, 160, 320, 640, 1280, 2560, 5120

const NAMESPACE_AND_NAME: &[&str] = &["namespace", "name"];
const API_VERSION_AND_KIND: &[&str] = &["apiVersion", "kind"];


impl Metrics {
    pub fn new() -> Metrics {
        let registry = Registry::new();

        let request_time_opts = HistogramOpts::new("api_server_request_time",
                "Total time from sending the request to receiving the response headers")
                .subsystem("client")
                .buckets(exponential_buckets(0.005, 2.0, 12).unwrap());
        let api_server_request_times = Histogram::with_opts(request_time_opts).unwrap();
        registry.register(Box::new(api_server_request_times.clone())).unwrap();

        let watch_events_opts = Opts::new("events_received", "total number of events processed by the operator, including from watches and initial seeds");
        let total_watch_events_received = IntCounter::with_opts(watch_events_opts).unwrap();
        registry.register(Box::new(total_watch_events_received.clone())).unwrap();

        let sync_count_opts = Opts::new("sync_counts", "the number of times each parent has been synced")
            .variable_label("namespace").variable_label("name");
        let sync_count_by_parent = IntCounterVec::new(sync_count_opts, NAMESPACE_AND_NAME).unwrap();
        registry.register(Box::new(sync_count_by_parent.clone())).unwrap();

        let sync_error_opts = Opts::new("sync_errors", "the number of errors during sync by parent")
                .variable_label("namespace").variable_label("name");
        let sync_errors_by_parent = IntCounterVec::new(sync_error_opts, NAMESPACE_AND_NAME).unwrap();
        registry.register(Box::new(sync_errors_by_parent.clone())).unwrap();

        let resource_count_opts = Opts::new("cached_resources", "number of resources in the in-memory cache")
                .variable_label("apiVersion").variable_label("kind");
        let resources_by_type = IntGaugeVec::new(resource_count_opts, API_VERSION_AND_KIND).unwrap();
        registry.register(Box::new(resources_by_type.clone())).unwrap();

        let watcher_request_opts = Opts::new("watcher_requests", "number of requests from watchers")
                .variable_label("apiVersion").variable_label("kind");
        let watcher_requests_by_type = IntCounterVec::new(watcher_request_opts, API_VERSION_AND_KIND).unwrap();
        registry.register(Box::new(watcher_requests_by_type.clone())).unwrap();

        let watcher_error_opts = Opts::new("watcher_errors", "number of errors from watchers")
                .variable_label("apiVersion").variable_label("kind");
        let watcher_errors_by_type = IntCounterVec::new(watcher_error_opts, API_VERSION_AND_KIND).unwrap();
        registry.register(Box::new(watcher_errors_by_type.clone())).unwrap();

        let watcher_event_opts = Opts::new("watch_events", "number of watch events received by watchers")
                .variable_label("apiVersion").variable_label("kind");
        let watch_events_by_type = IntCounterVec::new(watcher_event_opts, API_VERSION_AND_KIND).unwrap();
        registry.register(Box::new(watch_events_by_type.clone())).unwrap();

        Metrics {
            registry,
            api_server_request_times,
            total_watch_events_received,
            sync_count_by_parent,
            sync_errors_by_parent,
            resources_by_type,
            watcher_requests_by_type,
            watcher_errors_by_type,
            watch_events_by_type,
        }
    }

    pub fn client_metrics(&self) -> ClientMetrics {
        ClientMetrics {
            api_server_request_times: self.api_server_request_times.clone(),
        }
    }

    pub fn watcher_metrics(&self, k8s_type: &K8sType) -> WatcherMetrics {
        let api_version = k8s_type.format_api_version();
        let labels = &[api_version.as_str(), k8s_type.kind];
        WatcherMetrics {
            watcher_requests: self.watcher_requests_by_type.with_label_values(labels),
            watcher_errors: self.watcher_errors_by_type.with_label_values(labels),
            watch_events: self.watch_events_by_type.with_label_values(labels),
            resource_count: self.resources_by_type.with_label_values(labels),
        }
    }

    pub fn parent_deleted(&self, id: &ObjectIdRef<'_>) {
        let labels = id_labels(id);
        let _ = self.sync_count_by_parent.remove_label_values(&labels);
        let _ = self.sync_errors_by_parent.remove_label_values(&labels);
    }

    pub fn watch_event_received(&self) {
        self.total_watch_events_received.inc();
    }

    pub fn parent_sync_started(&self, id: &ObjectIdRef<'_>) {
        let labels = id_labels(id);
        self.sync_count_by_parent.with_label_values(&labels).inc();
    }

    pub fn parent_sync_error(&self, id: &ObjectIdRef<'_>) {
        self.sync_errors_by_parent.with_label_values(&id_labels(id)).inc();
    }

    pub fn encode_as_text(&self) -> Result<Vec<u8>, prometheus::Error> {
        use prometheus::Encoder;
        let encoder = prometheus::TextEncoder::new();
        let mut buffer = Vec::with_capacity(4096);
        encoder.encode(self.registry.gather().as_slice(), &mut buffer)?;
        Ok(buffer)
    }
}

pub struct ClientMetrics {
    api_server_request_times: Histogram,
}

impl Debug for ClientMetrics {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("ClientMetrics")
    }
}
impl ClientMetrics {
    pub fn request_started(&self) -> prometheus::HistogramTimer {
        self.api_server_request_times.start_timer()
    }
}

pub struct WatcherMetrics {
    watcher_requests: IntCounter,
    watcher_errors: IntCounter,
    watch_events: IntCounter,
    resource_count: IntGauge,
}
impl Debug for WatcherMetrics {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("WatcherMetrics")
    }
}

impl WatcherMetrics {
    pub fn set_resource_count(&self, count: usize) {
        self.resource_count.set(count as i64);
    }

    pub fn request_started(&self) {
        self.watcher_requests.inc();
    }

    pub fn event_received(&self) {
        self.watch_events.inc();
    }

    pub fn error(&self) {
        self.watcher_errors.inc();
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn metrics_are_created_successfully() {
        let _metrics = Metrics::new();
    }
}

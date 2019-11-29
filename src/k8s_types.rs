use crate::resource::K8sTypeRef;

use std::fmt::{self, Display};
use std::hash::{self, Hash};

/// A basic description of a Kubernetes resource, with just enough information to allow Roperator
/// to communicate with the api server. We use `&'static str` for all of these so that it's easy
/// to pass references around without copying. You can define your own k8s types simply by declaring
/// a static, like:
///
/// ```no_run
/// use roperator::k8s_types::K8sType;
///
/// #[allow(non_upper_case_globals)]
/// pub static MyCrd: &K8sType = &K8sType {
///     api_version: "example.com/v1",
///     kind: "MyCrd",
///     plural_kind: "mycrds"
/// };
/// ```
///
/// If for some reason you need to create `K8sType`s at runtime, then you can use a string internment library
/// like `string_cache` or else you can use the `define_type` function.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct K8sType {
    pub api_version: &'static str,
    pub kind: &'static str,
    pub plural_kind: &'static str,
}

impl Hash for K8sType {
    fn hash<H: hash::Hasher>(&self, hasher: &mut H) {
        self.api_version.hash(hasher);
        self.kind.hash(hasher);
    }
}


/// Creates a `&'static K8sType` at runtime **by leaking memory**. This is totally fine, as long as it's only
/// done once on application startup, but you definitely want to avoid repeated calls to define the same type.
pub fn define_type(api_version: String, kind: String, plural_kind: String) -> &'static K8sType {
    fn leak_str(s: String) -> &'static str {
        Box::leak(s.into_boxed_str())
    }

    let k8s_type = K8sType {
        api_version: leak_str(api_version),
        kind: leak_str(kind),
        plural_kind: leak_str(plural_kind),
    };
    log::info!("Dynamically defining {:?}", k8s_type);
    Box::leak(Box::new(k8s_type))
}

impl K8sType {

    pub fn as_group_and_version(&self) -> (&str, &str) {
        // TODO: validate the apiVersion string and panic with a helpful message if it's wrong
        match self.api_version.find('/') {
            Some(slash_idx) => {
                (&self.api_version[..slash_idx], &self.api_version[(slash_idx + 1)..])
            }
            None => {
                ("", self.api_version)
            }
        }
    }

    pub fn group(&self) -> &str {
        self.as_group_and_version().0
    }

    pub fn version(&self) -> &str {
        self.as_group_and_version().1
    }

    pub fn to_type_ref(&self) -> K8sTypeRef<'static> {
        K8sTypeRef(self.api_version, self.kind)
    }

}

impl Display for K8sType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}/{}", self.api_version, self.plural_kind)
    }
}

macro_rules! k8s_type {
    ($ref_name:ident, $api_version:expr, $kind:expr, $plural_kind:expr) => {
        #[allow(non_upper_case_globals)]
        pub static $ref_name: &crate::k8s_types::K8sType = &crate::k8s_types::K8sType {
            api_version: $api_version,
            kind: $kind,
            plural_kind: $plural_kind,
        };
    };
    ($ref_name:ident, core, v1, $kind:expr, $plural_kind:expr) => {
        k8s_type!($ref_name, "v1", $kind, $plural_kind)
    };
}

macro_rules! def_types {
    (@nogroupmod, $group:expr, [
        $( $version:ident => [
            $( $kind:ident ~ $plural_kind:ident ),*
        ]),*
    ]) => {
        $(
            pub mod $version {

                $(
                    k8s_type!($kind, concat!($group, "/", stringify!($version)), stringify!($kind), stringify!($plural_kind));
                )*
            }

        )*
    };
    ($group:ident => $rem:tt ) => {

        pub mod $group {

            def_types!{@nogroupmod, stringify!($group), $rem }
        }

    };
    (@core => [
        $( $version:ident => [
            $( $kind:ident ~ $plural_kind:ident ),*
        ]),*
    ]) => {
        pub mod core {
            $(pub mod $version {
                $(
                    k8s_type!($kind, stringify!($version), stringify!($kind), stringify!($plural_kind));
                )*
            })*
        }
    }
}

def_types!{
    @core => [
        v1 => [
            Namespace ~ namespaces,
            Node ~ nodes,
            Pod ~ pods,
            PodTemplate ~ podtemplates,
            ReplicationController ~ replicationcontrollers,
            Event ~ events,
            Service ~ services,
            Endpoints ~ endpoints,
            ComponentStatus ~ componentstatuses,
            Secret ~ secrets,
            ConfigMap ~ configmaps,
            LimitRange ~ limitranges,
            PersistentVolumeClaim ~ persistentvolumeclaims,
            PersistentVolume ~ persistentvolumes,
            ResourceQuota ~ resourcequotas,
            Binding ~ bindings,
            ServiceAccount ~ serviceaccounts
        ]
    ]
}

pub mod admissionregistration_k8s_io {
    def_types!{
        @nogroupmod, "admissionregistration.k8s.io", [
            v1beta1 => [
                MutatingWebhookConfiguration ~ mutatingwebhookconfigurations,
                ValidatingWebhookConfiguration ~ validatingwebhookconfigurations
            ]
        ]
    }
}

pub mod apiextensions_k8s_io {
    def_types!{
        @nogroupmod, "apiextensions.k8s.io", [
            v1beta1 => [
                CustomResourceDefinition ~ customresourcedefinitions
            ]
        ]
    }
}

pub mod apiregistration_k8s_io {
    def_types!{
        @nogroupmod, "apiregistration.k8s.io", [
            v1 => [
                APIService ~ apiservices
            ]
        ]
    }
}

def_types!{
    apps => [
        v1 => [
            ControllerRevision ~ controllerrevisions,
            DaemonSet ~ daemonsets,
            Deployment ~ deployments,
            ReplicaSet ~ replicasets
        ]
    ]
}

def_types!{
    autoscaling => [
        v1 => [
                HorizontalPodAutoscaler ~ horizontalpodautoscalers
        ]
    ]
}

pub mod authentication_k8s_io {
    def_types!{
        @nogroupmod, "authentication.k8s.io", [
            v1 => [
                TokenReview ~ tokenreviews
            ]
        ]
    }
}

pub mod authorization_k8s_io {
    def_types!{
        @nogroupmod, "authorization.k8s.io", [
            v1 => [
                LocalSubjectAccessReview ~ localsubjectaccessreviews,
                SelfSubjectAccessReview ~ selfsubjectaccessreviews,
                SelfSubjectRulesReview ~ selfsubjectrulesreviews,
                SubjectAccessReview ~ subjectaccessreviews
            ]
        ]
    }
}

def_types!{
    batch => [
        v1 => [
            CronJob ~ cronjobs,
            Job ~ jobs
        ]
    ]
}

pub mod certificates_k8s_io {
    def_types!{
        @nogroupmod, "certificates.k8s.io", [
            v1beta1 => [
                CertificateSigningRequest ~ certificatesigningrequests
            ]
        ]
    }
}

pub mod coordination_k8s_io {
    def_types!{
        @nogroupmod, "coordination.k8s.io", [
            v1 => [
                Lease ~ leases
            ]
        ]
    }
}

pub mod extensions {
    def_types!{
        @nogroupmod, "extensions", [
            v1beta1 => [
                DaemonSet ~ daemonsets,
                Deployment ~ deployments,
                Ingress ~ ingresses,
                NetworkPolicy ~ networkpolicies,
                PodSecurityPolicy ~ podsecuritypolicies,
                ReplicaSet ~ replicasets
            ]
        ]
    }
}

pub mod node_k8s_io {
    def_types!{
        @nogroupmod, "node.k8s.io", [
            v1beta1 => [
                RuntimeClass ~ runtimeclasses
            ]
        ]
    }
}

pub mod policy {
    def_types!{
        @nogroupmod, "policy", [
            v1beta1 => [
                PodDisruptionBudget ~ poddisruptionbudgets,
                PodSecurityPolicy ~ podsecuritypolicies
            ]
        ]
    }
}

pub mod rbac_authorization_k8s_io {
    def_types!{
        @nogroupmod, "rbac.authorization.k8s.io", [
            v1 => [
                ClusterRoleBinding ~ clusterrolebindings,
                ClusterRole ~ clusterroles,
                RoleBinding ~ rolebindings,
                Role ~ roles
            ]
        ]
    }
}

pub mod scheduling_k8s_io {
    def_types!{
        @nogroupmod, "scheduling.k8s.io", [
            v1 => [
                PriorityClass ~ priorityclasses
            ]
        ]
    }
}

pub mod storage_k8s_io {
    def_types!{
        @nogroupmod, "storage.k8s.io", [
            v1 => [
                CSIDriver ~ csidrivers,
                CSINode ~ csinodes,
                StorageClass ~ storageclasses,
                VolumeAttachment ~ volumeattachments
            ]
        ]
    }
}


#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn k8s_type_returns_group_and_api_version_when_both_are_present() {
        let subject = storage_k8s_io::v1::CSIDriver;
        assert_eq!("storage.k8s.io", subject.group());
        assert_eq!("v1", subject.version());
    }

    #[test]
    fn k8s_type_returns_empty_str_for_group_when_no_group_is_present() {
        let subject = core::v1::Pod;
        assert_eq!("", subject.group());
        assert_eq!("v1", subject.version());
    }
}


use crate::resource::K8sTypeRef;

use std::fmt::{self, Display};

/// A basic description of a Kubernetes resource, with just enough information to allow Roperator
/// to communicate with the api server. We use `&'static str` for all of these so that it's easy
/// to pass references around without copying. You can define your own k8s types simply by declaring
/// a static, like:
///
/// ```no_run
/// use roperator::k8s_types::K8sType;
///
/// pub static MYCrd: &K8sType = &K8sType {
///     group: "example.com",
///     version: "v1",
///     kind: "MyCrd",
///     plural_kind: "mycrds"
/// };
/// ```
///
/// If for some reason you need to create `K8sType`s at runtime, then you can use a string internment library
/// or else you can use `Box::leak` to create static strings at runtime by leaking a `String`. Of course you should
/// only use that second method cautiously when you know that you won't be repeatedly creating new K8sTypes.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct K8sType {
    pub group: &'static str,
    pub version: &'static str,
    pub kind: &'static str,
    pub plural_kind: &'static str,
}

impl K8sType {

    pub fn to_type_ref(&self) -> K8sTypeRef<'static> {
        K8sTypeRef::new(self.format_api_version(), self.kind)
    }

    pub fn format_api_version(&self) -> String {
        if self.group.is_empty() {
            self.version.to_string()
        } else {
            format!("{}/{}", self.group, self.version)
        }
    }
}

impl Display for K8sType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.group.is_empty() {
            write!(f, "{}/{}", self.version, self.plural_kind)
        } else {
            write!(f, "{}/{}/{}", self.group, self.version, self.plural_kind)
        }
    }
}

macro_rules! k8s_type {
    ($ref_name:ident, $group:expr, $version:expr, $kind:expr, $plural_kind:expr) => {
        #[allow(non_upper_case_globals)]
        pub static $ref_name: &crate::k8s_types::K8sType = &crate::k8s_types::K8sType {
            group: $group,
            version: $version,
            kind: $kind,
            plural_kind: $plural_kind,
        };
    };
    ($ref_name:ident, core, v1, $kind:expr, $plural_kind:expr) => {
        k8s_type!($ref_name, "", "v1", $kind, $plural_kind)
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
                    k8s_type!($kind, $group, stringify!($version), stringify!($kind), stringify!($plural_kind));
                )*
            }

        )*
    };
    ($group:ident => $rem:tt ) => {

        pub mod $group {

            def_types!{@nogroupmod, stringify!($group), $rem }
        }

    };
    (@core => $rem:tt ) => {
        pub mod core {
            def_types!{@nogroupmod, "", $rem }
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

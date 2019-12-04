use super::{CAData, ClientConfig, Credentials};

use dirs::home_dir;

use std::fmt::{self, Display};
use std::fs::File;
use std::io;
use std::path::PathBuf;

const MISSING_CREDENTIAL_MESSAGE: &str = "No supported credentials found in kubeconfig file for the selected context. Only token, username/password, client certificate, and exec are currently supported. Please file an issue if there's another mechanism that you need";
const NO_HOME_DIR_MESSAGE: &str = "Unable to determine HOME directory to load ~/.kube/config";

#[derive(Debug)]
pub enum KubeConfigError {
    Io(io::Error),
    Format(serde_yaml::Error),
    MissingCredentials,
    NoHomeDir,
    InvalidKubeconfig(String),
    ExecErr(String),
}

impl From<serde_yaml::Error> for KubeConfigError {
    fn from(err: serde_yaml::Error) -> KubeConfigError {
        KubeConfigError::Format(err)
    }
}

impl From<io::Error> for KubeConfigError {
    fn from(err: io::Error) -> KubeConfigError {
        KubeConfigError::Io(err)
    }
}

impl Display for KubeConfigError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            KubeConfigError::Io(ref e) => write!(f, "IO error: {}", e),
            KubeConfigError::Format(ref e) => write!(f, "Kubeconfig format error: {}", e),
            KubeConfigError::MissingCredentials => f.write_str(MISSING_CREDENTIAL_MESSAGE),
            KubeConfigError::NoHomeDir => f.write_str(NO_HOME_DIR_MESSAGE),
            KubeConfigError::InvalidKubeconfig(ref msg) => {
                write!(f, "Invalid kubeconfig file: {}", msg)
            }
            KubeConfigError::ExecErr(ref msg) => write!(f, "exec error: {}", msg),
        }
    }
}
impl std::error::Error for KubeConfigError {}

fn get_kubeconfig_path() -> Result<PathBuf, KubeConfigError> {
    std::env::var("KUBECONFIG")
        .ok()
        .map(PathBuf::from)
        .or_else(|| {
            home_dir().map(|mut home| {
                home.push(".kube/config");
                home
            })
        })
        .ok_or(KubeConfigError::NoHomeDir)
}

pub fn load_from_kubeconfig(user_agent: String) -> Result<ClientConfig, KubeConfigError> {
    let path = get_kubeconfig_path()?;
    let reader = File::open(&path)?;
    let kubeconfig: KubeConfig = serde_yaml::from_reader(reader)?;

    let current_context = kubeconfig.current_context.as_str();

    let found_context = kubeconfig
        .contexts
        .iter()
        .find(|ctx| ctx.name.as_str() == current_context)
        .ok_or_else(|| {
            KubeConfigError::InvalidKubeconfig(format!(
                "No countext found for current context: '{}'",
                current_context
            ))
        })?;
    let found_cluster = kubeconfig
        .clusters
        .iter()
        .find(|cluster| cluster.name.as_str() == found_context.context.cluster.as_str())
        .ok_or_else(|| {
            KubeConfigError::InvalidKubeconfig(format!(
                "No cluster found for name: '{}'",
                found_context.context.cluster
            ))
        })?;
    let found_user = kubeconfig
        .users
        .iter()
        .find(|user| user.name.as_str() == found_context.context.user.as_str())
        .ok_or_else(|| {
            KubeConfigError::InvalidKubeconfig(format!(
                "No user found for name: '{}'",
                found_context.context.user
            ))
        })?;

    let credentials = get_credentials(&found_user.user)?;

    let impersonate = found_user.user.as_user.clone();
    let impersonate_groups = found_user.user.as_groups.clone();

    let conf = ClientConfig {
        user_agent,
        credentials,
        impersonate,
        impersonate_groups,
        api_server_endpoint: found_cluster.cluster.server.clone(),
        ca_data: Some(CAData::Contents(
            found_cluster.cluster.certificate_authority_data.clone(),
        )),
        verify_ssl_certs: true,
    };
    Ok(conf)
}

fn get_credentials(user: &UserInfo) -> Result<Credentials, KubeConfigError> {
    if let Some(token) = user.token.as_ref() {
        log::debug!("Using auth token from kubeconfig");
        return Ok(Credentials::Header(format!("Bearer {}", token)));
    }
    if let Some(username) = user.username.as_ref() {
        let pass = user.password.as_ref().ok_or_else(|| {
            KubeConfigError::InvalidKubeconfig("Username is specified but not password".to_owned())
        })?;
        log::debug!("Using username/password from kubeconfig");
        let user_and_pass = format!("{}:{}", username, pass);
        let encoded = base64::encode(&user_and_pass);
        return Ok(Credentials::Header(format!("Basic {}", encoded)));
    }
    if let Some(exec) = user.exec.as_ref() {
        return get_exec_token(exec).map(Credentials::Header);
    }

    if let Some(certificate) = user.client_certificate_data.as_ref() {
        let private_key = user.client_key_data.as_ref().ok_or_else(|| {
            KubeConfigError::InvalidKubeconfig(
                "'client-certificate-data' is specified, but 'client-key-data' is missing"
                    .to_owned(),
            )
        })?;
        return Ok(Credentials::Pem {
            certificate_base64: certificate.clone(),
            private_key_base64: private_key.clone(),
        });
    }

    Err(KubeConfigError::MissingCredentials)
}

fn get_exec_token(exec: &Exec) -> Result<String, KubeConfigError> {
    use std::process::Command;

    log::debug!("Getting credentials from: {:?}", exec);
    let mut cmd = Command::new(exec.command.as_str());
    for arg in exec.args.iter() {
        cmd.arg(arg);
    }

    for var in exec.env.iter() {
        cmd.env(var.name.as_str(), var.value.as_str());
    }

    let output = cmd.output()?;
    let credential: ExecCredential =
        serde_yaml::from_slice(output.stdout.as_slice()).map_err(|err| {
            KubeConfigError::ExecErr(format!(
                "Invalid stdout from exec command: '{}' : err: {}",
                exec.command, err
            ))
        })?;

    log::info!(
        "Successfully got token from command: '{}' with expiration: {:?}",
        exec.command,
        credential.status.expiration_timestamp
    );
    Ok(credential.status.token)
}

/// used only for deserializing the output of the `exec` command for retrieving credentials
#[derive(Deserialize, Clone, Debug)]
struct ExecCredential {
    status: ExecCredentialStatus,
}

/// used only for deserializing the output of the `exec` command for retrieving credentials
#[derive(Deserialize, Clone, Debug)]
struct ExecCredentialStatus {
    token: String,
    #[serde(rename = "expirationTimestamp")]
    expiration_timestamp: Option<String>,
}

// below are struct definitions that are used only for deserializing the kubeconfig. These are NOT
// complete definitions, so should not be exposed outside of this module.

#[derive(Deserialize, Debug, PartialEq, Clone)]
#[serde(rename_all = "kebab-case")]
struct ClusterInfo {
    server: String,
    certificate_authority_data: String,
}

#[derive(Deserialize, Debug, PartialEq, Clone)]
struct Cluster {
    name: String,
    cluster: ClusterInfo,
}

#[derive(Deserialize, Debug, PartialEq, Clone)]
struct UserInfo {
    pub username: Option<String>,
    pub password: Option<String>,
    pub token: Option<String>,

    #[serde(rename = "client-certificate-data")]
    pub client_certificate_data: Option<String>,
    #[serde(rename = "client-key-data")]
    pub client_key_data: Option<String>,

    #[serde(rename = "as")]
    pub as_user: Option<String>,
    #[serde(rename = "as-groups", default)]
    pub as_groups: Vec<String>,

    pub exec: Option<Exec>,
}

#[derive(Deserialize, Debug, PartialEq, Clone)]
struct ExecEnv {
    name: String,
    value: String,
}

#[derive(Deserialize, Debug, PartialEq, Clone)]
struct Exec {
    command: String,
    #[serde(default)]
    args: Vec<String>,
    #[serde(default)]
    env: Vec<ExecEnv>,
}

#[derive(Deserialize, Debug, PartialEq, Clone)]
struct User {
    name: String,
    user: UserInfo,
}

#[derive(Deserialize, Debug, PartialEq, Clone)]
struct ContextInfo {
    cluster: String,
    user: String,
}

#[derive(Deserialize, Debug, PartialEq, Clone)]
struct Context {
    name: String,
    context: ContextInfo,
}

#[derive(Deserialize, Debug, PartialEq, Clone)]
struct KubeConfig {
    #[serde(rename = "current-context")]
    current_context: String,
    clusters: Vec<Cluster>,
    users: Vec<User>,
    contexts: Vec<Context>,
}

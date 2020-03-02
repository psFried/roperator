use super::{CAData, ClientConfig, Credentials};

use dirs::home_dir;

use std::fmt::{self, Display};
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};

const MISSING_CREDENTIAL_MESSAGE: &str = "No supported credentials found in kubeconfig file for the selected context. Only token, username/password, client certificate, and exec are currently supported. Please file an issue if there's another mechanism that you need";
const NO_HOME_DIR_MESSAGE: &str = "Unable to determine HOME directory to load ~/.kube/config";

/// Error representing a problem with loading a kubeconfig file, or creating a `ClientConfig`
/// from it.
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

pub fn load_kubeconfig(
    user_agent: String,
    file_path: impl AsRef<Path>,
) -> Result<ClientConfig, KubeConfigError> {
    let reader = File::open(file_path.as_ref())?;
    let kubeconfig: KubeConfig = serde_yaml::from_reader(reader)?;
    let dir = file_path.as_ref().parent().ok_or_else(|| {
        KubeConfigError::Io(io::Error::new(
            io::ErrorKind::Other,
            format!(
                "Cannot determine parent directory of kube config file at path: '{}'",
                file_path.as_ref().display()
            ),
        ))
    })?;
    kubeconfig.create_client_config(user_agent, dir)
}

pub fn load_from_kubeconfig(user_agent: String) -> Result<ClientConfig, KubeConfigError> {
    let path = get_kubeconfig_path()?;
    load_kubeconfig(user_agent, path)
}

fn get_credentials(user: &UserInfo) -> Result<Credentials, KubeConfigError> {
    if let Some(token) = user.token.as_ref() {
        log::debug!("Using auth token from kubeconfig");
        return Ok(Credentials::base64_bearer_token(token));
    }
    if let Some(username) = user.username.as_ref() {
        let pass = user.password.as_ref().ok_or_else(|| {
            KubeConfigError::InvalidKubeconfig("Username is specified but not password".to_owned())
        })?;
        log::debug!("Using username/password from kubeconfig");
        return Ok(Credentials::basic(username, pass));
    }
    if let Some(exec) = user.exec.as_ref() {
        return get_exec_token(exec).map(Credentials::Header);
    }

    if let Some(certificate_path) = user.client_certificate.as_ref() {
        let private_key_path = user.client_key.as_ref().ok_or_else(|| {
            KubeConfigError::InvalidKubeconfig(
                "'client-certificate' is specified, but 'client-key' is missing".to_owned(),
            )
        })?;

        return Ok(Credentials::PemPath {
            certificate_path: certificate_path.clone(),
            private_key_path: private_key_path.clone(),
        });
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
    Ok(format!("Bearer {}", credential.status.token))
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
    certificate_authority_data: Option<String>,
    certificate_authority: Option<PathBuf>,
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

    #[serde(rename = "client-certificate")]
    pub client_certificate: Option<String>,
    #[serde(rename = "client-key")]
    pub client_key: Option<String>,

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

/// Represents a kubeconfig file that's been loaded into memory. This struct exposes
/// some functions to help support some less common authentication and connection scenarios.
/// For the vast majority of use cases, you can simply use the high-level functions for
/// loading the kubeconfig file and creating a `ClientConfig` from it.
#[derive(Deserialize, Debug, PartialEq, Clone)]
pub struct KubeConfig {
    #[serde(rename = "current-context")]
    current_context: String,
    clusters: Vec<Cluster>,
    users: Vec<User>,
    contexts: Vec<Context>,
}

impl KubeConfig {
    /// Attempts to load the kube config file from the usual locations. If the `KUBECONFIG`
    /// environement variable is set, then the path specified by it is used. Otherwise,
    /// it will look in `~/.kube/config`. If the file is not found, or if it is syntactically
    /// or structurally invalid, then an error will be returned.
    /// If the file is found, then the path that it was loaded from is also returned in
    /// the tuple.
    pub fn load() -> Result<(KubeConfig, PathBuf), KubeConfigError> {
        let path = get_kubeconfig_path()?;
        let conf = KubeConfig::load_file(&path)?;
        Ok((conf, path))
    }

    /// Attempts to load the kube config file from the specified path. Returns an error if
    /// the file is missing, or if it is syntactically or structurally invalid.
    pub fn load_file(path: &Path) -> Result<KubeConfig, KubeConfigError> {
        let reader = File::open(path)?;
        let conf = serde_yaml::from_reader(reader)?;
        Ok(conf)
    }

    /// Creates a `ClientConfig` from the data in this kube config. Returns an error if the kube config
    /// file is missing required data. The `kubeconfig_parent_dir` is used in order to resolve relative
    /// file paths that appear in the file, for example as paths to certificate files.
    pub fn create_client_config(
        &self,
        user_agent: String,
        kubeconfig_parent_dir: &Path,
    ) -> Result<ClientConfig, KubeConfigError> {
        self.resolve(user_agent, kubeconfig_parent_dir, None)
    }

    /// Creates a `ClientConfig`, the same as `create_client_config`, except that the given `credentials`
    /// will be used _instead of_ trying to determine the credentials from the kube config file. This allows
    /// you to work around cases where the authentication mechanism specified in the kubeconfig file is not
    /// supported out of the box in roperator.
    pub fn create_client_config_with_credentials(
        &self,
        user_agent: String,
        kubeconfig_parent_dir: &Path,
        credentials: Credentials,
    ) -> Result<ClientConfig, KubeConfigError> {
        self.resolve(user_agent, kubeconfig_parent_dir, Some(credentials))
    }

    fn resolve(
        &self,
        user_agent: String,
        kube_config_dir: &Path,
        override_credentials: Option<Credentials>,
    ) -> Result<ClientConfig, KubeConfigError> {
        let current_context = self.current_context.as_str();
        let found_context = self
            .contexts
            .iter()
            .find(|ctx| ctx.name.as_str() == current_context)
            .ok_or_else(|| {
                KubeConfigError::InvalidKubeconfig(format!(
                    "No countext found for current context: '{}'",
                    current_context
                ))
            })?;
        let found_cluster = self
            .clusters
            .iter()
            .find(|cluster| cluster.name.as_str() == found_context.context.cluster.as_str())
            .ok_or_else(|| {
                KubeConfigError::InvalidKubeconfig(format!(
                    "No cluster found for name: '{}'",
                    found_context.context.cluster
                ))
            })?;
        let found_user = self
            .users
            .iter()
            .find(|user| user.name.as_str() == found_context.context.user.as_str())
            .ok_or_else(|| {
                KubeConfigError::InvalidKubeconfig(format!(
                    "No user found for name: '{}'",
                    found_context.context.user
                ))
            })?;

        let credentials = if let Some(creds) = override_credentials {
            creds
        } else {
            get_credentials(&found_user.user)?
        };

        let impersonate = found_user.user.as_user.clone();
        let impersonate_groups = found_user.user.as_groups.clone();

        let ca_data = found_cluster
            .cluster
            .certificate_authority_data
            .clone()
            .map(CAData::Contents)
            .or_else(|| {
                found_cluster
                    .cluster
                    .certificate_authority
                    .clone()
                    .map(|ca_path| {
                        // TODO: we'll need to make a breaking change to the CAData enum so that we can
                        // always use Paths instead of strings for these
                        let resolved_path =
                            kube_config_dir.join(&ca_path).to_string_lossy().to_string();
                        log::debug!(
                            "Resolved cluster certificate-authority path '{}' to '{}'",
                            ca_path.display(),
                            resolved_path
                        );
                        CAData::File(resolved_path)
                    })
            });

        let conf = ClientConfig {
            user_agent,
            credentials,
            impersonate,
            impersonate_groups,
            api_server_endpoint: found_cluster.cluster.server.clone(),
            ca_data,
            verify_ssl_certs: true,
        };
        Ok(conf)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn loads_kubeconfig_with_cluster_ca_file() {
        let file = "src/config/test-data/kubeconfig-with-ca-file.yaml";
        let user_agent = "my-user-agent";
        let loaded =
            load_kubeconfig(user_agent.to_string(), file).expect("failed to load kubeconfig");
        let expected = CAData::File("src/config/test-data/./dummy-ca.crt".to_string());
        assert_eq!(Some(expected), loaded.ca_data);
    }
}

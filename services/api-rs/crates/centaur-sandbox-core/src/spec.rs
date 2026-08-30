use std::collections::BTreeMap;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize};

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RepoCacheAccess {
    None,
    Public,
    #[default]
    All,
}

impl RepoCacheAccess {
    pub fn enabled(&self) -> bool {
        !matches!(self, Self::None)
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Public => "public",
            Self::All => "all",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SandboxCapabilities {
    #[serde(default)]
    pub repo_cache: RepoCacheAccess,
    pub observability_enabled: bool,
}

impl SandboxCapabilities {
    pub const fn default_enabled() -> Self {
        Self {
            repo_cache: RepoCacheAccess::All,
            observability_enabled: true,
        }
    }

    pub fn is_default_enabled(&self) -> bool {
        self.repo_cache.enabled() && self.observability_enabled
    }
}

impl Default for SandboxCapabilities {
    fn default() -> Self {
        Self::default_enabled()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SandboxSpec {
    pub image: String,
    #[serde(default)]
    pub labels: std::collections::BTreeMap<String, String>,
    pub command: Option<Vec<String>>,
    pub args: Vec<String>,
    pub env: Vec<EnvVar>,
    /// Files materialized in the sandbox before the workload starts.
    /// Target paths are absolute paths inside the sandbox.
    #[serde(default)]
    pub files: Vec<SandboxFile>,
    pub working_dir: Option<String>,
    pub mounts: Vec<Mount>,
    pub resources: Option<ResourceRequirements>,
    /// iron-control principal OID (``prn_…``) this sandbox's egress proxy
    /// should act as. When set, the backend registers/binds an iron-control
    /// proxy for the sandbox instead of rendering a static proxy config.
    #[serde(default)]
    pub iron_control_principal: Option<String>,
    /// iron-control principal OID of the human requesting the turn that
    /// creates this sandbox, bound to the proxy alongside
    /// [`Self::iron_control_principal`].
    #[serde(default)]
    pub iron_control_requester_principal: Option<String>,
    /// Labels applied to the iron-control proxy registered for this sandbox.
    /// These are distinct from Kubernetes labels and are used by iron-control
    /// when rendering proxy-specific config.
    #[serde(default)]
    pub iron_control_proxy_labels: std::collections::BTreeMap<String, String>,
    #[serde(default)]
    pub capabilities: SandboxCapabilities,
}

impl SandboxSpec {
    pub fn new(image: impl Into<String>) -> Self {
        Self {
            image: image.into(),
            labels: std::collections::BTreeMap::new(),
            command: None,
            args: Vec::new(),
            env: Vec::new(),
            files: Vec::new(),
            working_dir: None,
            mounts: Vec::new(),
            resources: None,
            iron_control_principal: None,
            iron_control_requester_principal: None,
            iron_control_proxy_labels: std::collections::BTreeMap::new(),
            capabilities: SandboxCapabilities::default_enabled(),
        }
    }

    pub fn iron_control_principal(mut self, principal_foreign_id: impl Into<String>) -> Self {
        self.iron_control_principal = Some(principal_foreign_id.into());
        self
    }

    pub fn capabilities(mut self, capabilities: SandboxCapabilities) -> Self {
        self.capabilities = capabilities;
        self
    }

    pub fn label(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.labels.insert(name.into(), value.into());
        self
    }

    pub fn command(mut self, command: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.command = Some(command.into_iter().map(Into::into).collect());
        self
    }

    pub fn args(mut self, args: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.args = args.into_iter().map(Into::into).collect();
        self
    }

    pub fn env(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.env.push(EnvVar::new(name, value));
        self
    }

    pub fn file(mut self, target_path: impl Into<String>, contents: impl Into<String>) -> Self {
        self.files.push(SandboxFile::new(target_path, contents));
        self
    }

    pub fn working_dir(mut self, working_dir: impl Into<String>) -> Self {
        self.working_dir = Some(working_dir.into());
        self
    }

    pub fn mount(mut self, mount: Mount) -> Self {
        self.mounts.push(mount);
        self
    }

    pub fn resources(mut self, resources: ResourceRequirements) -> Self {
        self.resources = Some(resources);
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SandboxFile {
    /// Absolute destination path inside the sandbox.
    pub target_path: String,
    pub contents: String,
}

impl SandboxFile {
    pub fn new(target_path: impl Into<String>, contents: impl Into<String>) -> Self {
        Self {
            target_path: target_path.into(),
            contents: contents.into(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EnvVar {
    pub name: String,
    pub value: String,
}

impl EnvVar {
    pub fn new(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: value.into(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Mount {
    pub kind: MountKind,
    pub target_path: String,
    pub read_only: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sub_path: Option<String>,
}

impl Mount {
    pub fn new(kind: MountKind, target_path: impl Into<String>) -> Self {
        Self {
            kind,
            target_path: target_path.into(),
            read_only: false,
            sub_path: None,
        }
    }

    pub fn read_only(mut self) -> Self {
        self.read_only = true;
        self
    }

    pub fn sub_path(mut self, sub_path: impl Into<String>) -> Self {
        self.sub_path = Some(sub_path.into());
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum MountKind {
    EmptyDir,
    NamedVolume(String),
    Bind { source_path: String },
}

/// Container resources in the Kubernetes `ResourceRequirements` shape.
/// Quantity values are retained as strings and resource names are not limited
/// to CPU and memory, so extended and ephemeral-storage resources survive the
/// backend-neutral sandbox boundary.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceRequirements {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub claims: Vec<ResourceClaim>,
    #[serde(
        default,
        deserialize_with = "deserialize_quantity_map",
        skip_serializing_if = "BTreeMap::is_empty"
    )]
    pub limits: BTreeMap<String, String>,
    #[serde(
        default,
        deserialize_with = "deserialize_quantity_map",
        skip_serializing_if = "BTreeMap::is_empty"
    )]
    pub requests: BTreeMap<String, String>,
}

impl ResourceRequirements {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn request(mut self, name: impl Into<String>, quantity: impl Into<String>) -> Self {
        self.requests.insert(name.into(), quantity.into());
        self
    }

    pub fn limit(mut self, name: impl Into<String>, quantity: impl Into<String>) -> Self {
        self.limits.insert(name.into(), quantity.into());
        self
    }

    pub fn claim(mut self, name: impl Into<String>, request: Option<String>) -> Self {
        self.claims.push(ResourceClaim {
            name: name.into(),
            request,
        });
        self
    }

    pub fn is_empty(&self) -> bool {
        self.claims.is_empty() && self.limits.is_empty() && self.requests.is_empty()
    }

    /// The `memory` limit in bytes, `None` when it is absent or not
    /// expressible as a whole-byte quantity.
    pub fn memory_limit_bytes(&self) -> Option<u64> {
        self.limits
            .get("memory")
            .and_then(|quantity| quantity_to_bytes(quantity))
    }
}

/// Multiplier of one binary kibi unit; a whole quantity of this size is
/// 1 GiB of bytes.
const GIB_BYTES: u64 = 1 << 30;

/// Parse a Kubernetes memory quantity into bytes: a bare integer, or an
/// integer mantissa with a binary (`Ki`, `Mi`, `Gi`, `Ti`) or decimal
/// (`K`, `M`, `G`, `T`) suffix. Anything else, including fractional and
/// milli forms, parses to `None` rather than guessing.
pub fn quantity_to_bytes(quantity: &str) -> Option<u64> {
    let quantity = quantity.trim();
    let suffixes: &[(&str, u64)] = &[
        ("Ki", 1 << 10),
        ("Mi", 1 << 20),
        ("Gi", GIB_BYTES),
        ("Ti", 1 << 40),
        ("K", 1_000),
        ("M", 1_000_000),
        ("G", 1_000_000_000),
        ("T", 1_000_000_000_000),
    ];
    let (mantissa, multiplier) = suffixes
        .iter()
        .find_map(|(suffix, multiplier)| {
            quantity
                .strip_suffix(suffix)
                .map(|mantissa| (mantissa, *multiplier))
        })
        .unwrap_or((quantity, 1));
    let mantissa = u64::from_str(mantissa).ok()?;
    mantissa.checked_mul(multiplier)
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceClaim {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request: Option<String>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum ResourceQuantity {
    String(String),
    Signed(i64),
    Unsigned(u64),
    Float(f64),
}

impl ResourceQuantity {
    fn into_string(self) -> String {
        match self {
            Self::String(value) => value,
            Self::Signed(value) => value.to_string(),
            Self::Unsigned(value) => value.to_string(),
            Self::Float(value) => value.to_string(),
        }
    }
}

fn deserialize_quantity_map<'de, D>(deserializer: D) -> Result<BTreeMap<String, String>, D::Error>
where
    D: Deserializer<'de>,
{
    let quantities = Option::<BTreeMap<String, ResourceQuantity>>::deserialize(deserializer)?
        .unwrap_or_default();
    Ok(quantities
        .into_iter()
        .map(|(name, quantity)| (name, quantity.into_string()))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quantity_parsing_covers_fleet_shapes() {
        assert_eq!(quantity_to_bytes("1048576"), Some(1_048_576));
        assert_eq!(quantity_to_bytes("512Mi"), Some(512 * (1 << 20)));
        assert_eq!(quantity_to_bytes("32Gi"), Some(32 * GIB_BYTES));
        assert_eq!(quantity_to_bytes("1Ti"), Some(1 << 40));
        assert_eq!(quantity_to_bytes("2G"), Some(2_000_000_000));
        assert_eq!(quantity_to_bytes(" 8Ki "), Some(8 * (1 << 10)));
    }

    #[test]
    fn quantity_parsing_rejects_unsizable_forms() {
        assert_eq!(quantity_to_bytes("0.5Gi"), None);
        assert_eq!(quantity_to_bytes("500m"), None);
        assert_eq!(quantity_to_bytes(""), None);
        assert_eq!(quantity_to_bytes("not-a-quantity"), None);
        // Mantissa that overflows once the multiplier is applied.
        assert_eq!(quantity_to_bytes("18446744073709551616Ki"), None);
    }

    #[test]
    fn memory_limit_bytes_reads_the_limit_map() {
        let resources = ResourceRequirements::new().limit("memory", "32Gi");
        assert_eq!(resources.memory_limit_bytes(), Some(32 * GIB_BYTES));
        assert_eq!(ResourceRequirements::new().memory_limit_bytes(), None);
    }
}

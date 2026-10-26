//! Configuration file: DNS listeners plus one `[[network]]` table per served seed name.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use hickory_proto::rr::Name;
use serde::Deserialize;

use crate::coin;
use crate::store::is_routable;

const LISTEN_ENV: &str = "SEEDCRYSTAL_DNS_LISTEN";
/// The environment variable that can carry the Cloudflare API token instead of a file.
pub const TOKEN_ENV: &str = "SEEDCRYSTAL_CLOUDFLARE_TOKEN";
const MIN_TTL: u32 = 60;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub dns: DnsConfig,
    #[serde(default)]
    pub network: Vec<NetworkConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DnsConfig {
    #[serde(default = "default_listen")]
    pub listen: Vec<SocketAddr>,
}

impl Default for DnsConfig {
    fn default() -> Self {
        Self {
            listen: default_listen(),
        }
    }
}

fn default_listen() -> Vec<SocketAddr> {
    vec!["0.0.0.0:53".parse().unwrap(), "[::]:53".parse().unwrap()]
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NetworkConfig {
    pub preset: Option<String>,
    /// A custom preset file. A relative path is resolved against the config file's directory.
    pub preset_file: Option<PathBuf>,
    pub seed: String,
    /// Required for `publish = "authoritative"`.
    pub nameserver: Option<String>,
    /// Required for `publish = "authoritative"`.
    pub mailbox: Option<String>,
    pub data_dir: PathBuf,
    #[serde(default)]
    pub publish: Publish,
    pub cloudflare: Option<CloudflareConfig>,
    #[serde(default)]
    pub bootstrap: Vec<SocketAddr>,
    #[serde(default)]
    pub allow_private: bool,
}

/// How a network's answers reach resolvers.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Publish {
    /// Seedcrystal's own DNS server answers queries.
    #[default]
    Authoritative,
    /// Seedcrystal writes A/AAAA records into a Cloudflare zone; Cloudflare answers.
    Cloudflare,
}

/// The `[network.cloudflare]` table. The token is deliberately not a key: it comes from
/// `token_file` or the environment, so it never sits in a config file that gets shared.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CloudflareConfig {
    /// The zone's ID (zone Overview page, "API" section).
    pub zone_id: String,
    pub token_file: Option<PathBuf>,
    #[serde(default = "default_ttl")]
    pub ttl: u32,
    #[serde(default = "default_interval_secs")]
    pub interval_secs: u64,
    /// Addresses published per name and family, 1 to 50.
    #[serde(default = "default_per_name")]
    pub per_name: usize,
    #[serde(default)]
    pub dry_run: bool,
    #[serde(default = "default_api_base")]
    pub api_base: String,
}

fn default_ttl() -> u32 {
    MIN_TTL
}

fn default_interval_secs() -> u64 {
    300
}

fn default_per_name() -> usize {
    20
}

fn default_api_base() -> String {
    crate::cloudflare::api::DEFAULT_API_BASE.to_string()
}

impl CloudflareConfig {
    fn validate(&self) -> Result<()> {
        if self.zone_id.trim().is_empty() {
            bail!(
                "network.cloudflare.zone_id is empty; copy it from the zone's Overview page \
                 in the Cloudflare dashboard (\"API\" section)"
            );
        }
        if self.ttl < MIN_TTL {
            bail!(
                "network.cloudflare.ttl is {}; Cloudflare's minimum is {MIN_TTL} seconds",
                self.ttl
            );
        }
        if self.interval_secs == 0 {
            bail!("network.cloudflare.interval_secs must be greater than zero");
        }
        if !(1..=crate::cloudflare::publish::MAX_PER_NAME).contains(&self.per_name) {
            bail!(
                "network.cloudflare.per_name is {}; it must be between 1 and {}",
                self.per_name,
                crate::cloudflare::publish::MAX_PER_NAME
            );
        }
        if self.api_base.trim().is_empty() {
            bail!("network.cloudflare.api_base is empty; remove the key to use the default");
        }
        Ok(())
    }
}

/// The API token: the contents of `token_file` (surrounding whitespace trimmed) or `env`, the
/// value of [`TOKEN_ENV`]. Exactly one must be given. No error message contains the token.
pub fn resolve_token(cf: &CloudflareConfig, env: Option<&str>) -> Result<String> {
    let env = env.map(str::trim).filter(|v| !v.is_empty());
    match (&cf.token_file, env) {
        (Some(_), Some(_)) => bail!(
            "the Cloudflare token is given twice: both `token_file` and the {TOKEN_ENV} \
             environment variable are set; use only one"
        ),
        (None, None) => bail!(
            "no Cloudflare API token: set `token_file` in [network.cloudflare] to a file \
             holding the token, or export it as {TOKEN_ENV}"
        ),
        (None, Some(v)) => Ok(v.to_string()),
        (Some(file), None) => {
            let text = std::fs::read_to_string(file)
                .with_context(|| format!("read Cloudflare token_file {}", file.display()))?;
            warn_if_readable_by_others(file);
            let token = text.trim();
            if token.is_empty() {
                bail!("Cloudflare token_file {} is empty", file.display());
            }
            Ok(token.to_string())
        }
    }
}

fn warn_if_readable_by_others(file: &Path) {
    use std::os::unix::fs::PermissionsExt;
    if let Ok(meta) = std::fs::metadata(file)
        && meta.permissions().mode() & 0o077 != 0
    {
        tracing::warn!(
            "Cloudflare token_file {} is readable by other users; run `chmod 600 {}`",
            file.display(),
            file.display()
        );
    }
}

/// Loads and validates the config at `path`. `SEEDCRYSTAL_DNS_LISTEN` (a comma-separated
/// list of socket addresses) overrides `dns.listen`.
pub fn load(path: &Path) -> Result<Config> {
    load_with(path, std::env::var(LISTEN_ENV).ok().as_deref())
}

pub(crate) fn load_with(path: &Path, env_listen: Option<&str>) -> Result<Config> {
    let text =
        std::fs::read_to_string(path).with_context(|| format!("read config {}", path.display()))?;
    let mut cfg: Config =
        toml::from_str(&text).with_context(|| format!("parse config {}", path.display()))?;
    if let Some(list) = env_listen {
        cfg.dns.listen = list
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| {
                s.parse::<SocketAddr>()
                    .with_context(|| format!("{LISTEN_ENV}: bad address `{s}`"))
            })
            .collect::<Result<_>>()?;
    }
    if cfg.dns.listen.is_empty() {
        bail!("dns.listen is empty");
    }
    if cfg.network.is_empty() {
        bail!("no [[network]] table in {}", path.display());
    }
    let base = path.parent().unwrap_or(Path::new("."));
    let mut seen: Vec<String> = Vec::new();
    let mut stores: Vec<(PathBuf, String)> = Vec::new();
    for (i, n) in cfg.network.iter_mut().enumerate() {
        n.validate(base)
            .with_context(|| format!("network #{}", i + 1))?;
        if seen.contains(&n.seed) {
            bail!("network #{}: seed `{}` is configured twice", i + 1, n.seed);
        }
        seen.push(n.seed.clone());
        let preset = n
            .load_preset()
            .with_context(|| format!("network #{}", i + 1))?;
        n.check_bootstrap(preset.default_port)
            .with_context(|| format!("network #{}", i + 1))?;
        let key = (n.data_dir.clone(), preset.id);
        if stores.contains(&key) {
            bail!(
                "network #{}: preset `{}` in data_dir {} is already used by another network \
                 and would share its database; give each network its own data_dir",
                i + 1,
                key.1,
                key.0.display()
            );
        }
        stores.push(key);
    }
    Ok(cfg)
}

impl NetworkConfig {
    /// The preset this network uses: a built-in one, or the one in `preset_file`.
    pub fn load_preset(&self) -> Result<coin::CoinPreset> {
        match (&self.preset, &self.preset_file) {
            (Some(id), _) => Ok(coin::builtin(id)?),
            (None, Some(file)) => {
                let text = std::fs::read_to_string(file)
                    .with_context(|| format!("read preset file {}", file.display()))?;
                coin::from_toml(&text).with_context(|| format!("preset file {}", file.display()))
            }
            (None, None) => bail!("set exactly one of `preset` or `preset_file`"),
        }
    }

    /// The store drops gossip on the wrong port and unroutable addresses, so a
    /// bootstrap entry like that would be probed and then silently never served.
    fn check_bootstrap(&self, default_port: u16) -> Result<()> {
        if self.allow_private {
            return Ok(());
        }
        for b in &self.bootstrap {
            if b.port() != default_port {
                bail!(
                    "bootstrap entry {b}: port {} is not the preset's port {default_port} \
                     (set allow_private = true to allow other ports on a test network)",
                    b.port()
                );
            }
            if !is_routable(b.ip()) {
                bail!(
                    "bootstrap entry {b}: address is not publicly routable \
                     (set allow_private = true on a private test network)"
                );
            }
        }
        Ok(())
    }

    fn validate(&mut self, base: &Path) -> Result<()> {
        match (&self.preset, &mut self.preset_file) {
            (Some(_), Some(_)) => bail!("set exactly one of `preset` and `preset_file`, not both"),
            (None, None) => bail!("set exactly one of `preset` or `preset_file`"),
            (Some(id), None) => {
                coin::builtin(id)?;
            }
            (None, Some(f)) => {
                if f.is_relative() {
                    *f = base.join(f.as_path());
                }
            }
        }
        self.seed = normalise(&self.seed, "seed")?;
        fqdn(&self.seed)?;
        match self.publish {
            Publish::Authoritative => self.validate_authoritative(),
            Publish::Cloudflare => self.validate_cloudflare(),
        }
    }

    fn validate_authoritative(&mut self) -> Result<()> {
        if self.cloudflare.is_some() {
            bail!(
                "[network.cloudflare] is set but publish is \"authoritative\"; \
                 add `publish = \"cloudflare\"` or remove the table"
            );
        }
        let (Some(ns), Some(mailbox)) = (&self.nameserver, &self.mailbox) else {
            let missing = if self.nameserver.is_none() {
                "nameserver"
            } else {
                "mailbox"
            };
            bail!(
                "`{missing}` is required when publish = \"authoritative\" (the default); \
                 to publish through Cloudflare instead, set `publish = \"cloudflare\"`"
            );
        };
        let ns = normalise(ns, "nameserver")?;
        let mailbox = normalise(&mailbox.replacen('@', ".", 1), "mailbox")?;
        if fqdn(&self.seed)?.zone_of(&fqdn(&ns)?) {
            bail!(
                "nameserver `{ns}` is inside the seed zone `{}`; use a name outside it \
                 (there is no glue, so resolvers could never find it)",
                self.seed
            );
        }
        self.nameserver = Some(ns);
        self.mailbox = Some(mailbox);
        Ok(())
    }

    fn validate_cloudflare(&self) -> Result<()> {
        for (key, set) in [
            ("nameserver", self.nameserver.is_some()),
            ("mailbox", self.mailbox.is_some()),
        ] {
            if set {
                bail!(
                    "`{key}` is only used by publish = \"authoritative\"; \
                     remove it, Cloudflare serves the answers in this mode"
                );
            }
        }
        match &self.cloudflare {
            None => bail!(
                "publish = \"cloudflare\" needs a [network.cloudflare] table with at least \
                 `zone_id`"
            ),
            Some(cf) => cf.validate(),
        }
    }
}

/// Lowercase, no trailing dot, and a syntactically valid DNS name.
fn normalise(raw: &str, what: &str) -> Result<String> {
    let s = raw.trim().trim_end_matches('.').to_ascii_lowercase();
    if s.is_empty() {
        bail!("{what} is empty");
    }
    fqdn(&s).with_context(|| format!("{what} `{raw}` is not a valid DNS name"))?;
    Ok(s)
}

/// The absolute DNS name for a normalised config string.
pub fn fqdn(s: &str) -> Result<Name> {
    Ok(Name::from_ascii(format!("{s}."))?)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(dir: &tempfile::TempDir, text: &str) -> std::path::PathBuf {
        let p = dir.path().join("seedcrystal.toml");
        std::fs::write(&p, text).unwrap();
        p
    }

    const MINIMAL: &str = r#"
[[network]]
preset = "osmium-mainnet"
seed = "dnsseed.osmium.space"
nameserver = "seed-ns.osmium.space"
mailbox = "hostmaster.osmium.space"
data_dir = "/var/lib/seedcrystal"
"#;

    #[test]
    fn minimal_osmium_config_loads() {
        let d = tempfile::tempdir().unwrap();
        let c = load_with(&write(&d, MINIMAL), None).unwrap();
        assert_eq!(c.dns.listen.len(), 2);
        assert_eq!(c.dns.listen[0].to_string(), "0.0.0.0:53");
        assert_eq!(c.dns.listen[1].to_string(), "[::]:53");
        let n = &c.network[0];
        assert_eq!(n.seed, "dnsseed.osmium.space");
        assert!(n.bootstrap.is_empty() && !n.allow_private);
    }

    #[test]
    fn unknown_key_is_named() {
        let d = tempfile::tempdir().unwrap();
        let e = load_with(&write(&d, &MINIMAL.replace("seed =", "seeed =")), None).unwrap_err();
        assert!(format!("{e:#}").contains("seeed"), "{e:#}");
    }

    #[test]
    fn exactly_one_of_preset_or_preset_file() {
        let d = tempfile::tempdir().unwrap();
        let both = MINIMAL.replace(
            "preset = \"osmium-mainnet\"",
            "preset = \"osmium-mainnet\"\npreset_file = \"x.toml\"",
        );
        let e = load_with(&write(&d, &both), None).unwrap_err();
        assert!(
            format!("{e:#}").contains("exactly one of `preset` and `preset_file`"),
            "{e:#}"
        );
        let none = MINIMAL.replace("preset = \"osmium-mainnet\"\n", "");
        let e = load_with(&write(&d, &none), None).unwrap_err();
        assert!(
            format!("{e:#}").contains("exactly one of `preset` or `preset_file`"),
            "{e:#}"
        );
    }

    #[test]
    fn seed_name_normalised() {
        let d = tempfile::tempdir().unwrap();
        let t = MINIMAL.replace("dnsseed.osmium.space", "DNSSeed.Osmium.Space.");
        let c = load_with(&write(&d, &t), None).unwrap();
        assert_eq!(c.network[0].seed, "dnsseed.osmium.space");
    }

    #[test]
    fn nameserver_inside_seed_zone_rejected() {
        let d = tempfile::tempdir().unwrap();
        for ns in [
            "ns.dnsseed.osmium.space",
            "NS.DNSSeed.Osmium.Space.",
            "dnsseed.osmium.space",
        ] {
            let t = MINIMAL.replace("seed-ns.osmium.space", ns);
            let e = load_with(&write(&d, &t), None).unwrap_err();
            assert!(
                format!("{e:#}").contains("inside the seed zone"),
                "{ns}: {e:#}"
            );
        }
    }

    #[test]
    fn env_overrides_listen() {
        let d = tempfile::tempdir().unwrap();
        let t = format!("[dns]\nlisten = [\"127.0.0.1:5300\"]\n{MINIMAL}");
        let p = write(&d, &t);
        assert_eq!(load_with(&p, None).unwrap().dns.listen.len(), 1);
        let c = load_with(&p, Some("127.0.0.1:5400, [::1]:5400")).unwrap();
        assert_eq!(c.dns.listen[0].to_string(), "127.0.0.1:5400");
        assert_eq!(c.dns.listen[1].to_string(), "[::1]:5400");
    }

    #[test]
    fn mailbox_email_becomes_dns_name() {
        let d = tempfile::tempdir().unwrap();
        let t = MINIMAL.replace("hostmaster.osmium.space", "hostmaster@osmium.space");
        let c = load_with(&write(&d, &t), None).unwrap();
        assert_eq!(
            c.network[0].mailbox.as_deref(),
            Some("hostmaster.osmium.space")
        );
    }

    #[test]
    fn duplicate_data_dir_and_preset_rejected() {
        let d = tempfile::tempdir().unwrap();
        let second = MINIMAL.replace("dnsseed.osmium.space", "other.osmium.space");
        let e = load_with(&write(&d, &format!("{MINIMAL}{second}")), None).unwrap_err();
        assert!(format!("{e:#}").contains("share its database"), "{e:#}");
        let other_dir = second.replace("/var/lib/seedcrystal", "/var/lib/seedcrystal2");
        load_with(&write(&d, &format!("{MINIMAL}{other_dir}")), None).unwrap();
    }

    #[test]
    fn duplicate_check_sees_through_preset_file() {
        let d = tempfile::tempdir().unwrap();
        std::fs::write(
            d.path().join("p.toml"),
            include_str!("../presets/osmium-mainnet.toml"),
        )
        .unwrap();
        let second = MINIMAL
            .replace("dnsseed.osmium.space", "other.osmium.space")
            .replace("preset = \"osmium-mainnet\"", "preset_file = \"p.toml\"");
        let e = load_with(&write(&d, &format!("{MINIMAL}{second}")), None).unwrap_err();
        assert!(format!("{e:#}").contains("share its database"), "{e:#}");
    }

    #[test]
    fn bootstrap_the_store_would_drop_is_rejected() {
        let d = tempfile::tempdir().unwrap();
        let with = |b: &str, extra: &str| {
            MINIMAL.replace(
                "data_dir =",
                &format!("bootstrap = [\"{b}\"]\n{extra}data_dir ="),
            )
        };
        let e = load_with(&write(&d, &with("8.8.8.8:1234", "")), None).unwrap_err();
        let msg = format!("{e:#}");
        assert!(
            msg.contains("8.8.8.8:1234") && msg.contains("port 1234"),
            "{msg}"
        );
        let e = load_with(&write(&d, &with("10.0.0.1:9969", "")), None).unwrap_err();
        let msg = format!("{e:#}");
        assert!(
            msg.contains("10.0.0.1:9969") && msg.contains("routable"),
            "{msg}"
        );
        load_with(&write(&d, &with("8.8.8.8:9969", "")), None).unwrap();
        load_with(
            &write(&d, &with("10.0.0.1:1234", "allow_private = true\n")),
            None,
        )
        .unwrap();
    }

    #[test]
    fn missing_preset_file_is_a_load_error() {
        let d = tempfile::tempdir().unwrap();
        let t = MINIMAL.replace("preset = \"osmium-mainnet\"", "preset_file = \"nope.toml\"");
        let e = load_with(&write(&d, &t), None).unwrap_err();
        assert!(format!("{e:#}").contains("nope.toml"), "{e:#}");
    }

    const CLOUDFLARE: &str = r#"
[[network]]
preset = "osmium-mainnet"
seed = "dnsseed.osmium.space"
data_dir = "/var/lib/seedcrystal"
publish = "cloudflare"
[network.cloudflare]
zone_id = "abc123"
token_file = "/etc/seedcrystal/cloudflare.token"
"#;

    #[test]
    fn cloudflare_network_needs_no_nameserver() {
        let d = tempfile::tempdir().unwrap();
        let c = load_with(&write(&d, CLOUDFLARE), None).unwrap();
        let n = &c.network[0];
        assert_eq!(n.publish, Publish::Cloudflare);
        let cf = n.cloudflare.as_ref().unwrap();
        assert_eq!(cf.zone_id, "abc123");
        assert_eq!(cf.ttl, 60);
        assert_eq!(cf.interval_secs, 300);
        assert!(!cf.dry_run);
        assert_eq!(cf.per_name, 20);
        assert_eq!(cf.api_base, "https://api.cloudflare.com/client/v4");
        assert!(n.nameserver.is_none() && n.mailbox.is_none());
    }

    #[test]
    fn authoritative_still_needs_nameserver_and_mailbox() {
        let d = tempfile::tempdir().unwrap();
        for key in ["nameserver", "mailbox"] {
            let t: String = MINIMAL
                .lines()
                .filter(|l| !l.starts_with(key))
                .map(|l| format!("{l}\n"))
                .collect();
            let e = load_with(&write(&d, &t), None).unwrap_err();
            let msg = format!("{e:#}");
            assert!(msg.contains(key) && msg.contains("authoritative"), "{msg}");
        }
        assert_eq!(
            load_with(&write(&d, MINIMAL), None).unwrap().network[0].publish,
            Publish::Authoritative
        );
    }

    #[test]
    fn cloudflare_section_with_authoritative_mode_is_named_error() {
        let d = tempfile::tempdir().unwrap();
        let t = format!("{MINIMAL}[network.cloudflare]\nzone_id = \"z\"\n");
        let e = load_with(&write(&d, &t), None).unwrap_err();
        assert!(format!("{e:#}").contains("[network.cloudflare]"), "{e:#}");
        let t = CLOUDFLARE.replace("[network.cloudflare]\n", "[network.cloudfare]\n");
        assert!(load_with(&write(&d, &t), None).is_err());
        let t = CLOUDFLARE
            .lines()
            .take(6)
            .map(|l| format!("{l}\n"))
            .collect::<String>();
        let e = load_with(&write(&d, &t), None).unwrap_err();
        assert!(format!("{e:#}").contains("[network.cloudflare]"), "{e:#}");
    }

    #[test]
    fn token_comes_from_file_or_env_never_inline() {
        let d = tempfile::tempdir().unwrap();
        let t = CLOUDFLARE.replace("zone_id", "token = \"abc\"\nzone_id");
        let e = load_with(&write(&d, &t), None).unwrap_err();
        assert!(format!("{e:#}").contains("token"), "{e:#}");

        let file = d.path().join("t.token");
        std::fs::write(&file, "  file-token \n\n").unwrap();
        let cf = CloudflareConfig {
            token_file: Some(file),
            ..cf_defaults()
        };
        assert_eq!(resolve_token(&cf, None).unwrap(), "file-token");
        let e = resolve_token(&cf, Some("env-token")).unwrap_err();
        let msg = format!("{e:#}");
        assert!(
            msg.contains("token_file") && msg.contains(TOKEN_ENV),
            "{msg}"
        );
        assert!(!msg.contains("env-token") && !msg.contains("file-token"));

        let cf = cf_defaults();
        assert_eq!(
            resolve_token(&cf, Some("env-token\n")).unwrap(),
            "env-token"
        );
        let e = resolve_token(&cf, None).unwrap_err();
        let msg = format!("{e:#}");
        assert!(
            msg.contains("token_file") && msg.contains(TOKEN_ENV),
            "{msg}"
        );
        let e = resolve_token(&cf, Some("  ")).unwrap_err();
        assert!(format!("{e:#}").contains(TOKEN_ENV));

        let missing = CloudflareConfig {
            token_file: Some(d.path().join("nope.token")),
            ..cf_defaults()
        };
        let e = resolve_token(&missing, None).unwrap_err();
        assert!(format!("{e:#}").contains("nope.token"), "{e:#}");
    }

    #[test]
    fn per_name_is_validated() {
        let d = tempfile::tempdir().unwrap();
        for bad in ["0", "51"] {
            let t = CLOUDFLARE.replace("zone_id", &format!("per_name = {bad}\nzone_id"));
            let e = load_with(&write(&d, &t), None).unwrap_err();
            assert!(format!("{e:#}").contains("per_name"), "{e:#}");
        }
        let t = CLOUDFLARE.replace("zone_id", "per_name = 50\nzone_id");
        let c = load_with(&write(&d, &t), None).unwrap();
        assert_eq!(c.network[0].cloudflare.as_ref().unwrap().per_name, 50);
    }

    fn cf_defaults() -> CloudflareConfig {
        toml::from_str("zone_id = \"z\"").unwrap()
    }

    #[test]
    fn nameserver_or_mailbox_with_cloudflare_is_named_error() {
        let d = tempfile::tempdir().unwrap();
        for (key, val) in [
            ("nameserver", "ns.osmium.space"),
            ("mailbox", "h.osmium.space"),
        ] {
            let t = CLOUDFLARE.replace("publish =", &format!("{key} = \"{val}\"\npublish ="));
            let e = load_with(&write(&d, &t), None).unwrap_err();
            let msg = format!("{e:#}");
            assert!(
                msg.contains(key) && msg.contains("only used by publish"),
                "{msg}"
            );
        }
    }

    #[test]
    fn ttl_below_60_rejected() {
        let d = tempfile::tempdir().unwrap();
        let t = CLOUDFLARE.replace("zone_id", "ttl = 30\nzone_id");
        let e = load_with(&write(&d, &t), None).unwrap_err();
        let msg = format!("{e:#}");
        assert!(msg.contains("ttl") && msg.contains("60"), "{msg}");
        load_with(
            &write(&d, &CLOUDFLARE.replace("zone_id", "ttl = 60\nzone_id")),
            None,
        )
        .unwrap();
        let t = CLOUDFLARE.replace("zone_id", "interval_secs = 0\nzone_id");
        let e = load_with(&write(&d, &t), None).unwrap_err();
        assert!(format!("{e:#}").contains("interval_secs"), "{e:#}");
    }

    #[test]
    fn mixed_modes_share_a_config() {
        let d = tempfile::tempdir().unwrap();
        let second = CLOUDFLARE
            .replace("dnsseed.osmium.space", "other.osmium.space")
            .replace("/var/lib/seedcrystal", "/var/lib/seedcrystal2");
        load_with(&write(&d, &format!("{MINIMAL}{second}")), None).unwrap();
    }

    #[test]
    fn shipped_examples_load() {
        let d = tempfile::tempdir().unwrap();
        for text in [
            include_str!("../examples/osmium.toml"),
            include_str!("../examples/osmium-cloudflare.toml"),
        ] {
            load_with(&write(&d, text), None).unwrap();
        }
    }
}

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    /// Address to listen on.
    pub listen: String,
    /// Directory holding metadata and object data.
    pub data_dir: PathBuf,
    /// Region reported to clients (GetBucketLocation). Any signed region is accepted.
    pub region: String,
    /// Base domain for virtual-host style requests (bucket.domain). Empty disables it.
    pub domain: String,
    /// fsync data and metadata before acknowledging writes.
    pub fsync: bool,
    /// Hours between background integrity scrubs of all data; 0 disables them.
    pub scrub_interval_hours: u64,
    /// Most client connections served at once; further connections wait.
    pub max_connections: usize,
    pub keys: Vec<KeyConfig>,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            listen: "0.0.0.0:9000".into(),
            data_dir: "./data".into(),
            region: "us-east-1".into(),
            domain: String::new(),
            fsync: true,
            scrub_interval_hours: 168,
            max_connections: 4096,
            keys: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyConfig {
    pub name: String,
    pub access_key: String,
    pub secret_key: String,
    /// Buckets this key may access. Empty means all buckets.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub buckets: Vec<String>,
    /// Read-only keys can only GET/HEAD/list.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub read_only: bool,
}

impl KeyConfig {
    pub fn can_access(&self, bucket: &str) -> bool {
        self.buckets.is_empty() || self.buckets.iter().any(|b| b == "*" || b == bucket)
    }
}

impl Config {
    /// Load the config file (missing file = defaults), then apply OBJEX_* env overrides.
    pub fn load(path: &Path) -> Result<Config, String> {
        let mut cfg = match std::fs::read_to_string(path) {
            Ok(s) => toml::from_str::<Config>(&s).map_err(|e| format!("{}: {e}", path.display()))?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Config::default(),
            Err(e) => return Err(format!("{}: {e}", path.display())),
        };
        cfg.apply_env();
        Ok(cfg)
    }

    fn apply_env(&mut self) {
        let env = |k: &str| std::env::var(k).ok().filter(|v| !v.is_empty());
        if let Some(v) = env("OBJEX_LISTEN") {
            self.listen = v;
        }
        if let Some(v) = env("OBJEX_DATA_DIR") {
            self.data_dir = v.into();
        }
        if let Some(v) = env("OBJEX_REGION") {
            self.region = v;
        }
        if let Some(v) = env("OBJEX_DOMAIN") {
            self.domain = v;
        }
        if let Some(v) = env("OBJEX_FSYNC") {
            self.fsync = !matches!(v.to_ascii_lowercase().as_str(), "0" | "false" | "no" | "off");
        }
        if let Some(v) = env("OBJEX_SCRUB_INTERVAL_HOURS").and_then(|v| v.parse().ok()) {
            self.scrub_interval_hours = v;
        }
        if let Some(v) = env("OBJEX_MAX_CONNECTIONS").and_then(|v| v.parse().ok()) {
            self.max_connections = v;
        }
        if let (Some(ak), Some(sk)) = (env("OBJEX_ACCESS_KEY"), env("OBJEX_SECRET_KEY")) {
            self.keys.retain(|k| k.access_key != ak);
            self.keys.push(KeyConfig { name: "env".into(), access_key: ak, secret_key: sk, buckets: vec![], read_only: false });
        }
    }

    /// Only the keys, re-read from disk (used for hot reload).
    pub fn load_keys(path: &Path) -> Result<Vec<KeyConfig>, String> {
        Config::load(path).map(|c| c.keys)
    }
}

pub const DEFAULT_CONFIG: &str = r#"# objex configuration

# Address to listen on
listen = "0.0.0.0:9000"

# Where metadata and object data are stored
data_dir = "./data"

# Region reported to clients. Requests signed for any region (including "auto") are accepted.
region = "us-east-1"

# Base domain for virtual-host style addressing (bucket.s3.example.com). Leave empty to disable.
domain = ""

# fsync data and metadata before acknowledging writes. Disable for speed at the cost of durability.
fsync = true

# Hours between background checks of every stored blob against its checksum (0 = off)
scrub_interval_hours = 168

# Most client connections served at once
max_connections = 4096

# Access keys are managed with `objex key add|list|rm`, and are hot-reloaded by a running server.
# [[keys]]
# name = "admin"
# access_key = "..."
# secret_key = "..."
# buckets = ["photos"]   # optional: restrict to these buckets
# read_only = false      # optional
"#;

const KEY_ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
const SECRET_ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

/// Open a file for writing that only the owner can read (it holds secret keys).
fn open_private(path: &Path, create_new: bool) -> std::io::Result<std::fs::File> {
    let mut o = std::fs::OpenOptions::new();
    o.write(true);
    if create_new {
        o.create_new(true);
    } else {
        o.create(true).truncate(true);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
        o.mode(0o600);
        let f = o.open(path)?;
        // An existing file keeps its old mode; tighten it.
        f.set_permissions(std::fs::Permissions::from_mode(0o600))?;
        Ok(f)
    }
    #[cfg(not(unix))]
    o.open(path)
}

fn write_private(path: &Path, contents: &str, create_new: bool) -> Result<(), String> {
    use std::io::Write;
    let err = |e: std::io::Error| format!("{}: {e}", path.display());
    let mut f = open_private(path, create_new).map_err(err)?;
    f.write_all(contents.as_bytes()).map_err(err)?;
    f.sync_all().map_err(err)
}

fn sync_parent(path: &Path) {
    #[cfg(unix)]
    if let Some(dir) = path.parent() {
        let dir = if dir.as_os_str().is_empty() { Path::new(".") } else { dir };
        if let Ok(d) = std::fs::File::open(dir) {
            let _ = d.sync_all();
        }
    }
}

pub fn init_config(path: &Path) -> Result<(), String> {
    if path.exists() {
        return Err(format!("{} already exists", path.display()));
    }
    write_private(path, DEFAULT_CONFIG, true)?;
    sync_parent(path);
    Ok(())
}

fn read_doc(path: &Path) -> Result<toml_edit::DocumentMut, String> {
    let s = match std::fs::read_to_string(path) {
        Ok(s) => s,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => DEFAULT_CONFIG.to_string(),
        Err(e) => return Err(e.to_string()),
    };
    s.parse::<toml_edit::DocumentMut>().map_err(|e| format!("{}: {e}", path.display()))
}

/// Replace `path` atomically with an owner-only file.
fn write_atomic(path: &Path, contents: &str) -> Result<(), String> {
    let tmp = path.with_extension("toml.tmp");
    write_private(&tmp, contents, false)?;
    std::fs::rename(&tmp, path).map_err(|e| e.to_string())?;
    sync_parent(path);
    Ok(())
}

pub fn add_key(path: &Path, name: &str, buckets: Vec<String>, read_only: bool) -> Result<KeyConfig, String> {
    let mut doc = read_doc(path)?;
    let key = KeyConfig {
        name: name.to_string(),
        access_key: format!("OBX{}", crate::util::random_string(17, KEY_ALPHABET)),
        secret_key: crate::util::random_string(40, SECRET_ALPHABET),
        buckets,
        read_only,
    };
    let keys = doc
        .entry("keys")
        .or_insert_with(|| toml_edit::Item::ArrayOfTables(Default::default()))
        .as_array_of_tables_mut()
        .ok_or("`keys` in config is not an array of tables")?;
    let mut t = toml_edit::Table::new();
    t["name"] = toml_edit::value(&key.name);
    t["access_key"] = toml_edit::value(&key.access_key);
    t["secret_key"] = toml_edit::value(&key.secret_key);
    if !key.buckets.is_empty() {
        let mut arr = toml_edit::Array::new();
        key.buckets.iter().for_each(|b| arr.push(b.as_str()));
        t["buckets"] = toml_edit::value(arr);
    }
    if key.read_only {
        t["read_only"] = toml_edit::value(true);
    }
    keys.push(t);
    write_atomic(path, &doc.to_string())?;
    Ok(key)
}

pub fn remove_key(path: &Path, access_key_or_name: &str) -> Result<usize, String> {
    let mut doc = read_doc(path)?;
    let Some(keys) = doc.get_mut("keys").and_then(|k| k.as_array_of_tables_mut()) else {
        return Ok(0);
    };
    let before = keys.len();
    keys.retain(|t| {
        let is = |f: &str| t.get(f).and_then(|v| v.as_str()) == Some(access_key_or_name);
        !(is("access_key") || is("name"))
    });
    let removed = before - keys.len();
    if removed > 0 {
        write_atomic(path, &doc.to_string())?;
    }
    Ok(removed)
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    /// OBJ-003: files holding secrets are owner-only, whatever the umask.
    #[test]
    fn config_files_are_private() {
        let dir = std::env::temp_dir().join(format!("objex-cfg-{}", crate::util::random_hex(8)));
        std::fs::create_dir_all(&dir).unwrap();
        let mode = |p: &Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;

        let path = dir.join("objex.toml");
        init_config(&path).unwrap();
        assert_eq!(mode(&path), 0o600);
        add_key(&path, "a", vec![], false).unwrap();
        assert_eq!(mode(&path), 0o600);

        // A pre-existing world-readable config is tightened on the next key change.
        let loose = dir.join("loose.toml");
        std::fs::write(&loose, DEFAULT_CONFIG).unwrap();
        std::fs::set_permissions(&loose, std::fs::Permissions::from_mode(0o644)).unwrap();
        add_key(&loose, "b", vec![], false).unwrap();
        assert_eq!(mode(&loose), 0o600);
        assert!(!dir.join("loose.toml.tmp").exists());
        let _ = std::fs::remove_dir_all(dir);
    }
}

use std::env;

/// Central configuration for VaporStore.
/// All values can be overridden via environment variables.
#[derive(Debug, Clone)]
pub struct Config {
    /// TCP port to listen on.
    pub port: u16,
    /// Maximum single-PUT object size in bytes.
    pub max_object_size: usize,
    /// Default TTL for objects in seconds.
    pub default_ttl_seconds: i64,
    /// Interval (seconds) between TTL reaper sweeps.
    pub reaper_interval_seconds: u64,
    /// Maximum number of buckets allowed (0 = unlimited).
    pub max_buckets: usize,
    /// Maximum number of objects per bucket (0 = unlimited).
    pub max_objects_per_bucket: usize,
    /// Enable authentication.
    pub auth_enabled: bool,
    /// Requests per second per IP for rate limiting (0 = disabled).
    pub rate_limit_rps: u64,
    /// Enable disk persistence.
    pub persistence_enabled: bool,
    /// Directory for snapshot and WAL files.
    pub data_dir: String,
    /// Enable Write-Ahead Log (only when persistence is enabled).
    pub wal_enabled: bool,
    /// Interval (seconds) between periodic snapshots (0 = only on shutdown).
    pub snapshot_interval_seconds: u64,
}

impl Config {
    /// Build configuration from environment variables with sensible defaults.
    pub fn from_env() -> Self {
        Self {
            port: parse_env("PORT", 9353),
            max_object_size: parse_env("VAPORSTORE_MAX_OBJECT_SIZE", 5 * 1024 * 1024),
            default_ttl_seconds: parse_env("VAPORSTORE_DEFAULT_TTL", 300),
            reaper_interval_seconds: parse_env("VAPORSTORE_REAPER_INTERVAL", 30),
            max_buckets: parse_env("VAPORSTORE_MAX_BUCKETS", 0),
            max_objects_per_bucket: parse_env("VAPORSTORE_MAX_OBJECTS_PER_BUCKET", 0),
            auth_enabled: parse_env_bool("VAPORSTORE_AUTH", false),
            rate_limit_rps: parse_env("VAPORSTORE_RATE_LIMIT_RPS", 0),
            persistence_enabled: parse_env_bool("VAPORSTORE_PERSISTENCE", false),
            data_dir: env::var("VAPORSTORE_DATA_DIR").unwrap_or_else(|_| "./data".to_string()),
            wal_enabled: parse_env_bool("VAPORSTORE_WAL", true),
            snapshot_interval_seconds: parse_env("VAPORSTORE_SNAPSHOT_INTERVAL", 60),
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            port: 9353,
            max_object_size: 5 * 1024 * 1024,
            default_ttl_seconds: 300,
            reaper_interval_seconds: 30,
            max_buckets: 0,
            max_objects_per_bucket: 0,
            auth_enabled: false,
            rate_limit_rps: 0,
            persistence_enabled: false,
            data_dir: "./data".to_string(),
            wal_enabled: true,
            snapshot_interval_seconds: 60,
        }
    }
}

fn parse_env<T: std::str::FromStr>(key: &str, default: T) -> T {
    env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn parse_env_bool(key: &str, default: bool) -> bool {
    env::var(key)
        .map(|v| v.to_lowercase() == "true" || v == "1")
        .unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let cfg = Config::default();
        assert_eq!(cfg.port, 9353);
        assert_eq!(cfg.max_object_size, 5 * 1024 * 1024);
        assert_eq!(cfg.default_ttl_seconds, 300);
        assert_eq!(cfg.reaper_interval_seconds, 30);
        assert_eq!(cfg.max_buckets, 0);
        assert_eq!(cfg.max_objects_per_bucket, 0);
        assert!(!cfg.auth_enabled);
        assert_eq!(cfg.rate_limit_rps, 0);
    }

    #[test]
    fn test_from_env_uses_defaults() {
        // Without setting any env vars, from_env should use defaults
        let cfg = Config::from_env();
        assert_eq!(cfg.port, 9353);
        assert_eq!(cfg.default_ttl_seconds, 300);
    }
}

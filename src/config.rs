use std::{collections::BTreeMap, env, fmt, time::Duration};

use anyhow::{Context, bail};

use crate::{
    planner::{TRON_CHAIN_ID, default_chain_config},
    warmup::DatalensWarmupConfig,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeConfig {
    pub datalens: DatalensConfig,
    pub warmup: DatalensWarmupConfig,
    pub metrics: MetricsConfig,
    pub database_url: Option<SecretString>,
    pub enabled_chains: Vec<ChainConfig>,
    pub batch_size: u64,
    pub start_block: u64,
    pub finality_mode: FinalityMode,
    pub reorg_window_blocks: u64,
    pub poll_interval: Duration,
}

impl RuntimeConfig {
    pub fn from_env() -> anyhow::Result<Self> {
        let env = env::vars().collect::<BTreeMap<_, _>>();
        Self::from_env_map(&env)
    }

    pub fn database_url_from_env() -> anyhow::Result<SecretString> {
        let env = env::vars().collect::<BTreeMap<_, _>>();
        Self::database_url_from_env_map(&env)
    }

    pub fn database_url_from_env_map(
        env: &BTreeMap<String, String>,
    ) -> anyhow::Result<SecretString> {
        optional_env(env, "ORMPINDEXER_DATABASE_URL")
            .map(SecretString::new)
            .context("ORMPINDEXER_DATABASE_URL must be configured")
    }

    pub fn from_env_map(env: &BTreeMap<String, String>) -> anyhow::Result<Self> {
        let start_block = optional_u64(env, "ORMPINDEXER_START_BLOCK")?;
        let batch_size = optional_u64(env, "ORMPINDEXER_BATCH_SIZE")?.unwrap_or(1_000);
        if batch_size == 0 {
            bail!("ORMPINDEXER_BATCH_SIZE must be greater than zero");
        }
        let chain_ids = required_list(env, "ORMPINDEXER_ENABLED_CHAINS")?
            .into_iter()
            .map(|value| {
                value
                    .parse::<u64>()
                    .with_context(|| format!("parse ORMPINDEXER_ENABLED_CHAINS item: {value}"))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;

        let finality_mode = optional_env(env, "ORMPINDEXER_FINALITY_MODE")
            .as_deref()
            .map(str::parse)
            .transpose()?
            .unwrap_or(FinalityMode::Finalized);

        let enabled_chains = chain_ids
            .into_iter()
            .map(|chain_id| {
                ChainConfig::from_env_map(env, chain_id, start_block, batch_size, finality_mode)
            })
            .collect::<anyhow::Result<Vec<_>>>()?;

        let warmup_chunk_size =
            optional_u64(env, "ORMPINDEXER_DATALENS_WARMUP_CHUNK_SIZE")?.unwrap_or(batch_size);
        if warmup_chunk_size == 0 {
            bail!("ORMPINDEXER_DATALENS_WARMUP_CHUNK_SIZE must be greater than zero");
        }
        let datalens_timeout_secs =
            optional_u64(env, "ORMPINDEXER_DATALENS_TIMEOUT_SECS")?.unwrap_or(300);
        if datalens_timeout_secs == 0 {
            bail!("ORMPINDEXER_DATALENS_TIMEOUT_SECS must be greater than zero");
        }
        let datalens_query_max_attempts =
            optional_u64(env, "ORMPINDEXER_DATALENS_QUERY_MAX_ATTEMPTS")?.unwrap_or(3);
        if datalens_query_max_attempts == 0 {
            bail!("ORMPINDEXER_DATALENS_QUERY_MAX_ATTEMPTS must be greater than zero");
        }
        let datalens_head_buffer_blocks =
            optional_u64(env, "ORMPINDEXER_DATALENS_HEAD_BUFFER_BLOCKS")?.unwrap_or(1);
        let datalens_min_request_interval_ms =
            optional_u64(env, "ORMPINDEXER_DATALENS_MIN_REQUEST_INTERVAL_MS")?.unwrap_or(100);
        let reorg_window_blocks =
            optional_u64(env, "ORMPINDEXER_REORG_WINDOW_BLOCKS")?.unwrap_or(128);
        if reorg_window_blocks == 0
            && enabled_chains
                .iter()
                .any(|chain| chain.finality_mode.uses_reorg_protection())
        {
            bail!("ORMPINDEXER_REORG_WINDOW_BLOCKS must be greater than zero");
        }
        let metrics = MetricsConfig::from_env_map(env)?;

        Ok(Self {
            datalens: DatalensConfig {
                endpoint: optional_env(env, "ORMPINDEXER_DATALENS_ENDPOINT")
                    .unwrap_or_else(|| "http://localhost:8080".to_owned()),
                application: optional_env(env, "ORMPINDEXER_DATALENS_APPLICATION")
                    .unwrap_or_else(|| "ormpindexer".to_owned()),
                token: optional_env(env, "ORMPINDEXER_DATALENS_TOKEN").map(SecretString::new),
                timeout: Duration::from_secs(datalens_timeout_secs),
                query_max_attempts: datalens_query_max_attempts,
                head_buffer_blocks: datalens_head_buffer_blocks,
                min_request_interval: Duration::from_millis(datalens_min_request_interval_ms),
            },
            warmup: DatalensWarmupConfig {
                enabled: optional_bool(env, "ORMPINDEXER_DATALENS_WARMUP_ENABLED")?.unwrap_or(true),
                ensure_on_startup: optional_bool(
                    env,
                    "ORMPINDEXER_DATALENS_WARMUP_ENSURE_ON_STARTUP",
                )?
                .unwrap_or(true),
                required: optional_bool(env, "ORMPINDEXER_DATALENS_WARMUP_REQUIRED")?
                    .unwrap_or(false),
                chunk_size: warmup_chunk_size,
                end_block: optional_u64(env, "ORMPINDEXER_DATALENS_WARMUP_END_BLOCK")?,
            },
            metrics,
            database_url: optional_env(env, "ORMPINDEXER_DATABASE_URL").map(SecretString::new),
            enabled_chains,
            batch_size,
            start_block: start_block.unwrap_or(0),
            finality_mode,
            reorg_window_blocks,
            poll_interval: Duration::from_secs(
                optional_u64(env, "ORMPINDEXER_POLL_INTERVAL_SECS")?.unwrap_or(30),
            ),
        })
    }

    pub fn chain(&self, chain_id: u64) -> Option<&ChainConfig> {
        self.enabled_chains
            .iter()
            .find(|chain| chain.chain_id == chain_id)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetricsConfig {
    pub refresh_delay: Duration,
    pub refresh_timeout: Duration,
}

impl MetricsConfig {
    pub fn from_env() -> anyhow::Result<Self> {
        let env = env::vars().collect::<BTreeMap<_, _>>();
        Self::from_env_map(&env)
    }

    pub fn from_env_map(env: &BTreeMap<String, String>) -> anyhow::Result<Self> {
        let refresh_delay_ms =
            optional_u64(env, "ORMPINDEXER_METRICS_REFRESH_DELAY_MS")?.unwrap_or(5_000);
        if refresh_delay_ms == 0 {
            bail!("ORMPINDEXER_METRICS_REFRESH_DELAY_MS must be greater than zero");
        }
        let refresh_timeout_ms =
            optional_u64(env, "ORMPINDEXER_METRICS_REFRESH_TIMEOUT_MS")?.unwrap_or(10_000);
        if refresh_timeout_ms == 0 {
            bail!("ORMPINDEXER_METRICS_REFRESH_TIMEOUT_MS must be greater than zero");
        }

        Ok(Self {
            refresh_delay: Duration::from_millis(refresh_delay_ms),
            refresh_timeout: Duration::from_millis(refresh_timeout_ms),
        })
    }
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            refresh_delay: Duration::from_secs(5),
            refresh_timeout: Duration::from_secs(10),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DatalensConfig {
    pub endpoint: String,
    pub application: String,
    pub token: Option<SecretString>,
    pub timeout: Duration,
    pub query_max_attempts: u64,
    pub head_buffer_blocks: u64,
    pub min_request_interval: Duration,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChainConfig {
    pub chain_id: u64,
    pub start_block: u64,
    pub batch_size: u64,
    pub contracts: Vec<String>,
    pub topics: Vec<String>,
    pub finality_mode: FinalityMode,
}

impl ChainConfig {
    fn from_env_map(
        env: &BTreeMap<String, String>,
        chain_id: u64,
        default_start_block: Option<u64>,
        default_batch_size: u64,
        default_finality_mode: FinalityMode,
    ) -> anyhow::Result<Self> {
        let prefix = format!("ORMPINDEXER_CHAIN_{chain_id}");
        let default = default_chain_config(chain_id)?;
        let contracts = optional_list(env, &format!("{prefix}_CONTRACTS"));
        let topics = optional_list(env, &format!("{prefix}_TOPICS"));
        let chain_start_block = optional_u64(env, &format!("{prefix}_START_BLOCK"))?;
        let start_block = if chain_id == TRON_CHAIN_ID {
            chain_start_block
                .with_context(|| format!("{prefix}_START_BLOCK must be configured for Tron"))?
        } else {
            chain_start_block
                .or(default_start_block)
                .unwrap_or(default.start_block)
        };
        let batch_size =
            optional_u64(env, &format!("{prefix}_BATCH_SIZE"))?.unwrap_or(default_batch_size);
        if batch_size == 0 {
            bail!("{prefix}_BATCH_SIZE must be greater than zero");
        }
        let finality_mode = optional_env(env, &format!("{prefix}_FINALITY_MODE"))
            .as_deref()
            .map(str::parse)
            .transpose()?
            .unwrap_or(default_finality_mode);

        Ok(Self {
            chain_id,
            start_block,
            batch_size,
            contracts: if contracts.is_empty() {
                default.contracts
            } else {
                contracts
            },
            topics: if topics.is_empty() {
                default.topics
            } else {
                topics
            },
            finality_mode,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FinalityMode {
    Finalized,
    Durable,
    Safe,
    Latest,
}

impl FinalityMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Finalized => "finalized",
            Self::Durable => "durable",
            Self::Safe => "safe",
            Self::Latest => "latest",
        }
    }

    pub fn uses_reorg_protection(self) -> bool {
        matches!(self, Self::Safe | Self::Latest)
    }
}

impl std::str::FromStr for FinalityMode {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "finalized" => Ok(Self::Finalized),
            "durable" => Ok(Self::Durable),
            "safe" => Ok(Self::Safe),
            "latest" => Ok(Self::Latest),
            _ => bail!("ORMPINDEXER_FINALITY_MODE must be finalized, durable, safe, or latest"),
        }
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct SecretString(String);

impl SecretString {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn expose_secret(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SecretString {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("<redacted>")
    }
}

fn optional_env(env: &BTreeMap<String, String>, key: &str) -> Option<String> {
    env.get(key)
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn optional_u64(env: &BTreeMap<String, String>, key: &str) -> anyhow::Result<Option<u64>> {
    optional_env(env, key)
        .map(|value| {
            value
                .parse::<u64>()
                .with_context(|| format!("parse {key} as u64"))
        })
        .transpose()
}

fn optional_bool(env: &BTreeMap<String, String>, key: &str) -> anyhow::Result<Option<bool>> {
    optional_env(env, key)
        .map(|value| match value.as_str() {
            "true" | "1" | "yes" => Ok(true),
            "false" | "0" | "no" => Ok(false),
            _ => bail!("{key} must be true or false"),
        })
        .transpose()
}

fn optional_list(env: &BTreeMap<String, String>, key: &str) -> Vec<String> {
    optional_env(env, key)
        .map(|value| split_list(&value))
        .unwrap_or_default()
}

fn required_list(env: &BTreeMap<String, String>, key: &str) -> anyhow::Result<Vec<String>> {
    let values = optional_list(env, key);
    if values.is_empty() {
        bail!("{key} must be configured");
    }
    Ok(values)
}

fn split_list(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

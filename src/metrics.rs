use std::{
    collections::BTreeMap,
    sync::{Mutex, OnceLock},
    time::{SystemTime, UNIX_EPOCH},
};

use crate::config::RuntimeConfig;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ConfiguredScopeMetricsRow {
    pub chain_id: u64,
    pub dataset: String,
    pub start_block: u64,
    pub batch_size: u64,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct RuntimeMetricsRow {
    pub chain_id: u64,
    pub dataset: String,
    pub target_block: Option<u64>,
    pub remaining_blocks: Option<u64>,
    pub datalens_head_height: Option<u64>,
    pub datalens_head_observed_timestamp_seconds: Option<f64>,
    pub datalens_head_advanced_timestamp_seconds: Option<f64>,
    pub chain_last_success_timestamp_seconds: Option<f64>,
    pub checkpoint_last_forward_advance_timestamp_seconds: Option<f64>,
    pub consecutive_failures: u64,
    pub chain_pass_success_total: u64,
    pub chain_pass_failure_total: u64,
    pub ranges_success_total: u64,
    pub ranges_failure_total: u64,
    pub blocks_processed_total: u64,
    pub records_read_total: u64,
    pub records_decoded_total: u64,
    pub records_written_total: u64,
    pub batch_duration_seconds_sum: f64,
    pub batch_duration_seconds_count: u64,
    pub datalens_log_requests_success_total: u64,
    pub datalens_log_requests_failure_total: u64,
    pub datalens_block_requests_success_total: u64,
    pub datalens_block_requests_failure_total: u64,
    pub datalens_tx_requests_success_total: u64,
    pub datalens_tx_requests_failure_total: u64,
    pub reorg_rollbacks_total: u64,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct MetricsSnapshot {
    pub configured_scopes: Vec<ConfiguredScopeMetricsRow>,
    pub runtime_rows: Vec<RuntimeMetricsRow>,
}

pub struct RangeSuccessMetrics<'a> {
    pub chain_id: u64,
    pub dataset: &'a str,
    pub blocks_processed: u64,
    pub records_read: u64,
    pub records_decoded: u64,
    pub records_written: u64,
    pub remaining_blocks: u64,
    pub duration_seconds: f64,
}

#[derive(Clone, Debug, Default)]
struct MetricsState {
    configured_scopes: BTreeMap<MetricsScopeKey, ConfiguredScopeMetricsRow>,
    runtime_rows: BTreeMap<MetricsScopeKey, RuntimeMetricsRow>,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct MetricsScopeKey {
    chain_id: u64,
    dataset: String,
}

static METRICS: OnceLock<Mutex<MetricsState>> = OnceLock::new();

pub fn record_configured_scopes(config: &RuntimeConfig) {
    let mut state = metrics_state().lock().expect("ORMP metrics mutex");
    for chain in &config.enabled_chains {
        let Ok(dataset) = crate::planner::chain_dataset(chain.chain_id) else {
            continue;
        };
        let key = MetricsScopeKey {
            chain_id: chain.chain_id,
            dataset: dataset.to_owned(),
        };
        state.configured_scopes.insert(
            key,
            ConfiguredScopeMetricsRow {
                chain_id: chain.chain_id,
                dataset: dataset.to_owned(),
                start_block: chain.start_block,
                batch_size: chain.batch_size,
            },
        );
    }
}

pub fn record_chain_head(chain_id: u64, dataset: &str, latest_block: u64, target_block: u64) {
    let now = unix_timestamp_seconds();
    let row = runtime_row(chain_id, dataset);
    let mut state = metrics_state().lock().expect("ORMP metrics mutex");
    let row = state.runtime_rows.entry(row.0).or_insert(row.1);
    let advanced = row
        .datalens_head_height
        .is_none_or(|previous| latest_block > previous);
    row.datalens_head_height = Some(latest_block);
    row.target_block = Some(target_block);
    row.datalens_head_observed_timestamp_seconds = Some(now);
    if advanced {
        row.datalens_head_advanced_timestamp_seconds = Some(now);
    }
}

pub fn record_chain_pass(chain_id: u64, dataset: &str, success: bool, consecutive_failures: u64) {
    let row = runtime_row(chain_id, dataset);
    let mut state = metrics_state().lock().expect("ORMP metrics mutex");
    let row = state.runtime_rows.entry(row.0).or_insert(row.1);
    row.consecutive_failures = consecutive_failures;
    if success {
        row.chain_pass_success_total = row.chain_pass_success_total.saturating_add(1);
        row.chain_last_success_timestamp_seconds = Some(unix_timestamp_seconds());
    } else {
        row.chain_pass_failure_total = row.chain_pass_failure_total.saturating_add(1);
    }
}

pub fn record_remaining_blocks(chain_id: u64, dataset: &str, remaining_blocks: u64) {
    let row = runtime_row(chain_id, dataset);
    let mut state = metrics_state().lock().expect("ORMP metrics mutex");
    let row = state.runtime_rows.entry(row.0).or_insert(row.1);
    row.remaining_blocks = Some(remaining_blocks);
}

pub fn record_range_success(metrics: RangeSuccessMetrics<'_>) {
    let row = runtime_row(metrics.chain_id, metrics.dataset);
    let mut state = metrics_state().lock().expect("ORMP metrics mutex");
    let row = state.runtime_rows.entry(row.0).or_insert(row.1);
    row.ranges_success_total = row.ranges_success_total.saturating_add(1);
    row.blocks_processed_total = row
        .blocks_processed_total
        .saturating_add(metrics.blocks_processed);
    row.records_read_total = row.records_read_total.saturating_add(metrics.records_read);
    row.records_decoded_total = row
        .records_decoded_total
        .saturating_add(metrics.records_decoded);
    row.records_written_total = row
        .records_written_total
        .saturating_add(metrics.records_written);
    row.batch_duration_seconds_sum += metrics.duration_seconds;
    row.batch_duration_seconds_count = row.batch_duration_seconds_count.saturating_add(1);
    row.remaining_blocks = Some(metrics.remaining_blocks);
    row.checkpoint_last_forward_advance_timestamp_seconds = Some(unix_timestamp_seconds());
}

pub fn record_range_failure(chain_id: u64, dataset: &str) {
    let row = runtime_row(chain_id, dataset);
    let mut state = metrics_state().lock().expect("ORMP metrics mutex");
    let row = state.runtime_rows.entry(row.0).or_insert(row.1);
    row.ranges_failure_total = row.ranges_failure_total.saturating_add(1);
}

pub fn record_datalens_request(chain_id: u64, dataset: &str, operation: &str, success: bool) {
    let row = runtime_row(chain_id, dataset);
    let mut state = metrics_state().lock().expect("ORMP metrics mutex");
    let row = state.runtime_rows.entry(row.0).or_insert(row.1);
    match (operation, success) {
        ("logs", true) => {
            row.datalens_log_requests_success_total =
                row.datalens_log_requests_success_total.saturating_add(1)
        }
        ("logs", false) => {
            row.datalens_log_requests_failure_total =
                row.datalens_log_requests_failure_total.saturating_add(1)
        }
        ("blocks", true) => {
            row.datalens_block_requests_success_total =
                row.datalens_block_requests_success_total.saturating_add(1)
        }
        ("blocks", false) => {
            row.datalens_block_requests_failure_total =
                row.datalens_block_requests_failure_total.saturating_add(1)
        }
        ("transactions", true) => {
            row.datalens_tx_requests_success_total =
                row.datalens_tx_requests_success_total.saturating_add(1)
        }
        ("transactions", false) => {
            row.datalens_tx_requests_failure_total =
                row.datalens_tx_requests_failure_total.saturating_add(1)
        }
        _ => {}
    }
}

pub fn record_reorg_rollback(chain_id: u64, dataset: &str) {
    let row = runtime_row(chain_id, dataset);
    let mut state = metrics_state().lock().expect("ORMP metrics mutex");
    let row = state.runtime_rows.entry(row.0).or_insert(row.1);
    row.reorg_rollbacks_total = row.reorg_rollbacks_total.saturating_add(1);
}

pub fn snapshot() -> MetricsSnapshot {
    let state = metrics_state().lock().expect("ORMP metrics mutex");
    MetricsSnapshot {
        configured_scopes: state.configured_scopes.values().cloned().collect(),
        runtime_rows: state.runtime_rows.values().cloned().collect(),
    }
}

fn runtime_row(chain_id: u64, dataset: &str) -> (MetricsScopeKey, RuntimeMetricsRow) {
    let dataset = dataset.to_owned();
    (
        MetricsScopeKey {
            chain_id,
            dataset: dataset.clone(),
        },
        RuntimeMetricsRow {
            chain_id,
            dataset,
            ..Default::default()
        },
    )
}

fn metrics_state() -> &'static Mutex<MetricsState> {
    METRICS.get_or_init(|| Mutex::new(MetricsState::default()))
}

fn unix_timestamp_seconds() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

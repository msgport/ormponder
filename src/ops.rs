use std::{
    sync::Arc,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use serde::Serialize;
use sqlx::{FromRow, PgPool};
use tokio::{
    sync::{Mutex, RwLock},
    time,
};

use crate::config::MetricsConfig;

const LEGACY_TABLES: &[&str] = &[
    "ormp_hash_imported",
    "ormp_message_accepted",
    "ormp_message_assigned",
    "ormp_message_dispatched",
    "msgport_message_recv",
    "msgport_message_sent",
    "signature_pub_signature_submittion",
];

#[derive(Clone, Debug, Eq, PartialEq, FromRow, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CheckpointRow {
    pub chain_id: String,
    pub dataset: String,
    pub next_block: String,
    pub updated_at: String,
    #[serde(skip_serializing)]
    pub updated_timestamp_seconds: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, FromRow, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChainProgressRow {
    pub chain_id: String,
    pub datasets: i64,
    pub min_next_block: String,
    pub max_next_block: String,
    pub updated_at: String,
}

#[derive(Clone, Debug, Eq, PartialEq, FromRow, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DatasetProgressRow {
    pub dataset: String,
    pub chains: i64,
    pub min_next_block: String,
    pub max_next_block: String,
    pub updated_at: String,
}

#[derive(Clone, Debug, Eq, PartialEq, FromRow)]
pub struct LegacyTableRowCount {
    pub table_name: String,
    pub row_count: i64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct DbMetricsSnapshot {
    checkpoints: Vec<CheckpointRow>,
    row_counts: Vec<LegacyTableRowCount>,
}

#[derive(Clone, Debug, PartialEq)]
struct MetricsCacheStatus {
    last_success_timestamp_seconds: Option<f64>,
    snapshot_age_seconds: Option<f64>,
    last_refresh_duration_seconds: Option<f64>,
    last_refresh_success: bool,
    refresh_errors_total: u64,
    stale: bool,
}

impl MetricsCacheStatus {
    fn for_direct_render() -> Self {
        Self {
            last_success_timestamp_seconds: None,
            snapshot_age_seconds: None,
            last_refresh_duration_seconds: None,
            last_refresh_success: true,
            refresh_errors_total: 0,
            stale: false,
        }
    }
}

#[derive(Clone, Debug, Default)]
struct MetricsCacheState {
    snapshot: Option<DbMetricsSnapshot>,
    last_success_at: Option<Instant>,
    last_success_timestamp_seconds: Option<f64>,
    last_refresh_duration_seconds: Option<f64>,
    last_refresh_success: bool,
    refresh_errors_total: u64,
    last_error: Option<String>,
}

#[derive(Clone, Debug)]
pub struct MetricsCache {
    state: Arc<RwLock<MetricsCacheState>>,
    refresh_lock: Arc<Mutex<()>>,
    refresh_delay: std::time::Duration,
    refresh_timeout: std::time::Duration,
    stale_after: std::time::Duration,
}

impl MetricsCache {
    pub fn new(config: MetricsConfig) -> Self {
        Self {
            state: Arc::new(RwLock::new(MetricsCacheState {
                last_refresh_success: true,
                ..Default::default()
            })),
            refresh_lock: Arc::new(Mutex::new(())),
            refresh_delay: config.refresh_delay,
            refresh_timeout: config.refresh_timeout,
            stale_after: config.refresh_delay * 3,
        }
    }

    pub fn spawn_refresh_loop(&self, pool: PgPool) {
        let cache = self.clone();
        tokio::spawn(async move {
            loop {
                cache.refresh(&pool, true).await;
                time::sleep(cache.refresh_delay).await;
            }
        });
    }

    pub async fn render(&self, pool: &PgPool) -> anyhow::Result<String> {
        if self.state.read().await.snapshot.is_none() {
            self.refresh(pool, false).await;
        }

        let (snapshot, status, last_error) = self.snapshot().await;
        match snapshot {
            Some(snapshot) => Ok(format_metrics_with_status(
                &snapshot.checkpoints,
                &snapshot.row_counts,
                &crate::metrics::snapshot(),
                &status,
            )),
            None => Err(anyhow::anyhow!(
                "metrics snapshot unavailable{}",
                last_error
                    .map(|error| format!(": {error}"))
                    .unwrap_or_default()
            )),
        }
    }

    async fn refresh(&self, pool: &PgPool, force: bool) {
        let _guard = self.refresh_lock.lock().await;
        if !force && self.state.read().await.snapshot.is_some() {
            return;
        }

        let started_at = Instant::now();
        let result = time::timeout(self.refresh_timeout, collect_db_metrics_snapshot(pool)).await;
        match result {
            Ok(Ok(snapshot)) => {
                let mut state = self.state.write().await;
                state.snapshot = Some(snapshot);
                state.last_success_at = Some(Instant::now());
                state.last_success_timestamp_seconds = Some(unix_timestamp_seconds());
                state.last_refresh_duration_seconds = Some(started_at.elapsed().as_secs_f64());
                state.last_refresh_success = true;
                state.last_error = None;
            }
            Ok(Err(error)) => {
                self.record_refresh_failure(started_at, error.to_string())
                    .await;
            }
            Err(_) => {
                self.record_refresh_failure(
                    started_at,
                    format!(
                        "collect ORMP indexer metrics exceeded {}s timeout",
                        self.refresh_timeout.as_secs_f64()
                    ),
                )
                .await;
            }
        }
    }

    async fn record_refresh_failure(&self, started_at: Instant, error: String) {
        let mut state = self.state.write().await;
        state.last_refresh_duration_seconds = Some(started_at.elapsed().as_secs_f64());
        state.last_refresh_success = false;
        state.refresh_errors_total = state.refresh_errors_total.saturating_add(1);
        state.last_error = Some(error);
    }

    async fn snapshot(
        &self,
    ) -> (
        Option<DbMetricsSnapshot>,
        MetricsCacheStatus,
        Option<String>,
    ) {
        let state = self.state.read().await;
        let snapshot_age_seconds = state
            .last_success_at
            .map(|last_success_at| last_success_at.elapsed().as_secs_f64());
        let status = MetricsCacheStatus {
            last_success_timestamp_seconds: state.last_success_timestamp_seconds,
            snapshot_age_seconds,
            last_refresh_duration_seconds: state.last_refresh_duration_seconds,
            last_refresh_success: state.last_refresh_success,
            refresh_errors_total: state.refresh_errors_total,
            stale: snapshot_age_seconds
                .map(|age| age > self.stale_after.as_secs_f64())
                .unwrap_or(true),
        };
        (state.snapshot.clone(), status, state.last_error.clone())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StatusResponse {
    pub checkpoints: Vec<CheckpointRow>,
    pub progress: ProgressSummary,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProgressSummary {
    pub chains: Vec<ChainProgressRow>,
    pub datasets: Vec<DatasetProgressRow>,
}

pub async fn check_readiness(pool: &PgPool) -> anyhow::Result<()> {
    sqlx::query("SELECT 1").execute(pool).await?;
    Ok(())
}

pub async fn load_status(pool: &PgPool) -> anyhow::Result<StatusResponse> {
    let checkpoints = sqlx::query_as::<_, CheckpointRow>(
        "SELECT
            chain_id::TEXT AS chain_id,
            dataset,
            next_block::TEXT AS next_block,
            updated_at::TEXT AS updated_at,
            EXTRACT(EPOCH FROM updated_at)::TEXT AS updated_timestamp_seconds
         FROM ormp_indexer_checkpoint
         ORDER BY chain_id::NUMERIC, dataset",
    )
    .fetch_all(pool)
    .await?;

    let chains = sqlx::query_as::<_, ChainProgressRow>(
        "SELECT
            chain_id::TEXT AS chain_id,
            COUNT(*)::BIGINT AS datasets,
            MIN(next_block)::TEXT AS min_next_block,
            MAX(next_block)::TEXT AS max_next_block,
            MAX(updated_at)::TEXT AS updated_at
         FROM ormp_indexer_checkpoint
         GROUP BY chain_id
         ORDER BY chain_id::NUMERIC",
    )
    .fetch_all(pool)
    .await?;

    let datasets = sqlx::query_as::<_, DatasetProgressRow>(
        "SELECT
            dataset,
            COUNT(*)::BIGINT AS chains,
            MIN(next_block)::TEXT AS min_next_block,
            MAX(next_block)::TEXT AS max_next_block,
            MAX(updated_at)::TEXT AS updated_at
         FROM ormp_indexer_checkpoint
         GROUP BY dataset
         ORDER BY dataset",
    )
    .fetch_all(pool)
    .await?;

    Ok(StatusResponse {
        checkpoints,
        progress: ProgressSummary { chains, datasets },
    })
}

pub async fn render_metrics(pool: &PgPool) -> anyhow::Result<String> {
    let snapshot = collect_db_metrics_snapshot(pool).await?;
    Ok(format_metrics_with_status(
        &snapshot.checkpoints,
        &snapshot.row_counts,
        &crate::metrics::snapshot(),
        &MetricsCacheStatus::for_direct_render(),
    ))
}

async fn collect_db_metrics_snapshot(pool: &PgPool) -> anyhow::Result<DbMetricsSnapshot> {
    let checkpoints = sqlx::query_as::<_, CheckpointRow>(
        "SELECT
            chain_id::TEXT AS chain_id,
            dataset,
            next_block::TEXT AS next_block,
            updated_at::TEXT AS updated_at,
            EXTRACT(EPOCH FROM updated_at)::TEXT AS updated_timestamp_seconds
         FROM ormp_indexer_checkpoint
         ORDER BY chain_id::NUMERIC, dataset",
    )
    .fetch_all(pool)
    .await?;

    let row_counts = sqlx::query_as::<_, LegacyTableRowCount>(
        "SELECT table_name, row_count
         FROM (
           SELECT 'ormp_hash_imported'::TEXT AS table_name, COUNT(*)::BIGINT AS row_count FROM ormp_hash_imported
           UNION ALL
           SELECT 'ormp_message_accepted', COUNT(*)::BIGINT FROM ormp_message_accepted
           UNION ALL
           SELECT 'ormp_message_assigned', COUNT(*)::BIGINT FROM ormp_message_assigned
           UNION ALL
           SELECT 'ormp_message_dispatched', COUNT(*)::BIGINT FROM ormp_message_dispatched
           UNION ALL
           SELECT 'msgport_message_recv', COUNT(*)::BIGINT FROM msgport_message_recv
           UNION ALL
           SELECT 'msgport_message_sent', COUNT(*)::BIGINT FROM msgport_message_sent
           UNION ALL
           SELECT 'signature_pub_signature_submittion', COUNT(*)::BIGINT FROM signature_pub_signature_submittion
         ) counts",
    )
    .fetch_all(pool)
    .await?;

    Ok(DbMetricsSnapshot {
        checkpoints,
        row_counts,
    })
}

#[cfg(test)]
fn format_metrics(
    checkpoints: &[CheckpointRow],
    row_counts: &[LegacyTableRowCount],
    runtime: &crate::metrics::MetricsSnapshot,
) -> String {
    format_metrics_with_status(
        checkpoints,
        row_counts,
        runtime,
        &MetricsCacheStatus::for_direct_render(),
    )
}

fn format_metrics_with_status(
    checkpoints: &[CheckpointRow],
    row_counts: &[LegacyTableRowCount],
    runtime: &crate::metrics::MetricsSnapshot,
    cache_status: &MetricsCacheStatus,
) -> String {
    let mut body = String::new();

    body.push_str("# HELP ormp_metrics_snapshot_last_success_timestamp_seconds Unix timestamp of the last successful DB-backed ORMP metrics snapshot refresh.\n");
    body.push_str("# TYPE ormp_metrics_snapshot_last_success_timestamp_seconds gauge\n");
    body.push_str("# HELP ormp_metrics_snapshot_age_seconds Seconds since the last successful DB-backed ORMP metrics snapshot refresh.\n");
    body.push_str("# TYPE ormp_metrics_snapshot_age_seconds gauge\n");
    body.push_str("# HELP ormp_metrics_refresh_duration_seconds Duration of the most recent DB-backed ORMP metrics snapshot refresh.\n");
    body.push_str("# TYPE ormp_metrics_refresh_duration_seconds gauge\n");
    body.push_str("# HELP ormp_metrics_refresh_success Whether the most recent DB-backed ORMP metrics snapshot refresh succeeded.\n");
    body.push_str("# TYPE ormp_metrics_refresh_success gauge\n");
    body.push_str("# HELP ormp_metrics_refresh_errors_total Failed DB-backed ORMP metrics snapshot refresh attempts.\n");
    body.push_str("# TYPE ormp_metrics_refresh_errors_total counter\n");
    body.push_str("# HELP ormp_metrics_snapshot_stale Whether the DB-backed ORMP metrics snapshot is older than the configured freshness threshold.\n");
    body.push_str("# TYPE ormp_metrics_snapshot_stale gauge\n");
    append_optional_metric(
        &mut body,
        "ormp_metrics_snapshot_last_success_timestamp_seconds",
        &[],
        cache_status.last_success_timestamp_seconds,
    );
    append_optional_metric(
        &mut body,
        "ormp_metrics_snapshot_age_seconds",
        &[],
        cache_status.snapshot_age_seconds,
    );
    append_optional_metric(
        &mut body,
        "ormp_metrics_refresh_duration_seconds",
        &[],
        cache_status.last_refresh_duration_seconds,
    );
    append_metric(
        &mut body,
        "ormp_metrics_refresh_success",
        &[],
        if cache_status.last_refresh_success {
            1_u64
        } else {
            0_u64
        },
    );
    append_metric(
        &mut body,
        "ormp_metrics_refresh_errors_total",
        &[],
        cache_status.refresh_errors_total,
    );
    append_metric(
        &mut body,
        "ormp_metrics_snapshot_stale",
        &[],
        if cache_status.stale { 1_u64 } else { 0_u64 },
    );

    body.push_str(
        "# HELP ormp_indexer_checkpoint_next_block Next block recorded per chain and dataset.\n",
    );
    body.push_str("# TYPE ormp_indexer_checkpoint_next_block gauge\n");
    body.push_str("# HELP ormp_indexer_checkpoint_updated_timestamp_seconds Unix timestamp of the checkpoint row update.\n");
    body.push_str("# TYPE ormp_indexer_checkpoint_updated_timestamp_seconds gauge\n");
    for checkpoint in checkpoints {
        body.push_str("ormp_indexer_checkpoint_next_block{chain_id=\"");
        body.push_str(&escape_label_value(&checkpoint.chain_id));
        body.push_str("\",dataset=\"");
        body.push_str(&escape_label_value(&checkpoint.dataset));
        body.push_str("\"} ");
        body.push_str(&checkpoint.next_block);
        body.push('\n');
        append_optional_metric(
            &mut body,
            "ormp_indexer_checkpoint_updated_timestamp_seconds",
            &[
                ("chain_id", checkpoint.chain_id.clone()),
                ("dataset", checkpoint.dataset.clone()),
            ],
            checkpoint.updated_timestamp_seconds.as_ref(),
        );
    }

    body.push_str("# HELP ormp_indexer_checkpoint_rows Total checkpoint rows.\n");
    body.push_str("# TYPE ormp_indexer_checkpoint_rows gauge\n");
    body.push_str("ormp_indexer_checkpoint_rows ");
    body.push_str(&checkpoints.len().to_string());
    body.push('\n');

    body.push_str(
        "# HELP ormp_indexer_configured_scope_info Configured ORMP indexer scope information.\n",
    );
    body.push_str("# TYPE ormp_indexer_configured_scope_info gauge\n");
    body.push_str(
        "# HELP ormp_indexer_checkpoint_present Whether the configured ORMP indexer scope has a checkpoint row.\n",
    );
    body.push_str("# TYPE ormp_indexer_checkpoint_present gauge\n");
    for scope in &runtime.configured_scopes {
        body.push_str("ormp_indexer_configured_scope_info{chain_id=\"");
        body.push_str(&scope.chain_id.to_string());
        body.push_str("\",dataset=\"");
        body.push_str(&escape_label_value(&scope.dataset));
        body.push_str("\",start_block=\"");
        body.push_str(&scope.start_block.to_string());
        body.push_str("\",batch_size=\"");
        body.push_str(&scope.batch_size.to_string());
        body.push_str("\"} 1\n");

        let checkpoint_present = checkpoints.iter().any(|checkpoint| {
            checkpoint.chain_id == scope.chain_id.to_string() && checkpoint.dataset == scope.dataset
        });
        body.push_str("ormp_indexer_checkpoint_present{chain_id=\"");
        body.push_str(&scope.chain_id.to_string());
        body.push_str("\",dataset=\"");
        body.push_str(&escape_label_value(&scope.dataset));
        body.push_str("\"} ");
        body.push_str(if checkpoint_present { "1\n" } else { "0\n" });
    }

    for checkpoint in checkpoints {
        if runtime.configured_scopes.iter().any(|scope| {
            checkpoint.chain_id == scope.chain_id.to_string() && checkpoint.dataset == scope.dataset
        }) {
            continue;
        }
        body.push_str("ormp_indexer_checkpoint_present{chain_id=\"");
        body.push_str(&escape_label_value(&checkpoint.chain_id));
        body.push_str("\",dataset=\"");
        body.push_str(&escape_label_value(&checkpoint.dataset));
        body.push_str("\"} 1\n");
    }

    body.push_str("# HELP ormp_indexer_target_block Current target block after Datalens head buffer per chain and dataset.\n");
    body.push_str("# TYPE ormp_indexer_target_block gauge\n");
    body.push_str(
        "# HELP ormp_indexer_remaining_blocks Current remaining blocks per chain and dataset.\n",
    );
    body.push_str("# TYPE ormp_indexer_remaining_blocks gauge\n");
    body.push_str("# HELP ormp_indexer_datalens_head_height Latest Datalens head height observed by the ORMP indexer.\n");
    body.push_str("# TYPE ormp_indexer_datalens_head_height gauge\n");
    body.push_str("# HELP ormp_indexer_datalens_head_observed_timestamp_seconds Unix timestamp of the last Datalens head observation.\n");
    body.push_str("# TYPE ormp_indexer_datalens_head_observed_timestamp_seconds gauge\n");
    body.push_str("# HELP ormp_indexer_datalens_head_advanced_timestamp_seconds Unix timestamp of the last Datalens head advancement.\n");
    body.push_str("# TYPE ormp_indexer_datalens_head_advanced_timestamp_seconds gauge\n");
    body.push_str("# HELP ormp_indexer_chain_last_success_timestamp_seconds Unix timestamp of the last successful ORMP chain pass.\n");
    body.push_str("# TYPE ormp_indexer_chain_last_success_timestamp_seconds gauge\n");
    body.push_str("# HELP ormp_indexer_checkpoint_last_forward_advance_timestamp_seconds Unix timestamp of the last forward checkpoint advancement.\n");
    body.push_str("# TYPE ormp_indexer_checkpoint_last_forward_advance_timestamp_seconds gauge\n");
    body.push_str(
        "# HELP ormp_indexer_consecutive_failures Consecutive ORMP chain pass failures.\n",
    );
    body.push_str("# TYPE ormp_indexer_consecutive_failures gauge\n");
    body.push_str("# HELP ormp_indexer_chain_passes_total ORMP chain passes by result.\n");
    body.push_str("# TYPE ormp_indexer_chain_passes_total counter\n");
    body.push_str("# HELP ormp_indexer_ranges_total ORMP Datalens ranges by result.\n");
    body.push_str("# TYPE ormp_indexer_ranges_total counter\n");
    body.push_str(
        "# HELP ormp_indexer_blocks_processed_total ORMP blocks processed successfully.\n",
    );
    body.push_str("# TYPE ormp_indexer_blocks_processed_total counter\n");
    body.push_str("# HELP ormp_indexer_records_total ORMP records by result stage.\n");
    body.push_str("# TYPE ormp_indexer_records_total counter\n");
    body.push_str(
        "# HELP ormp_indexer_batch_duration_seconds ORMP successful batch duration summary.\n",
    );
    body.push_str("# TYPE ormp_indexer_batch_duration_seconds summary\n");
    body.push_str("# HELP ormp_indexer_datalens_requests_total ORMP Datalens requests by operation and result.\n");
    body.push_str("# TYPE ormp_indexer_datalens_requests_total counter\n");
    body.push_str("# HELP ormp_indexer_reorg_rollbacks_total ORMP reorg rollbacks.\n");
    body.push_str("# TYPE ormp_indexer_reorg_rollbacks_total counter\n");

    for row in &runtime.runtime_rows {
        let labels = runtime_labels(row.chain_id, &row.dataset);
        append_optional_metric(
            &mut body,
            "ormp_indexer_target_block",
            &labels,
            row.target_block,
        );
        append_optional_metric(
            &mut body,
            "ormp_indexer_remaining_blocks",
            &labels,
            row.remaining_blocks,
        );
        append_optional_metric(
            &mut body,
            "ormp_indexer_datalens_head_height",
            &labels,
            row.datalens_head_height,
        );
        append_optional_metric(
            &mut body,
            "ormp_indexer_datalens_head_observed_timestamp_seconds",
            &labels,
            row.datalens_head_observed_timestamp_seconds,
        );
        append_optional_metric(
            &mut body,
            "ormp_indexer_datalens_head_advanced_timestamp_seconds",
            &labels,
            row.datalens_head_advanced_timestamp_seconds,
        );
        append_optional_metric(
            &mut body,
            "ormp_indexer_chain_last_success_timestamp_seconds",
            &labels,
            row.chain_last_success_timestamp_seconds,
        );
        append_optional_metric(
            &mut body,
            "ormp_indexer_checkpoint_last_forward_advance_timestamp_seconds",
            &labels,
            row.checkpoint_last_forward_advance_timestamp_seconds,
        );
        append_metric(
            &mut body,
            "ormp_indexer_consecutive_failures",
            &labels,
            row.consecutive_failures,
        );
        append_metric(
            &mut body,
            "ormp_indexer_chain_passes_total",
            &result_labels(row.chain_id, &row.dataset, "success"),
            row.chain_pass_success_total,
        );
        append_metric(
            &mut body,
            "ormp_indexer_chain_passes_total",
            &result_labels(row.chain_id, &row.dataset, "failure"),
            row.chain_pass_failure_total,
        );
        append_metric(
            &mut body,
            "ormp_indexer_ranges_total",
            &result_labels(row.chain_id, &row.dataset, "success"),
            row.ranges_success_total,
        );
        append_metric(
            &mut body,
            "ormp_indexer_ranges_total",
            &result_labels(row.chain_id, &row.dataset, "failure"),
            row.ranges_failure_total,
        );
        append_metric(
            &mut body,
            "ormp_indexer_blocks_processed_total",
            &labels,
            row.blocks_processed_total,
        );
        append_metric(
            &mut body,
            "ormp_indexer_records_total",
            &stage_labels(row.chain_id, &row.dataset, "read"),
            row.records_read_total,
        );
        append_metric(
            &mut body,
            "ormp_indexer_records_total",
            &stage_labels(row.chain_id, &row.dataset, "decoded"),
            row.records_decoded_total,
        );
        append_metric(
            &mut body,
            "ormp_indexer_records_total",
            &stage_labels(row.chain_id, &row.dataset, "written"),
            row.records_written_total,
        );
        append_metric(
            &mut body,
            "ormp_indexer_batch_duration_seconds_sum",
            &labels,
            row.batch_duration_seconds_sum,
        );
        append_metric(
            &mut body,
            "ormp_indexer_batch_duration_seconds_count",
            &labels,
            row.batch_duration_seconds_count,
        );
        append_datalens_request_metrics(&mut body, row);
        append_metric(
            &mut body,
            "ormp_indexer_reorg_rollbacks_total",
            &labels,
            row.reorg_rollbacks_total,
        );
    }

    body.push_str("# HELP ormp_indexer_legacy_table_rows Legacy GraphQL table row counts.\n");
    body.push_str("# TYPE ormp_indexer_legacy_table_rows gauge\n");
    for table in LEGACY_TABLES {
        let count = row_counts
            .iter()
            .find(|row| row.table_name == *table)
            .map(|row| row.row_count)
            .unwrap_or_default();
        body.push_str("ormp_indexer_legacy_table_rows{table=\"");
        body.push_str(table);
        body.push_str("\"} ");
        body.push_str(&count.to_string());
        body.push('\n');
    }

    body
}

fn unix_timestamp_seconds() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

fn append_datalens_request_metrics(body: &mut String, row: &crate::metrics::RuntimeMetricsRow) {
    for (operation, success, failure) in [
        (
            "logs",
            row.datalens_log_requests_success_total,
            row.datalens_log_requests_failure_total,
        ),
        (
            "blocks",
            row.datalens_block_requests_success_total,
            row.datalens_block_requests_failure_total,
        ),
        (
            "transactions",
            row.datalens_tx_requests_success_total,
            row.datalens_tx_requests_failure_total,
        ),
    ] {
        append_metric(
            body,
            "ormp_indexer_datalens_requests_total",
            &operation_result_labels(row.chain_id, &row.dataset, operation, "success"),
            success,
        );
        append_metric(
            body,
            "ormp_indexer_datalens_requests_total",
            &operation_result_labels(row.chain_id, &row.dataset, operation, "failure"),
            failure,
        );
    }
}

fn runtime_labels(chain_id: u64, dataset: &str) -> Vec<(&'static str, String)> {
    vec![
        ("chain_id", chain_id.to_string()),
        ("dataset", dataset.to_owned()),
    ]
}

fn result_labels(chain_id: u64, dataset: &str, result: &str) -> Vec<(&'static str, String)> {
    let mut labels = runtime_labels(chain_id, dataset);
    labels.push(("result", result.to_owned()));
    labels
}

fn stage_labels(chain_id: u64, dataset: &str, stage: &str) -> Vec<(&'static str, String)> {
    let mut labels = runtime_labels(chain_id, dataset);
    labels.push(("stage", stage.to_owned()));
    labels
}

fn operation_result_labels(
    chain_id: u64,
    dataset: &str,
    operation: &str,
    result: &str,
) -> Vec<(&'static str, String)> {
    let mut labels = runtime_labels(chain_id, dataset);
    labels.push(("operation", operation.to_owned()));
    labels.push(("result", result.to_owned()));
    labels
}

fn append_optional_metric<T: ToString>(
    body: &mut String,
    name: &str,
    labels: &[(&str, String)],
    value: Option<T>,
) {
    if let Some(value) = value {
        append_metric(body, name, labels, value);
    }
}

fn append_metric<T: ToString>(body: &mut String, name: &str, labels: &[(&str, String)], value: T) {
    body.push_str(name);
    body.push('{');
    for (index, (key, value)) in labels.iter().enumerate() {
        if index > 0 {
            body.push(',');
        }
        body.push_str(key);
        body.push_str("=\"");
        body.push_str(&escape_label_value(value));
        body.push('"');
    }
    body.push_str("} ");
    body.push_str(&value.to_string());
    body.push('\n');
}

fn escape_label_value(value: &str) -> String {
    value.replace('\\', r"\\").replace('"', "\\\"")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_metrics_emits_runtime_monitoring_metrics() {
        let checkpoints = vec![CheckpointRow {
            chain_id: "46".to_owned(),
            dataset: "datalens-native".to_owned(),
            next_block: "101".to_owned(),
            updated_at: "2026-07-30 00:00:00+00".to_owned(),
            updated_timestamp_seconds: Some("1785369600".to_owned()),
        }];
        let runtime = crate::metrics::MetricsSnapshot {
            configured_scopes: vec![crate::metrics::ConfiguredScopeMetricsRow {
                chain_id: 46,
                dataset: "datalens-native".to_owned(),
                start_block: 1,
                batch_size: 1000,
            }],
            runtime_rows: vec![crate::metrics::RuntimeMetricsRow {
                chain_id: 46,
                dataset: "datalens-native".to_owned(),
                target_block: Some(150),
                remaining_blocks: Some(50),
                datalens_head_height: Some(151),
                datalens_head_observed_timestamp_seconds: Some(1_785_389_000.0),
                datalens_head_advanced_timestamp_seconds: Some(1_785_389_000.0),
                chain_last_success_timestamp_seconds: Some(1_785_389_010.0),
                checkpoint_last_forward_advance_timestamp_seconds: Some(1_785_389_020.0),
                consecutive_failures: 0,
                chain_pass_success_total: 3,
                ranges_success_total: 2,
                blocks_processed_total: 100,
                records_read_total: 4,
                records_decoded_total: 4,
                records_written_total: 4,
                batch_duration_seconds_sum: 1.5,
                batch_duration_seconds_count: 2,
                datalens_log_requests_success_total: 2,
                ..Default::default()
            }],
        };

        let output = format_metrics(&checkpoints, &[], &runtime);

        assert!(output.contains(
            "ormp_indexer_configured_scope_info{chain_id=\"46\",dataset=\"datalens-native\",start_block=\"1\",batch_size=\"1000\"} 1"
        ));
        assert!(output.contains(
            "ormp_indexer_checkpoint_present{chain_id=\"46\",dataset=\"datalens-native\"} 1"
        ));
        assert!(output.contains(
            "ormp_indexer_checkpoint_updated_timestamp_seconds{chain_id=\"46\",dataset=\"datalens-native\"} 1785369600"
        ));
        assert!(output.contains(
            "ormp_indexer_target_block{chain_id=\"46\",dataset=\"datalens-native\"} 150"
        ));
        assert!(output.contains(
            "ormp_indexer_remaining_blocks{chain_id=\"46\",dataset=\"datalens-native\"} 50"
        ));
        assert!(output.contains(
            "ormp_indexer_chain_passes_total{chain_id=\"46\",dataset=\"datalens-native\",result=\"success\"} 3"
        ));
        assert!(output.contains(
            "ormp_indexer_records_total{chain_id=\"46\",dataset=\"datalens-native\",stage=\"written\"} 4"
        ));
        assert!(output.contains(
            "ormp_indexer_datalens_requests_total{chain_id=\"46\",dataset=\"datalens-native\",operation=\"logs\",result=\"success\"} 2"
        ));
    }
}

use serde::Serialize;
use sqlx::{FromRow, PgPool};

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

    Ok(format_metrics(
        &checkpoints,
        &row_counts,
        &crate::metrics::snapshot(),
    ))
}

fn format_metrics(
    checkpoints: &[CheckpointRow],
    row_counts: &[LegacyTableRowCount],
    runtime: &crate::metrics::MetricsSnapshot,
) -> String {
    let mut body = String::new();

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

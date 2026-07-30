use axum::{
    body::{Body, to_bytes},
    http::{HeaderMap, Method, Request, StatusCode, header},
};
use serde_json::Value;
use sqlx::{PgPool, postgres::PgPoolOptions};
use std::time::Duration;
use tower::ServiceExt;

use ormpindexer::{
    config::MetricsConfig,
    database::{EventWriter, PostgresEventWriter, apply_migrations},
    graphql::{build_router, build_router_with_metrics_config, build_schema},
    schema::{ADDRESS_ORACLE, ADDRESS_RELAYER, ChainLogMetadata, EventSource, LegacyOrmPEvent},
};

#[tokio::test]
async fn test_healthz_returns_ok_without_database_connectivity() {
    let pool = unreachable_pool();
    let app = build_router(build_schema(pool.clone()), pool);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .expect("healthz request"),
        )
        .await
        .expect("healthz response");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("healthz body"),
        "OK"
    );
}

#[tokio::test]
async fn test_readyz_reports_service_unavailable_when_database_is_unreachable() {
    let pool = unreachable_pool();
    let app = build_router(build_schema(pool.clone()), pool);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/readyz")
                .body(Body::empty())
                .expect("readyz request"),
        )
        .await
        .expect("readyz response");

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn test_graphql_preflight_allows_msgscan_origin() {
    let pool = unreachable_pool();
    let app = build_router(build_schema(pool.clone()), pool);

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::OPTIONS)
                .uri("/graphql")
                .header(header::ORIGIN, "https://msgport-scan.ringdao.com")
                .header(header::ACCESS_CONTROL_REQUEST_METHOD, "POST")
                .header(header::ACCESS_CONTROL_REQUEST_HEADERS, "content-type")
                .body(Body::empty())
                .expect("cors preflight request"),
        )
        .await
        .expect("cors preflight response");

    assert!(response.status().is_success());
    assert_eq!(
        response.headers().get(header::ACCESS_CONTROL_ALLOW_ORIGIN),
        Some(&"*".parse().expect("wildcard header"))
    );
    assert!(
        response
            .headers()
            .get(header::ACCESS_CONTROL_ALLOW_METHODS)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.contains("POST"))
    );
}

#[tokio::test]
async fn test_graphql_response_allows_msgscan_origin() {
    let pool = unreachable_pool();
    let app = build_router(build_schema(pool.clone()), pool);

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/graphql")
                .header(header::ORIGIN, "https://msgport-scan.ringdao.com")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"query":"{ __typename }"}"#))
                .expect("graphql cors request"),
        )
        .await
        .expect("graphql cors response");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(header::ACCESS_CONTROL_ALLOW_ORIGIN),
        Some(&"*".parse().expect("wildcard header"))
    );
}

#[tokio::test]
async fn test_status_and_metrics_expose_checkpoint_progress_from_postgres() {
    let Some(database_url) = test_database_url() else {
        eprintln!("skipping ops endpoint Postgres test; ORMPINDEXER_TEST_DATABASE_URL is not set");
        return;
    };
    let pool = PgPool::connect(&database_url)
        .await
        .expect("connect test postgres");
    apply_migrations(&pool).await.expect("apply migrations");
    truncate_tables(&pool).await;
    seed_checkpoint_rows(&pool).await;
    PostgresEventWriter::new(pool.clone())
        .write_events(&legacy_events())
        .await
        .expect("write legacy events");

    let app = build_router(build_schema(pool.clone()), pool);

    let status = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/status")
                .body(Body::empty())
                .expect("status request"),
        )
        .await
        .expect("status response");

    assert_eq!(status.status(), StatusCode::OK);
    let status_json: Value = serde_json::from_slice(
        &to_bytes(status.into_body(), usize::MAX)
            .await
            .expect("status body"),
    )
    .expect("status json");
    assert_eq!(status_json["checkpoints"].as_array().map(Vec::len), Some(2));
    assert_eq!(
        status_json["progress"]["chains"].as_array().map(Vec::len),
        Some(2)
    );
    assert_eq!(
        status_json["progress"]["datasets"][0]["dataset"],
        Value::String("datalens-native".to_owned())
    );
    assert_eq!(
        status_json["progress"]["datasets"][0]["chains"],
        Value::Number(2.into())
    );
    assert_eq!(
        status_json["progress"]["datasets"][0]["minNextBlock"],
        Value::String("123".to_owned())
    );
    assert_eq!(
        status_json["progress"]["datasets"][0]["maxNextBlock"],
        Value::String("456".to_owned())
    );

    let metrics = app
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .expect("metrics request"),
        )
        .await
        .expect("metrics response");

    assert_eq!(metrics.status(), StatusCode::OK);
    let metrics_headers = metrics.headers().clone();
    let metrics_body = String::from_utf8(
        to_bytes(metrics.into_body(), usize::MAX)
            .await
            .expect("metrics body")
            .to_vec(),
    )
    .expect("utf8 metrics");
    assert_eq!(
        content_type(&metrics_headers),
        Some("text/plain; version=0.0.4; charset=utf-8")
    );
    assert!(metrics_body.contains(
        "ormp_indexer_checkpoint_next_block{chain_id=\"46\",dataset=\"datalens-native\"} 123"
    ));
    assert!(metrics_body.contains("ormp_metrics_refresh_success{} 1"));
    assert!(metrics_body.contains("ormp_metrics_snapshot_stale{} 0"));
    assert!(metrics_body.contains(
        "ormp_indexer_checkpoint_next_block{chain_id=\"11155111\",dataset=\"datalens-native\"} 456"
    ));
    assert!(
        metrics_body.contains("ormp_indexer_legacy_table_rows{table=\"ormp_message_accepted\"} 1")
    );
    assert!(
        metrics_body.contains("ormp_indexer_legacy_table_rows{table=\"ormp_message_assigned\"} 1")
    );
}

#[tokio::test]
async fn test_metrics_endpoint_serves_cached_db_snapshot_between_refreshes() {
    let Some(database_url) = test_database_url() else {
        eprintln!("skipping metrics cache Postgres test; ORMPINDEXER_TEST_DATABASE_URL is not set");
        return;
    };
    let pool = PgPool::connect(&database_url)
        .await
        .expect("connect test postgres");
    apply_migrations(&pool).await.expect("apply migrations");
    truncate_tables(&pool).await;
    seed_checkpoint_rows(&pool).await;

    let app = build_router_with_metrics_config(
        build_schema(pool.clone()),
        pool.clone(),
        MetricsConfig {
            refresh_delay: Duration::from_secs(3600),
            refresh_timeout: Duration::from_secs(10),
        },
    );

    let first_body = metrics_body(app.clone()).await;
    assert!(first_body.contains(
        "ormp_indexer_checkpoint_next_block{chain_id=\"46\",dataset=\"datalens-native\"} 123"
    ));

    sqlx::query(
        "UPDATE ormp_indexer_checkpoint
         SET next_block = $1::NUMERIC
         WHERE chain_id = $2::NUMERIC AND dataset = $3",
    )
    .bind("789")
    .bind("46")
    .bind("datalens-native")
    .execute(&pool)
    .await
    .expect("update checkpoint");

    let second_body = metrics_body(app).await;
    assert!(second_body.contains(
        "ormp_indexer_checkpoint_next_block{chain_id=\"46\",dataset=\"datalens-native\"} 123"
    ));
    assert!(!second_body.contains(
        "ormp_indexer_checkpoint_next_block{chain_id=\"46\",dataset=\"datalens-native\"} 789"
    ));
}

fn legacy_events() -> Vec<LegacyOrmPEvent> {
    vec![
        LegacyOrmPEvent::MessageAccepted {
            metadata: evm_metadata("accepted-log"),
            msg_hash: "0xaccepted".to_owned(),
            channel: "0xchannel".to_owned(),
            index: 8,
            from_chain_id: 1,
            from: "0xfrom".to_owned(),
            to_chain_id: 46,
            to: "0xto".to_owned(),
            gas_limit: 500_000,
            encoded: "0xencoded".to_owned(),
        },
        LegacyOrmPEvent::MessageAssigned {
            metadata: evm_metadata("assigned-log"),
            msg_hash: "0xaccepted".to_owned(),
            oracle: ADDRESS_ORACLE[0].to_owned(),
            relayer: ADDRESS_RELAYER[0].to_owned(),
            oracle_fee: 11,
            relayer_fee: 22,
            params: "0xparams".to_owned(),
        },
    ]
}

fn evm_metadata(id: &str) -> ChainLogMetadata {
    ChainLogMetadata {
        id: id.to_owned(),
        source: EventSource::Evm,
        chain_id: 46,
        block_number: 123,
        block_hash: None,
        block_timestamp: 456,
        transaction_hash: "0xtx".to_owned(),
        transaction_index: 2,
        log_index: 3,
        contract_address: "0xport".to_owned(),
        transaction_from: Some("0xsender".to_owned()),
    }
}

async fn seed_checkpoint_rows(pool: &PgPool) {
    sqlx::query(
        "INSERT INTO ormp_indexer_checkpoint (chain_id, dataset, next_block)
         VALUES ($1::NUMERIC, $2, $3::NUMERIC), ($4::NUMERIC, $5, $6::NUMERIC)",
    )
    .bind("46")
    .bind("datalens-native")
    .bind("123")
    .bind("11155111")
    .bind("datalens-native")
    .bind("456")
    .execute(pool)
    .await
    .expect("seed checkpoints");
}

async fn truncate_tables(pool: &PgPool) {
    sqlx::query(
        "TRUNCATE
            ormp_indexer_checkpoint,
            ormp_hash_imported,
            ormp_message_accepted,
            ormp_message_assigned,
            ormp_message_dispatched,
            msgport_message_recv,
            msgport_message_sent,
            signature_pub_signature_submittion",
    )
    .execute(pool)
    .await
    .expect("truncate tables");
}

async fn metrics_body(app: axum::Router) -> String {
    let response = app
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .expect("metrics request"),
        )
        .await
        .expect("metrics response");

    assert_eq!(response.status(), StatusCode::OK);
    String::from_utf8(
        to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("metrics body")
            .to_vec(),
    )
    .expect("utf8 metrics")
}

fn content_type(headers: &HeaderMap) -> Option<&str> {
    headers
        .get("content-type")
        .and_then(|value| value.to_str().ok())
}

fn unreachable_pool() -> PgPool {
    PgPoolOptions::new()
        .acquire_timeout(Duration::from_millis(250))
        .connect_lazy("postgres://user:pass@127.0.0.1:1/ormpindexer?connect_timeout=1")
        .expect("lazy postgres pool")
}

fn test_database_url() -> Option<String> {
    std::env::var("ORMPINDEXER_TEST_DATABASE_URL").ok()
}

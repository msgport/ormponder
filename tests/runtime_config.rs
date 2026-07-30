use std::{collections::BTreeMap, time::Duration};

use ormpindexer::{
    checkpoint::{Checkpoint, plan_next_range},
    config::{FinalityMode, RuntimeConfig},
};

#[test]
fn test_runtime_config_from_env_map_reads_datalens_database_and_chain_settings() {
    let env = BTreeMap::from([
        (
            "ORMPINDEXER_DATALENS_ENDPOINT".to_owned(),
            "https://datalens.example".to_owned(),
        ),
        (
            "ORMPINDEXER_DATALENS_APPLICATION".to_owned(),
            "ormp-production".to_owned(),
        ),
        (
            "ORMPINDEXER_DATALENS_TOKEN".to_owned(),
            "secret-token".to_owned(),
        ),
        (
            "ORMPINDEXER_DATALENS_TIMEOUT_SECS".to_owned(),
            "120".to_owned(),
        ),
        (
            "ORMPINDEXER_DATALENS_QUERY_MAX_ATTEMPTS".to_owned(),
            "5".to_owned(),
        ),
        (
            "ORMPINDEXER_DATALENS_HEAD_BUFFER_BLOCKS".to_owned(),
            "2".to_owned(),
        ),
        (
            "ORMPINDEXER_DATALENS_MIN_REQUEST_INTERVAL_MS".to_owned(),
            "250".to_owned(),
        ),
        (
            "ORMPINDEXER_DATABASE_URL".to_owned(),
            "postgres://user:pass@localhost/ormp".to_owned(),
        ),
        ("ORMPINDEXER_ENABLED_CHAINS".to_owned(), "1,46".to_owned()),
        ("ORMPINDEXER_BATCH_SIZE".to_owned(), "250".to_owned()),
        ("ORMPINDEXER_START_BLOCK".to_owned(), "1000".to_owned()),
        ("ORMPINDEXER_FINALITY_MODE".to_owned(), "durable".to_owned()),
        (
            "ORMPINDEXER_DATALENS_WARMUP_ENABLED".to_owned(),
            "true".to_owned(),
        ),
        (
            "ORMPINDEXER_DATALENS_WARMUP_ENSURE_ON_STARTUP".to_owned(),
            "false".to_owned(),
        ),
        (
            "ORMPINDEXER_DATALENS_WARMUP_REQUIRED".to_owned(),
            "true".to_owned(),
        ),
        (
            "ORMPINDEXER_DATALENS_WARMUP_CHUNK_SIZE".to_owned(),
            "500".to_owned(),
        ),
        (
            "ORMPINDEXER_DATALENS_WARMUP_END_BLOCK".to_owned(),
            "9000".to_owned(),
        ),
        (
            "ORMPINDEXER_CHAIN_1_CONTRACTS".to_owned(),
            "0x111,0x222".to_owned(),
        ),
        (
            "ORMPINDEXER_CHAIN_1_TOPICS".to_owned(),
            "0xaaa,0xbbb".to_owned(),
        ),
        (
            "ORMPINDEXER_CHAIN_46_START_BLOCK".to_owned(),
            "2000".to_owned(),
        ),
        (
            "ORMPINDEXER_CHAIN_46_BATCH_SIZE".to_owned(),
            "50".to_owned(),
        ),
        (
            "ORMPINDEXER_CHAIN_46_CONTRACTS".to_owned(),
            "0x333".to_owned(),
        ),
    ]);

    let config = RuntimeConfig::from_env_map(&env).expect("config parses");

    assert_eq!(config.datalens.endpoint, "https://datalens.example");
    assert_eq!(config.datalens.application, "ormp-production");
    assert!(config.datalens.token.is_some());
    assert_eq!(config.datalens.timeout, Duration::from_secs(120));
    assert_eq!(config.datalens.query_max_attempts, 5);
    assert_eq!(config.datalens.head_buffer_blocks, 2);
    assert_eq!(
        config.datalens.min_request_interval,
        Duration::from_millis(250)
    );
    assert!(config.database_url.is_some());
    assert_eq!(config.enabled_chains.len(), 2);
    assert_eq!(config.batch_size, 250);
    assert_eq!(config.start_block, 1000);
    assert_eq!(config.finality_mode, FinalityMode::Durable);
    assert_eq!(config.reorg_window_blocks, 128);
    assert_eq!(config.metrics.refresh_delay, Duration::from_secs(5));
    assert_eq!(config.metrics.refresh_timeout, Duration::from_secs(10));
    assert!(config.warmup.enabled);
    assert!(!config.warmup.ensure_on_startup);
    assert!(config.warmup.required);
    assert_eq!(config.warmup.chunk_size, 500);
    assert_eq!(config.warmup.end_block, Some(9000));
    assert_eq!(
        config.chain(1).expect("chain 1").contracts,
        vec!["0x111", "0x222"]
    );
    assert_eq!(
        config.chain(1).expect("chain 1").topics,
        vec!["0xaaa", "0xbbb"]
    );
    assert_eq!(config.chain(46).expect("chain 46").start_block, 2000);
    assert_eq!(config.chain(1).expect("chain 1").batch_size, 250);
    assert_eq!(config.chain(46).expect("chain 46").batch_size, 50);
    assert_eq!(
        config.chain(1).expect("chain 1").finality_mode,
        FinalityMode::Durable
    );
    assert_eq!(
        config.chain(46).expect("chain 46").finality_mode,
        FinalityMode::Durable
    );
}

#[test]
fn test_runtime_config_reads_metrics_cache_settings() {
    let env = BTreeMap::from([
        (
            "ORMPINDEXER_DATALENS_ENDPOINT".to_owned(),
            "https://datalens.example".to_owned(),
        ),
        (
            "ORMPINDEXER_DATALENS_APPLICATION".to_owned(),
            "ormp-production".to_owned(),
        ),
        ("ORMPINDEXER_ENABLED_CHAINS".to_owned(), "46".to_owned()),
        (
            "ORMPINDEXER_METRICS_REFRESH_DELAY_MS".to_owned(),
            "3000".to_owned(),
        ),
        (
            "ORMPINDEXER_METRICS_REFRESH_TIMEOUT_MS".to_owned(),
            "7000".to_owned(),
        ),
    ]);

    let config = RuntimeConfig::from_env_map(&env).expect("config parses");

    assert_eq!(config.metrics.refresh_delay, Duration::from_secs(3));
    assert_eq!(config.metrics.refresh_timeout, Duration::from_secs(7));
}

#[test]
fn test_runtime_config_rejects_zero_metrics_refresh_delay() {
    let env = BTreeMap::from([
        (
            "ORMPINDEXER_DATALENS_ENDPOINT".to_owned(),
            "https://datalens.example".to_owned(),
        ),
        (
            "ORMPINDEXER_DATALENS_APPLICATION".to_owned(),
            "ormp-production".to_owned(),
        ),
        ("ORMPINDEXER_ENABLED_CHAINS".to_owned(), "46".to_owned()),
        (
            "ORMPINDEXER_METRICS_REFRESH_DELAY_MS".to_owned(),
            "0".to_owned(),
        ),
    ]);

    let error = RuntimeConfig::from_env_map(&env).expect_err("zero refresh delay is invalid");

    assert!(
        error
            .to_string()
            .contains("ORMPINDEXER_METRICS_REFRESH_DELAY_MS must be greater than zero")
    );
}

#[test]
fn test_runtime_config_rejects_zero_metrics_refresh_timeout() {
    let env = BTreeMap::from([
        (
            "ORMPINDEXER_DATALENS_ENDPOINT".to_owned(),
            "https://datalens.example".to_owned(),
        ),
        (
            "ORMPINDEXER_DATALENS_APPLICATION".to_owned(),
            "ormp-production".to_owned(),
        ),
        ("ORMPINDEXER_ENABLED_CHAINS".to_owned(), "46".to_owned()),
        (
            "ORMPINDEXER_METRICS_REFRESH_TIMEOUT_MS".to_owned(),
            "0".to_owned(),
        ),
    ]);

    let error = RuntimeConfig::from_env_map(&env).expect_err("zero refresh timeout is invalid");

    assert!(
        error
            .to_string()
            .contains("ORMPINDEXER_METRICS_REFRESH_TIMEOUT_MS must be greater than zero")
    );
}

#[test]
fn test_runtime_config_accepts_safe_and_latest_finality_modes() {
    for (value, expected) in [
        ("safe", FinalityMode::Safe),
        ("latest", FinalityMode::Latest),
    ] {
        let env = BTreeMap::from([
            (
                "ORMPINDEXER_DATALENS_ENDPOINT".to_owned(),
                "https://datalens.example".to_owned(),
            ),
            (
                "ORMPINDEXER_DATALENS_APPLICATION".to_owned(),
                "ormp-production".to_owned(),
            ),
            ("ORMPINDEXER_ENABLED_CHAINS".to_owned(), "46".to_owned()),
            ("ORMPINDEXER_FINALITY_MODE".to_owned(), value.to_owned()),
        ]);

        let config = RuntimeConfig::from_env_map(&env).expect("config parses");

        assert_eq!(config.finality_mode, expected);
        assert_eq!(config.chain(46).expect("chain 46").finality_mode, expected);
    }
}

#[test]
fn test_runtime_config_chain_finality_override_takes_precedence() {
    let env = BTreeMap::from([
        (
            "ORMPINDEXER_DATALENS_ENDPOINT".to_owned(),
            "https://datalens.example".to_owned(),
        ),
        (
            "ORMPINDEXER_DATALENS_APPLICATION".to_owned(),
            "ormp-production".to_owned(),
        ),
        ("ORMPINDEXER_ENABLED_CHAINS".to_owned(), "1,46".to_owned()),
        (
            "ORMPINDEXER_FINALITY_MODE".to_owned(),
            "finalized".to_owned(),
        ),
        (
            "ORMPINDEXER_CHAIN_46_FINALITY_MODE".to_owned(),
            "latest".to_owned(),
        ),
        (
            "ORMPINDEXER_REORG_WINDOW_BLOCKS".to_owned(),
            "256".to_owned(),
        ),
    ]);

    let config = RuntimeConfig::from_env_map(&env).expect("config parses");

    assert_eq!(config.finality_mode, FinalityMode::Finalized);
    assert_eq!(
        config.chain(1).expect("chain 1").finality_mode,
        FinalityMode::Finalized
    );
    assert_eq!(
        config.chain(46).expect("chain 46").finality_mode,
        FinalityMode::Latest
    );
    assert_eq!(config.reorg_window_blocks, 256);
}

#[test]
fn test_runtime_config_rejects_zero_reorg_window_for_reorg_finality() {
    let env = BTreeMap::from([
        (
            "ORMPINDEXER_DATALENS_ENDPOINT".to_owned(),
            "https://datalens.example".to_owned(),
        ),
        (
            "ORMPINDEXER_DATALENS_APPLICATION".to_owned(),
            "ormp-production".to_owned(),
        ),
        ("ORMPINDEXER_ENABLED_CHAINS".to_owned(), "46".to_owned()),
        ("ORMPINDEXER_FINALITY_MODE".to_owned(), "safe".to_owned()),
        ("ORMPINDEXER_REORG_WINDOW_BLOCKS".to_owned(), "0".to_owned()),
    ]);

    let error = RuntimeConfig::from_env_map(&env).expect_err("zero reorg window is invalid");

    assert!(
        error
            .to_string()
            .contains("ORMPINDEXER_REORG_WINDOW_BLOCKS must be greater than zero")
    );
}

#[test]
fn test_runtime_config_from_env_map_reads_warmup_defaults() {
    let env = BTreeMap::from([
        (
            "ORMPINDEXER_DATALENS_ENDPOINT".to_owned(),
            "https://datalens.example".to_owned(),
        ),
        (
            "ORMPINDEXER_DATALENS_APPLICATION".to_owned(),
            "ormp-production".to_owned(),
        ),
        ("ORMPINDEXER_ENABLED_CHAINS".to_owned(), "46".to_owned()),
        ("ORMPINDEXER_BATCH_SIZE".to_owned(), "250".to_owned()),
    ]);

    let config = RuntimeConfig::from_env_map(&env).expect("config parses");

    assert!(config.warmup.enabled);
    assert!(config.warmup.ensure_on_startup);
    assert!(!config.warmup.required);
    assert_eq!(config.warmup.chunk_size, 250);
    assert_eq!(config.warmup.end_block, None);
}

#[test]
fn test_runtime_config_reads_datalens_retry_defaults() {
    let env = BTreeMap::from([
        (
            "ORMPINDEXER_DATALENS_ENDPOINT".to_owned(),
            "https://datalens.example".to_owned(),
        ),
        (
            "ORMPINDEXER_DATALENS_APPLICATION".to_owned(),
            "ormp-production".to_owned(),
        ),
        ("ORMPINDEXER_ENABLED_CHAINS".to_owned(), "46".to_owned()),
    ]);

    let config = RuntimeConfig::from_env_map(&env).expect("config parses");

    assert_eq!(config.datalens.timeout, Duration::from_secs(300));
    assert_eq!(config.datalens.query_max_attempts, 3);
    assert_eq!(config.datalens.head_buffer_blocks, 1);
    assert_eq!(
        config.datalens.min_request_interval,
        Duration::from_millis(100)
    );
    assert_eq!(config.batch_size, 1_000);
    assert_eq!(config.chain(46).expect("chain 46").batch_size, 1_000);
}

#[test]
fn test_runtime_config_rejects_zero_chain_batch_size() {
    let env = BTreeMap::from([
        (
            "ORMPINDEXER_DATALENS_ENDPOINT".to_owned(),
            "https://datalens.example".to_owned(),
        ),
        (
            "ORMPINDEXER_DATALENS_APPLICATION".to_owned(),
            "ormp-production".to_owned(),
        ),
        ("ORMPINDEXER_ENABLED_CHAINS".to_owned(), "46".to_owned()),
        ("ORMPINDEXER_CHAIN_46_BATCH_SIZE".to_owned(), "0".to_owned()),
    ]);

    let error = RuntimeConfig::from_env_map(&env).expect_err("zero chain batch size is invalid");

    assert!(
        error
            .to_string()
            .contains("ORMPINDEXER_CHAIN_46_BATCH_SIZE must be greater than zero")
    );
}

#[test]
fn test_runtime_config_rejects_zero_datalens_timeout() {
    let env = BTreeMap::from([
        (
            "ORMPINDEXER_DATALENS_ENDPOINT".to_owned(),
            "https://datalens.example".to_owned(),
        ),
        (
            "ORMPINDEXER_DATALENS_APPLICATION".to_owned(),
            "ormp-production".to_owned(),
        ),
        ("ORMPINDEXER_ENABLED_CHAINS".to_owned(), "46".to_owned()),
        (
            "ORMPINDEXER_DATALENS_TIMEOUT_SECS".to_owned(),
            "0".to_owned(),
        ),
    ]);

    let error = RuntimeConfig::from_env_map(&env).expect_err("zero timeout is invalid");

    assert!(
        error
            .to_string()
            .contains("ORMPINDEXER_DATALENS_TIMEOUT_SECS must be greater than zero")
    );
}

#[test]
fn test_runtime_config_rejects_zero_datalens_query_max_attempts() {
    let env = BTreeMap::from([
        (
            "ORMPINDEXER_DATALENS_ENDPOINT".to_owned(),
            "https://datalens.example".to_owned(),
        ),
        (
            "ORMPINDEXER_DATALENS_APPLICATION".to_owned(),
            "ormp-production".to_owned(),
        ),
        ("ORMPINDEXER_ENABLED_CHAINS".to_owned(), "46".to_owned()),
        (
            "ORMPINDEXER_DATALENS_QUERY_MAX_ATTEMPTS".to_owned(),
            "0".to_owned(),
        ),
    ]);

    let error = RuntimeConfig::from_env_map(&env).expect_err("zero attempts is invalid");

    assert!(
        error
            .to_string()
            .contains("ORMPINDEXER_DATALENS_QUERY_MAX_ATTEMPTS must be greater than zero")
    );
}

#[test]
fn test_runtime_config_rejects_zero_warmup_chunk_size() {
    let env = BTreeMap::from([
        (
            "ORMPINDEXER_DATALENS_ENDPOINT".to_owned(),
            "https://datalens.example".to_owned(),
        ),
        (
            "ORMPINDEXER_DATALENS_APPLICATION".to_owned(),
            "ormp-production".to_owned(),
        ),
        ("ORMPINDEXER_ENABLED_CHAINS".to_owned(), "46".to_owned()),
        (
            "ORMPINDEXER_DATALENS_WARMUP_CHUNK_SIZE".to_owned(),
            "0".to_owned(),
        ),
    ]);

    let error = RuntimeConfig::from_env_map(&env).expect_err("zero warmup chunk size is invalid");

    assert!(
        error
            .to_string()
            .contains("ORMPINDEXER_DATALENS_WARMUP_CHUNK_SIZE must be greater than zero")
    );
}

#[test]
fn test_database_url_from_env_map_does_not_require_runtime_chain_settings() {
    let env = BTreeMap::from([(
        "ORMPINDEXER_DATABASE_URL".to_owned(),
        "postgres://user:pass@localhost/ormp".to_owned(),
    )]);

    let database_url =
        RuntimeConfig::database_url_from_env_map(&env).expect("database URL parses independently");

    assert_eq!(
        database_url.expose_secret(),
        "postgres://user:pass@localhost/ormp"
    );
}

#[test]
fn test_plan_next_range_uses_checkpoint_and_batch_size() {
    let checkpoint = Checkpoint {
        chain_id: 46,
        dataset: "datalens-native".to_owned(),
        next_block: 120,
    };

    let range = plan_next_range(&checkpoint, 25).expect("range is planned");

    assert_eq!(range.from_block, 120);
    assert_eq!(range.to_block, 144);
}

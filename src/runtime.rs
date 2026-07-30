use anyhow::Context;

use crate::{
    config::RuntimeConfig,
    database::{PostgresCheckpointStore, PostgresEventWriter, apply_migrations, connect},
    datalens::DatalensHttpClient,
    decoder::EvmEventDecoder,
    graphql::{build_router, build_schema},
    runner::IndexerRunner,
    warmup::ensure_startup_warmup,
};

pub async fn run(run_once: bool) -> anyhow::Result<()> {
    let config = RuntimeConfig::from_env().context("load ORMP indexer runtime config")?;
    let database_url = config
        .database_url
        .as_ref()
        .context("ORMPINDEXER_DATABASE_URL must be configured for run")?;
    let pool = connect(database_url, 5).await?;
    apply_migrations(&pool).await?;

    log::info!(
        "starting ORMP Datalens indexer endpoint={} application={} token_configured={} database_configured=true chains={} batch_size={} finality={} dry_run=false",
        config.datalens.endpoint,
        config.datalens.application,
        config.datalens.token.is_some(),
        config.enabled_chains.len(),
        config.batch_size,
        config.finality_mode.as_str(),
    );

    crate::metrics::record_configured_scopes(&config);

    let client = DatalensHttpClient::new(config.datalens.clone());
    let checkpoints = PostgresCheckpointStore::new(pool.clone());
    ensure_datalens_warmup_on_startup(&config, &checkpoints, &client).await?;

    let runner = IndexerRunner::new(
        config.clone(),
        client,
        checkpoints,
        EvmEventDecoder,
        PostgresEventWriter::new(pool),
    );

    if run_once {
        runner.run_once().await?;
    } else {
        runner.run_loop().await?;
    }

    Ok(())
}

pub async fn run_with_server(listen_addr: &str) -> anyhow::Result<()> {
    let config = RuntimeConfig::from_env().context("load ORMP indexer runtime config")?;
    let database_url = config
        .database_url
        .as_ref()
        .context("ORMPINDEXER_DATABASE_URL must be configured for run")?;
    let pool = connect(database_url, 5).await?;
    apply_migrations(&pool).await?;

    log::info!(
        "starting ORMP Datalens indexer with GraphQL server endpoint={} application={} token_configured={} database_configured=true chains={} batch_size={} finality={} listen_addr={} dry_run=false",
        config.datalens.endpoint,
        config.datalens.application,
        config.datalens.token.is_some(),
        config.enabled_chains.len(),
        config.batch_size,
        config.finality_mode.as_str(),
        listen_addr,
    );

    crate::metrics::record_configured_scopes(&config);

    let client = DatalensHttpClient::new(config.datalens.clone());
    let checkpoints = PostgresCheckpointStore::new(pool.clone());
    ensure_datalens_warmup_on_startup(&config, &checkpoints, &client).await?;

    let runner = IndexerRunner::new(
        config.clone(),
        client,
        checkpoints,
        EvmEventDecoder,
        PostgresEventWriter::new(pool.clone()),
    );
    let app = build_router(build_schema(pool.clone()), pool);
    let listener = tokio::net::TcpListener::bind(listen_addr)
        .await
        .with_context(|| format!("bind GraphQL server to {listen_addr}"))?;

    tokio::try_join!(runner.run_loop(), async {
        axum::serve(listener, app)
            .await
            .context("serve GraphQL server")
    },)?;

    Ok(())
}

async fn ensure_datalens_warmup_on_startup(
    config: &RuntimeConfig,
    checkpoints: &PostgresCheckpointStore,
    client: &DatalensHttpClient,
) -> anyhow::Result<()> {
    ensure_startup_warmup(config, checkpoints, client).await?;
    Ok(())
}

pub async fn migrate() -> anyhow::Result<()> {
    let database_url =
        RuntimeConfig::database_url_from_env().context("load ORMP indexer database config")?;
    let pool = connect(&database_url, 1).await?;
    apply_migrations(&pool).await?;

    log::info!("ORMP indexer migrations applied");

    Ok(())
}

use async_graphql::http::{GraphiQLPlugin, GraphiQLSource};
use async_graphql::{EmptyMutation, EmptySubscription, Schema};
use async_graphql_axum::{GraphQLRequest, GraphQLResponse};
use axum::{
    Json, Router,
    extract::State,
    http::{HeaderValue, Method, StatusCode, header::CONTENT_TYPE},
    response::{Html, IntoResponse},
    routing::{get, post},
};
use sqlx::PgPool;
use tower_http::cors::{Any, CorsLayer};

use super::QueryRoot;
use crate::{config::MetricsConfig, ops};

pub type IndexerGraphqlSchema = Schema<QueryRoot, EmptyMutation, EmptySubscription>;

pub fn build_schema(pool: PgPool) -> IndexerGraphqlSchema {
    Schema::build(QueryRoot, EmptyMutation, EmptySubscription)
        .data(GraphqlState { pool })
        .finish()
}

pub fn build_router(schema: IndexerGraphqlSchema, pool: PgPool) -> Router {
    build_router_with_metrics_config(schema, pool, MetricsConfig::default())
}

pub fn build_router_with_metrics_config(
    schema: IndexerGraphqlSchema,
    pool: PgPool,
    metrics_config: MetricsConfig,
) -> Router {
    let metrics_cache = ops::MetricsCache::new(metrics_config);
    metrics_cache.spawn_refresh_loop(pool.clone());

    Router::new()
        .route("/healthz", get(healthz_handler))
        .route("/readyz", get(readyz_handler))
        .route("/status", get(status_handler))
        .route("/metrics", get(metrics_handler))
        .route("/graphql", post(graphql_handler))
        .route("/graphiql", get(graphql_graphiql))
        .with_state(HttpState {
            schema,
            pool,
            metrics_cache,
        })
        .layer(cors_layer())
}

fn cors_layer() -> CorsLayer {
    CorsLayer::new()
        .allow_origin(Any)
        .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
        .allow_headers(Any)
}

async fn graphql_handler(
    State(state): State<HttpState>,
    request: GraphQLRequest,
) -> GraphQLResponse {
    state.schema.execute(request.into_inner()).await.into()
}

async fn healthz_handler() -> impl IntoResponse {
    (StatusCode::OK, "OK")
}

async fn readyz_handler(State(state): State<HttpState>) -> impl IntoResponse {
    match ops::check_readiness(&state.pool).await {
        Ok(()) => (StatusCode::OK, "READY").into_response(),
        Err(error) => (
            StatusCode::SERVICE_UNAVAILABLE,
            format!("database not ready: {error}"),
        )
            .into_response(),
    }
}

async fn status_handler(State(state): State<HttpState>) -> impl IntoResponse {
    match ops::load_status(&state.pool).await {
        Ok(status) => Json(status).into_response(),
        Err(error) => (
            StatusCode::SERVICE_UNAVAILABLE,
            format!("status unavailable: {error}"),
        )
            .into_response(),
    }
}

async fn metrics_handler(State(state): State<HttpState>) -> impl IntoResponse {
    match state.metrics_cache.render(&state.pool).await {
        Ok(metrics) => (
            [(
                CONTENT_TYPE,
                HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8"),
            )],
            metrics,
        )
            .into_response(),
        Err(error) => (
            StatusCode::SERVICE_UNAVAILABLE,
            format!("metrics unavailable: {error}"),
        )
            .into_response(),
    }
}

async fn graphql_graphiql() -> impl IntoResponse {
    Html(
        GraphiQLSource::build()
            .endpoint("/graphql")
            .version("3.9.0")
            .title("ORMP Indexer GraphiQL")
            .plugins(&[graphiql_explorer_plugin()])
            .finish(),
    )
}

fn graphiql_explorer_plugin<'a>() -> GraphiQLPlugin<'a> {
    GraphiQLPlugin {
        name: "GraphiQLPluginExplorer",
        constructor: "GraphiQLPluginExplorer.explorerPlugin",
        head_assets: Some(
            r#"<link rel="stylesheet" href="https://unpkg.com/@graphiql/plugin-explorer@3.0.0/dist/style.css" />"#,
        ),
        body_assets: Some(
            r#"<script
      src="https://unpkg.com/@graphiql/plugin-explorer@3.0.0/dist/index.umd.js"
      crossorigin
    ></script>"#,
        ),
        ..Default::default()
    }
}

pub(super) struct GraphqlState {
    pub(super) pool: PgPool,
}

#[derive(Clone)]
struct HttpState {
    schema: IndexerGraphqlSchema,
    pool: PgPool,
    metrics_cache: ops::MetricsCache,
}

use std::sync::Arc;

use arc_swap::ArcSwap;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing::Level;
use tracing_subscriber::fmt::time::UtcTime;

use rzrouter::{
    api::run_api_server,
    config::{Config, RouterMode},
    error::RZError,
    forwarder::Forwarder,
    hop_manager::HopManager,
    metrics::Metrics,
    resolver::RzPointResolver,
    routing_state::{EdgeState, RoutingSnapshot, ZoneState},
    rzid::RzidClient,
    tcp_server::TcpServer,
};

fn main() -> Result<(), RZError> {
    let subscriber = tracing_subscriber::fmt()
        .compact()
        .with_timer(UtcTime::rfc_3339())
        .with_max_level(Level::INFO)
        .with_target(false)
        .with_thread_names(false)
        .with_ansi(false)
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .expect("Failed to set global tracing subscriber");

    let config = Config::parse()?;

    let desired_workers = config.worker_threads;
    tracing::info!(
        workers = desired_workers,
        mode = ?config.mode,
        "starting tokio runtime"
    );

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(desired_workers)
        .max_blocking_threads(512)
        .enable_all()
        .build()
        .map_err(|e| RZError::System(format!("Failed to build runtime: {e}")))?;

    rt.block_on(async_main(config))
}

async fn async_main(config: Config) -> Result<(), RZError> {
    let config = Arc::new(config);
    let cancel = CancellationToken::new();

    // --- Ctrl+C handler ----------------------------------------------------
    {
        let cancel = cancel.clone();
        tokio::spawn(async move {
            let _ = tokio::signal::ctrl_c().await;
            tracing::info!("Ctrl+C received, shutting down");
            cancel.cancel();
        });
    }

    // --- RzPoint Resolver --------------------------------------------------
    let resolver = Arc::new(RzPointResolver::new(&config.rzpoint_addr, config.mode)?);

    // --- Hop Manager -------------------------------------------------------
    let hop_manager = Arc::new(HopManager::new(config.clone(), resolver.clone()));

    // --- Initial Routing State --------------------------------------------
    let initial_snapshot = match config.mode {
        RouterMode::Edge => RoutingSnapshot::Edge(EdgeState::new()),
        RouterMode::Zone => RoutingSnapshot::Zone(ZoneState::new()),
    };
    let routing_state = Arc::new(ArcSwap::new(Arc::new(initial_snapshot)));

    // --- RzID Client ------------------------------------------------------
    let rzid = Arc::new(RzidClient::new(&config)?);

    // --- RzID Refresh Task ------------------------------------------------
    let refresh_rzid = rzid.clone();
    let refresh_routing = routing_state.clone();
    let refresh_hops = hop_manager.clone();
    let refresh_cancel = cancel.clone();
    let refresh_interval = config.refresh_interval_secs;

    tokio::spawn(async move {
        run_refresh_task(
            refresh_rzid,
            refresh_routing,
            refresh_hops,
            refresh_interval,
            refresh_cancel,
        )
        .await;
    });

    // --- Heartbeat Task (zone mode only) ----------------------------------
    let heartbeat_rzid = rzid.clone();
    let heartbeat_cancel = cancel.clone();
    let heartbeat_interval = config.heartbeat_interval_secs;

    tokio::spawn(async move {
        run_heartbeat_task(heartbeat_rzid, heartbeat_interval, heartbeat_cancel).await;
    });

    // --- Forwarder --------------------------------------------------------
    let codecs = rzid.fetch_codecs().await?;
    let forwarder = Arc::new(Forwarder::new(
        routing_state.clone(),
        hop_manager.clone(),
        codecs.rate_features,
    ));

    // --- TCP Server -------------------------------------------------------
    let listen_addr = config.listen_addr();
    let listener = TcpListener::bind(&listen_addr)
        .await
        .map_err(|e| RZError::System(format!("bind {listen_addr}: {e}")))?;

    tracing::info!(%listen_addr, mode = ?config.mode, "tcp server bound");

    // Create metrics (usually near the top of main, after parsing config)
    let metrics = Arc::new(Metrics::new());

    // ... later when creating the server
    let server = TcpServer::new(config.clone(), cancel.clone(), forwarder, metrics.clone());

    let api_cancel = cancel.clone();
    let api_metrics = metrics.clone();
    let api_addr = config.api_listen_addr();

    tokio::spawn(async move {
        if let Err(e) = run_api_server(api_addr, api_metrics, api_cancel).await {
            tracing::error!("API server failed: {}", e);
        }
    });

    // --- Run --------------------------------------------------------------
    tokio::select! {
        _ = cancel.cancelled() => {
            tracing::info!("shutdown complete");
            Ok(())
        }
        res = server.run(listener) => {
            res.map_err(|e| RZError::System(format!("tcp server error: {e}")))
        }
    }
}

// ---------------------------------------------------------------------------
// Background Tasks
// ---------------------------------------------------------------------------

async fn run_refresh_task(
    client: Arc<RzidClient>,
    routing: Arc<ArcSwap<RoutingSnapshot>>,
    hop_manager: Arc<HopManager>,
    interval_secs: u64,
    cancel: CancellationToken,
) {
    let mut ticker = tokio::time::interval(std::time::Duration::from_secs(interval_secs));

    tracing::info!(interval = interval_secs, "rzid refresh task started");

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                tracing::info!("rzid refresh task stopping");
                break;
            }
            _ = ticker.tick() => {
                let current = routing.load();
                match client.sync_routing_state(Some(&current), &hop_manager).await {
                    Ok(Some(new_state)) => {
                        tracing::info!("routing state updated");
                        routing.store(Arc::new(new_state));
                    }
                    Ok(None) => {
                        // No changes
                    }
                    Err(e) => {
                        tracing::error!("routing sync failed: {}", e);
                    }
                }
            }
        }
    }
}

async fn run_heartbeat_task(
    client: Arc<RzidClient>,
    interval_secs: u64,
    cancel: CancellationToken,
) {
    let mut ticker = tokio::time::interval(std::time::Duration::from_secs(interval_secs));

    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = ticker.tick() => {
                if let Err(e) = client.send_heartbeat().await {
                    tracing::error!("heartbeat failed: {}", e);
                }
            }
        }
    }
}

// // SPDX-License-Identifier: BUSL-1.1
// // Copyright (c) 2026 M. Javani
// //
// // This file is part of rzrouter.
// //
// // Use of this software is governed by the Business Source License 1.1
// // included in the LICENSE file in the root of this repository.

use tokio_util::sync::CancellationToken;

use rzrouter::{async_main, config::Config, error::RZError};

fn main() -> Result<(), RZError> {
    let config = Config::parse()?;

    let desired_workers = config.worker_threads;
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(desired_workers)
        .max_blocking_threads(512)
        .enable_all()
        .build()
        .map_err(|e| RZError::System(format!("Failed to build runtime: {e}")))?;

    rt.block_on(async {
        let shutdown = CancellationToken::new();

        // Ctrl+C handler INSIDE the runtime
        {
            let shutdown = shutdown.clone();
            tokio::spawn(async move {
                let _ = tokio::signal::ctrl_c().await;
                tracing::info!("Ctrl+C received, shutting down");
                shutdown.cancel();
            });
        }

        async_main(config, shutdown).await
    })
}

// Copyright 2024-2026 Zunor
// SPDX-License-Identifier: Apache-2.0

//! `parod` server entry point.

use paro_common::config::ConfigLoader;
use paro_common::logging::targets;
use paro_common::logging::LogManager;
use paro_function::register_log_storage;
use paro_server::{CommandLineArgs, Server};
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Builder;

const PAROD_WORKER_STACK_SIZE: usize = 8 * 1024 * 1024;
const PAROD_SHUTDOWN_GRACE_PERIOD: Duration = Duration::from_secs(5);

fn main() -> anyhow::Result<()> {
    Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(PAROD_WORKER_STACK_SIZE)
        .build()?
        .block_on(async_main())
}

async fn async_main() -> anyhow::Result<()> {
    let args = CommandLineArgs::parse_args();

    if args.print_config {
        print!("{}", ConfigLoader::sample_config());
        return Ok(());
    }

    let mut config = ConfigLoader::load_with_options(args.config.clone())?;

    args.apply_to(&mut config);

    let log_manager = LogManager::init(config.logging.clone())
        .map_err(|e| anyhow::anyhow!("Failed to initialize logging: {}", e))?;

    register_log_storage(log_manager.memory_storage());

    tracing::info!(target: targets::INSTANCE, "Paro server startup requested");
    tracing::info!(
        target: targets::SERVER,
        host = %config.server.host,
        port = config.server.port,
        data_dir = %config.storage.data_dir.display(),
        max_memory = config.cluster.max_memory,
        "Configuration loaded"
    );

    let server = Arc::new(Server::from_config(&config).await?);
    let server_task = tokio::spawn(Arc::clone(&server).run());
    tokio::pin!(server_task);

    tokio::select! {
        run_result = &mut server_task => {
            run_result
                .map_err(|err| anyhow::anyhow!("server task failed: {}", err))??;
        }
        signal_result = shutdown_signal() => {
            signal_result?;
            tracing::info!(
                target: targets::SERVER,
                grace_period_ms = PAROD_SHUTDOWN_GRACE_PERIOD.as_millis() as u64,
                "Shutdown signal received"
            );

            let shutdown_report = server.shutdown(PAROD_SHUTDOWN_GRACE_PERIOD).await?;
            tracing::info!(
                target: targets::SERVER,
                drained_connections = shutdown_report.connections_drained,
                forced_connections = shutdown_report.connections_forced,
                instance_disposition = ?shutdown_report.instance_shutdown_report.disposition,
                clean_shutdown_persisted = shutdown_report.instance_shutdown_report.clean_shutdown_persisted,
                "Graceful shutdown coordinator completed"
            );

            server_task
                .await
                .map_err(|err| anyhow::anyhow!("server task failed: {}", err))??;
        }
    }

    Ok(())
}

#[cfg(unix)]
async fn shutdown_signal() -> anyhow::Result<()> {
    use tokio::signal::unix::{signal, SignalKind};

    let mut terminate = signal(SignalKind::terminate())
        .map_err(|err| anyhow::anyhow!("failed to install SIGTERM handler: {}", err))?;
    tokio::select! {
        _ = tokio::signal::ctrl_c() => Ok(()),
        _ = terminate.recv() => Ok(()),
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() -> anyhow::Result<()> {
    tokio::signal::ctrl_c()
        .await
        .map_err(|err| anyhow::anyhow!("failed to install Ctrl-C handler: {}", err))
}

use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};
use tokio::time::{Duration, Instant, MissedTickBehavior};

use crate::{OxanusError, config::Config, job_envelope::JobEnvelope, metrics::JobMetricsBuffer};

#[derive(Default, Debug)]
pub struct Stats {
    pub processed: u64,
    pub succeeded: u64,
    pub panicked: u64,
    pub failed: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct JobResult {
    pub kind: JobResultKind,
    pub envelope: JobEnvelope,
    pub duration_ms: u64,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum JobResultKind {
    Success,
    Panicked,
    Failed,
}

pub async fn run<DT, ET>(
    mut rx: mpsc::Receiver<JobResult>,
    config: Arc<Config<DT, ET>>,
    stats: Arc<Mutex<Stats>>,
) -> Result<(), OxanusError>
where
    DT: Send + Sync + Clone + 'static,
    ET: std::error::Error + Send + Sync + 'static,
{
    let mut metrics = JobMetricsBuffer::default();
    let mut flush_interval = tokio::time::interval_at(
        Instant::now() + Duration::from_secs(5),
        Duration::from_secs(5),
    );
    flush_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            result = rx.recv() => {
                match result {
                    Some(result) => {
                        metrics.record(&result);
                        config.storage.internal.track_redis_result(
                            update_stats(Arc::clone(&config), Arc::clone(&stats), &result).await
                        )?;
                        if config.cancel_token.is_cancelled() {
                            flush_metrics(Arc::clone(&config), &mut metrics).await?;
                            return Ok(());
                        }
                    }
                    None => {
                        flush_metrics(Arc::clone(&config), &mut metrics).await?;
                        return Ok(());
                    }
                }
            }
            _ = flush_interval.tick() => {
                flush_metrics(Arc::clone(&config), &mut metrics).await?;
            }
            _ = config.cancel_token.cancelled() => {
                flush_metrics(Arc::clone(&config), &mut metrics).await?;
                return Ok(());
            }
        }
    }
}

async fn update_stats<DT, ET>(
    config: Arc<Config<DT, ET>>,
    stats: Arc<Mutex<Stats>>,
    result: &JobResult,
) -> Result<(), OxanusError>
where
    DT: Send + Sync + Clone + 'static,
    ET: std::error::Error + Send + Sync + 'static,
{
    let processed = {
        let mut stats = stats.lock().await;
        stats.processed += 1;
        match result.kind {
            JobResultKind::Success => stats.succeeded += 1,
            JobResultKind::Panicked => {
                stats.panicked += 1;
                stats.failed += 1;
            }
            JobResultKind::Failed => stats.failed += 1,
        }

        stats.processed
    };

    config.storage.internal.update_stats(result).await?;

    if let Some(exit_when_processed) = config.exit_when_processed
        && processed >= exit_when_processed
    {
        config.cancel_token.cancel();
    }

    Ok(())
}

async fn flush_metrics<DT, ET>(
    config: Arc<Config<DT, ET>>,
    metrics: &mut JobMetricsBuffer,
) -> Result<(), OxanusError>
where
    DT: Send + Sync + Clone + 'static,
    ET: std::error::Error + Send + Sync + 'static,
{
    if metrics.is_empty() {
        return Ok(());
    }

    if config
        .storage
        .internal
        .track_redis_result(config.storage.internal.flush_job_metrics(metrics).await)?
        .is_some()
    {
        metrics.clear();
    }

    Ok(())
}

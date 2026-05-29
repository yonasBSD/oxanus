use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::sleep;

use crate::JobId;
use crate::error::OxanaError;
use crate::queue::{QueueConfig, QueueThrottle};
use crate::runtime::Runtime;
use crate::semaphores_map::QueueControl;
use crate::storage_internal::StorageInternal;
use crate::throttler::Throttler;
use crate::worker_event::WorkerJob;

pub async fn run<DT>(
    config: Arc<Runtime<DT>>,
    queue_config: QueueConfig,
    queue_key: String,
    job_tx: mpsc::Sender<WorkerJob>,
    queue_control: Arc<QueueControl>,
) -> Result<(), OxanaError>
where
    DT: Send + Sync + Clone + 'static,
{
    loop {
        let permit = tokio::select! {
            permit = queue_control.acquire() => permit,
            _ = config.cancel_token.cancelled() => {
                tracing::debug!("Stopping dispatcher for queue {}", queue_key);
                break;
            }
        };

        tokio::select! {
            result = pop_queue_message(&config.storage.internal, &queue_config, &queue_key, config.settings.dispatcher_idle_sleep, config.settings.throttled_queue_fallback_wait) => {
                match config.storage.internal.track_redis_result(result, config.settings.redis_failure_tolerance)? {
                    Some(Some(job_id)) => {
                        let job = WorkerJob { job_id, permit };
                        job_tx
                            .send(job)
                            .await
                            .expect("Failed to send job to worker");
                    }
                    Some(None) => {
                        drop(permit);
                    }
                    None => {
                        drop(permit);
                        sleep(config.settings.dispatcher_idle_sleep).await;
                    }
                }
            }
            _ = config.cancel_token.cancelled() => {
                tracing::debug!("Stopping dispatcher for queue {}", queue_key);
                drop(permit);
                break;
            }
        }
    }

    Ok(())
}

async fn pop_queue_message(
    storage: &StorageInternal,
    queue_config: &QueueConfig,
    queue_key: &str,
    idle_sleep: std::time::Duration,
    throttled_queue_fallback_wait: std::time::Duration,
) -> Result<Option<JobId>, OxanaError> {
    match &queue_config.throttle {
        Some(throttle) => {
            pop_queue_message_w_throttle(
                storage,
                queue_key,
                throttle,
                throttled_queue_fallback_wait,
            )
            .await
        }
        None => pop_queue_message_wo_throttle(storage, queue_key, idle_sleep).await,
    }
}

async fn pop_queue_message_wo_throttle(
    storage: &StorageInternal,
    queue_key: &str,
    timeout: Duration,
) -> Result<Option<JobId>, OxanaError> {
    let job_id = storage.dequeue(queue_key).await?;
    if job_id.is_none() {
        sleep(timeout).await;
    }
    Ok(job_id)
}

async fn pop_queue_message_w_throttle(
    storage: &StorageInternal,
    queue_key: &str,
    throttle: &QueueThrottle,
    fallback_wait: Duration,
) -> Result<Option<JobId>, OxanaError> {
    let pool = storage.pool().await?;
    let throttler = Throttler::new(pool, queue_key, throttle.limit, throttle.window_ms);

    let state = throttler.state().await?;

    if state.is_allowed
        && let Some(job_id) = storage.dequeue(queue_key).await?
    {
        let cost = storage
            .get_job(&job_id)
            .await?
            .and_then(|envelope| envelope.meta.throttle_cost);
        throttler.consume(cost).await?;
        return Ok(Some(job_id));
    }

    let wait = state
        .throttled_for
        .and_then(|millis| u64::try_from(millis).ok())
        .map_or(fallback_wait, Duration::from_millis);
    sleep(wait).await;
    Ok(None)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use testresult::TestResult;
    use tokio::sync::mpsc;

    use super::run;
    use crate::config::{Config, RuntimeSettings};
    use crate::runtime::Runtime;
    use crate::semaphores_map::QueueControlsMap;
    use crate::test_helper::random_string;
    use crate::worker_event::WorkerJob;
    use crate::{QueueConfig, QueueRuntimeConfig, Storage, StorageBuilderTimeouts};

    #[tokio::test]
    async fn idle_dispatcher_does_not_hold_pool_connection() -> TestResult {
        dotenvy::from_filename(".env.test").ok();
        let redis_url = std::env::var("REDIS_URL")?;
        let queue = random_string();
        let storage = Storage::builder()
            .namespace(random_string())
            .max_pool_size(1)
            .timeouts(StorageBuilderTimeouts::new(Duration::from_millis(50)))
            .build_from_redis_url(redis_url)?;
        let runtime = Arc::new(Runtime::new(
            storage.clone(),
            Config::<()>::new(),
            RuntimeSettings::new(),
        ));
        let (job_tx, _job_rx) = mpsc::channel::<WorkerJob>(1);
        let queue_controls = QueueControlsMap::new();
        let queue_control = queue_controls
            .get_or_create(queue.clone(), QueueRuntimeConfig::new(1))
            .await;

        let handle = tokio::spawn(run(
            Arc::clone(&runtime),
            QueueConfig::as_static(&queue),
            queue.clone(),
            job_tx,
            queue_control,
        ));

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(storage.internal.enqueued_count(&queue).await?, 0);

        runtime.cancel_token.cancel();
        tokio::time::timeout(Duration::from_secs(2), handle).await???;

        Ok(())
    }
}

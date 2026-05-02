use crate::shared::*;
use deadpool_redis::redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use testresult::TestResult;

#[derive(Serialize)]
struct MetricsQueueOne;

impl oxanus::Queue for MetricsQueueOne {
    fn to_config() -> oxanus::QueueConfig {
        oxanus::QueueConfig::as_static("metrics_one").concurrency(2)
    }
}

#[derive(Serialize)]
struct MetricsQueueTwo;

impl oxanus::Queue for MetricsQueueTwo {
    fn to_config() -> oxanus::QueueConfig {
        oxanus::QueueConfig::as_static("metrics_two").concurrency(2)
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct MetricSuccessJob {
    sleep_ms: u64,
}

impl oxanus::Job for MetricSuccessJob {
    fn worker_name() -> &'static str {
        std::any::type_name::<MetricSuccessWorker>()
    }
}

struct MetricSuccessWorker;

impl oxanus::FromContext<()> for MetricSuccessWorker {
    fn from_context(_ctx: &()) -> Self {
        Self
    }
}

#[async_trait::async_trait]
impl oxanus::Worker<MetricSuccessJob> for MetricSuccessWorker {
    type Error = WorkerError;

    async fn run_batch(
        &self,
        jobs: Vec<oxanus::BatchItem<MetricSuccessJob>>,
    ) -> Result<(), WorkerError> {
        for item in jobs {
            tokio::time::sleep(std::time::Duration::from_millis(item.job.sleep_ms)).await;
        }
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct MetricFailJob;

impl oxanus::Job for MetricFailJob {
    fn worker_name() -> &'static str {
        std::any::type_name::<MetricFailWorker>()
    }
}

struct MetricFailWorker;

impl oxanus::FromContext<()> for MetricFailWorker {
    fn from_context(_ctx: &()) -> Self {
        Self
    }
}

#[async_trait::async_trait]
impl oxanus::Worker<MetricFailJob> for MetricFailWorker {
    type Error = WorkerError;

    async fn run_batch(
        &self,
        _jobs: Vec<oxanus::BatchItem<MetricFailJob>>,
    ) -> Result<(), WorkerError> {
        Err(WorkerError::Generic("expected failure".to_string()))
    }

    fn max_retries(&self, _job: &MetricFailJob) -> u32 {
        0
    }
}

#[tokio::test]
async fn test_job_metrics_record_execution_time_counts_and_ttls() -> TestResult {
    let redis_pool = setup();
    let ctx = oxanus::ContextValue::new(());
    let storage = oxanus::Storage::builder()
        .namespace(random_string())
        .build_from_pool(redis_pool.clone())?;
    let config = oxanus::Config::new(&storage)
        .register_queue::<MetricsQueueOne>()
        .register_queue::<MetricsQueueTwo>()
        .register_worker::<MetricSuccessWorker, MetricSuccessJob>()
        .register_worker::<MetricFailWorker, MetricFailJob>()
        .exit_when_processed(3);

    storage
        .enqueue(MetricsQueueOne, MetricSuccessJob { sleep_ms: 25 })
        .await?;
    storage
        .enqueue(MetricsQueueTwo, MetricSuccessJob { sleep_ms: 15 })
        .await?;
    storage.enqueue(MetricsQueueOne, MetricFailJob).await?;

    let run_stats = oxanus::run(config, ctx).await?;
    assert_eq!(run_stats.processed, 3);

    let snapshot = storage
        .job_metrics(oxanus::JobMetricsQuery::default())
        .await?;
    assert_eq!(snapshot.totals.processed, 3);
    assert_eq!(snapshot.totals.failed, 1);
    assert_eq!(snapshot.totals.succeeded, 2);
    assert!(snapshot.totals.execution_ms >= 30);
    assert_eq!(snapshot.jobs.len(), 3);

    let success_one = oxanus::MetricIdentity {
        worker: std::any::type_name::<MetricSuccessWorker>().to_string(),
        queue: "metrics_one".to_string(),
    };
    let success_two = oxanus::MetricIdentity {
        worker: std::any::type_name::<MetricSuccessWorker>().to_string(),
        queue: "metrics_two".to_string(),
    };
    let failure = oxanus::MetricIdentity {
        worker: std::any::type_name::<MetricFailWorker>().to_string(),
        queue: "metrics_one".to_string(),
    };

    let success_one_metrics = storage
        .job_metrics_for(&success_one, oxanus::JobMetricsQuery::default())
        .await?;
    assert_eq!(success_one_metrics.totals.processed, 1);
    assert_eq!(success_one_metrics.totals.failed, 0);
    assert_eq!(success_one_metrics.totals.succeeded, 1);
    assert!(success_one_metrics.totals.execution_ms >= 20);
    assert_eq!(
        success_one_metrics
            .histogram
            .iter()
            .map(|bucket| bucket.count)
            .sum::<u64>(),
        1
    );

    let success_two_metrics = storage
        .job_metrics_for(&success_two, oxanus::JobMetricsQuery::default())
        .await?;
    assert_eq!(success_two_metrics.totals.processed, 1);
    assert_eq!(success_two_metrics.totals.failed, 0);
    assert_eq!(success_two_metrics.totals.succeeded, 1);
    assert!(success_two_metrics.totals.execution_ms >= 10);

    let failure_metrics = storage
        .job_metrics_for(&failure, oxanus::JobMetricsQuery::default())
        .await?;
    assert_eq!(failure_metrics.totals.processed, 1);
    assert_eq!(failure_metrics.totals.failed, 1);
    assert_eq!(failure_metrics.totals.succeeded, 0);
    assert_eq!(failure_metrics.totals.execution_ms, 0);
    assert_eq!(
        failure_metrics
            .histogram
            .iter()
            .map(|bucket| bucket.count)
            .sum::<u64>(),
        0
    );

    let mut redis = redis_pool.get().await?;
    let keys: Vec<String> = redis
        .keys(format!("{}:metrics:*", storage.namespace()))
        .await?;
    assert!(!keys.is_empty());

    for key in keys {
        let ttl: i64 = redis.ttl(&key).await?;
        assert!(ttl > 0);
        assert!(ttl <= 8 * 60 * 60);
    }

    Ok(())
}

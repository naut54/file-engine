use crate::profiler::{Entry, Workload};

use super::batch::{pack, Batch};
use super::config::BatchConfig;

#[derive(Debug, Clone)]
pub struct StreamJob {
    pub entry: Entry,
}

#[derive(Debug, Clone, Default)]
pub struct ExecutionPlan {
    pub batches: Vec<Batch>,
    pub streams: Vec<StreamJob>,
}

pub(crate) fn plan(workload: Workload, config: &BatchConfig) -> ExecutionPlan {
    let batches = pack(workload.small, config);
    let streams = workload
        .large
        .into_iter()
        .map(|entry| StreamJob { entry })
        .collect();
    ExecutionPlan { batches, streams }
}

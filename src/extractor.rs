// TODO(task-6): implement BatchExtractor — connector-x streaming, adaptive batch sizing
#[allow(dead_code)]
pub struct BatchExtractor {
    database_url: String,
    target_memory_mb: u64,
    default_batch_size: u64,
    current_batch_size: u64,
}

impl BatchExtractor {
    pub fn new(database_url: &str, target_memory_mb: u64, default_batch_size: u64) -> Self {
        Self {
            database_url: database_url.to_string(),
            target_memory_mb,
            default_batch_size,
            current_batch_size: default_batch_size,
        }
    }
}

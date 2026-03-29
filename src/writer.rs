// TODO(task-7.2): implement DeltaWriter struct with S3 storage configuration from Config
// TODO(task-7.3): implement ensure_table() — check/create Delta table at S3 path
// TODO(task-7.4): implement append_batch() — SaveMode::Append with HWM in commitInfo
// TODO(task-7.5): implement overwrite_table() — SaveMode::Overwrite for FullRefresh
// TODO(task-7.6): implement read_hwm() — read latest commitInfo from Delta log
// TODO(task-7.7): implement extract_hwm_from_batch() — scan batch for max updated_at + id
pub struct DeltaWriter;

impl DeltaWriter {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        _bucket: &str,
        _prefix: &str,
        _endpoint: Option<&str>,
        _region: &str,
        _access_key: &str,
        _secret_key: &str,
    ) -> Self {
        todo!()
    }
}

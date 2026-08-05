use anyhow::Result;
use std::collections::HashMap;

mod delta;
mod source;

use crate::writer::Hwm;
pub use delta::DeltaProbeAdapter;
pub use source::SourceProbeAdapter;

/// Table verdict outcome from verification.
#[derive(Debug, Clone, PartialEq)]
pub enum TableOutcome {
    Pass,
    Drift { reason: String },
    Discrepancy { reason: String },
    Skipped { reason: String },
    /// V3-r Tier 1: per-column value aggregates were verified, but the table's row-set
    /// completeness could NOT be key-verified (no single-column integer PRIMARY key). Not a
    /// failure, but not a full Clean either — surfaced distinctly so it isn't silent false confidence.
    PartiallyVerified { reason: String },
}

/// Overall verification verdict.
#[derive(Debug, Clone, PartialEq)]
pub enum VerifyVerdict {
    Clean,
    Discrepancy,
    /// V3-r Tier 1: no table had a discrepancy, but one or more could not be fully verified
    /// (key-set/row-set completeness unchecked). Distinct from Clean so exit-code gating can tell.
    PartiallyVerified,
}

#[derive(Debug, Clone)]
pub enum VerifyMode {
    Basic,
    FullRefresh,
    Incremental {
        cursor_col: String,
        hwm: Option<Hwm>,
    },
    TwoStream {
        insert_cursor: String,
        update_cursor: String,
        update_hwm: Option<Hwm>,
        insert_hwm: Option<i64>,
    },
}

#[derive(Debug, Clone)]
pub struct TablePlan {
    pub table: String,
    pub mode: VerifyMode,
}

#[derive(Debug, Clone)]
pub struct SourceScope {
    pub cursor_col: String,
    pub updated_at: String,
    pub last_id: i64,
    /// The resolved key column used for the tie-break predicate and the latest-per-key
    /// window (V3): the table's discovered single-column integer PRIMARY key, or the `id`
    /// fallback — never a literal `"id"` baked into the SQL.
    pub key_col: String,
}

impl TablePlan {
    pub fn basic(table: impl Into<String>) -> Self {
        Self {
            table: table.into(),
            mode: VerifyMode::Basic,
        }
    }

    fn describe(&self) -> String {
        format!("mode={}", self.mode.describe())
    }
}

impl VerifyMode {
    fn describe(&self) -> String {
        match self {
            VerifyMode::Basic => "basic".to_string(),
            VerifyMode::FullRefresh => "full_refresh".to_string(),
            VerifyMode::Incremental { cursor_col, hwm } => {
                format!(
                    "incremental cursor={} hwm={}",
                    cursor_col,
                    format_hwm(hwm.as_ref())
                )
            }
            VerifyMode::TwoStream {
                insert_cursor,
                update_cursor,
                update_hwm,
                insert_hwm,
            } => format!(
                "two_stream insert_cursor={} update_cursor={} update_hwm={} insert_hwm={}",
                insert_cursor,
                update_cursor,
                format_hwm(update_hwm.as_ref()),
                format_i64_hwm(*insert_hwm)
            ),
        }
    }

    fn freshness_cursor(&self) -> Option<&str> {
        match self {
            VerifyMode::Incremental { cursor_col, .. } => Some(cursor_col.as_str()),
            VerifyMode::TwoStream { update_cursor, .. } => Some(update_cursor.as_str()),
            VerifyMode::Basic | VerifyMode::FullRefresh => None,
        }
    }
}

fn format_hwm(hwm: Option<&Hwm>) -> String {
    match hwm {
        Some(hwm) => format!("updated_at={} last_id={}", hwm.updated_at, hwm.last_id),
        None => "none".to_string(),
    }
}

fn format_i64_hwm(hwm: Option<i64>) -> String {
    hwm.map(|value| value.to_string())
        .unwrap_or_else(|| "none".to_string())
}

const DEFAULT_ROW_CAP: i64 = 1_000_000;
const SAMPLE_SIZE: i64 = 100;

/// Column types whose CAST-to-string is identical across MySQL and Delta, so they can be
/// value-compared safely. Excludes tinyint (bool ambiguity), decimal/float (precision),
/// date/datetime/timestamp/time (format/tz), json/blob/binary/enum/set.
fn is_value_comparable(type_str: &str) -> bool {
    matches!(
        type_str.to_ascii_lowercase().as_str(),
        "smallint"
            | "mediumint"
            | "int"
            | "integer"
            | "bigint"
            | "varchar"
            | "char"
            | "text"
            | "tinytext"
            | "mediumtext"
            | "longtext"
    )
}

/// V5 (diagnostic): coarse type family of a MariaDB DATA_TYPE. Category-level only — this backs a
/// print-only type-drift diagnostic (NOT the verdict), so it groups by kind rather than exact type.
///
/// IMPORTANT calibration note: `decimal`/`numeric`, `date`, `datetime`/`timestamp`, and
/// `json`/`enum`/`set` all map to `"string"` here, NOT to a `"decimal"`/`"date"`/`"timestamp"`/
/// `"json"` label, because that's what they actually become on the Delta side. Verified against
/// `orchestrator::schema::mariadb_type_to_arrow`, which maps every one of these MariaDB types to
/// `DataType::Utf8` (mirroring the vendored connector_arrow's MySQL reader, which emits Utf8 for
/// DECIMAL/DATE/DATETIME/TIMESTAMP on the wire — MySQL server-timezone ambiguity and precision
/// concerns rule out native Decimal128/Date32/Timestamp on this pipeline). So a healthy synced
/// decimal/date/datetime/json column has Delta family `"string"` (see `delta_type_family`'s
/// `Utf8`/`Utf8View` arm), and the source family must match that or every such column would
/// false-flag as a type diff. `time`/`year`/`bit` are NOT in the extractable-type allowlist
/// (`discovery::EXTRACTABLE_DATA_TYPES`) — a column of one of those types never reaches the Delta
/// side at all, so it can never appear in the "columns present on both sides" comparison below;
/// their family labels here are unreachable in practice and kept only for completeness.
fn source_type_family(mariadb_type: &str) -> String {
    let lower = mariadb_type.to_ascii_lowercase();
    match lower.as_str() {
        "tinyint" | "smallint" | "mediumint" | "int" | "integer" | "bigint" => "int".to_string(),
        "decimal" | "numeric" => "string".to_string(),
        "float" | "double" | "real" => "float".to_string(),
        "date" | "datetime" | "timestamp" => "string".to_string(),
        "time" => "time".to_string(),
        "year" => "year".to_string(),
        "char" | "varchar" | "text" | "tinytext" | "mediumtext" | "longtext" | "enum" | "set"
        | "json" => "string".to_string(),
        "binary" | "varbinary" | "blob" | "tinyblob" | "mediumblob" | "longblob" => "binary".to_string(),
        "bit" => "bit".to_string(),
        _ => lower, // unknown -> its own label; a genuine mismatch will still show
    }
}

/// V5 (diagnostic): coarse type family of an Arrow DataType debug string (delta side). Kept in
/// lockstep with `source_type_family` above — see that function's doc comment for why
/// decimal/date/datetime/json land in `"string"` rather than a type-specific family. The
/// `Decimal`/`Date`/`Timestamp` arms below are defensive (unreachable for data synced through
/// this pipeline today, since those MariaDB types are written as Utf8) rather than live paths.
fn delta_type_family(arrow_debug: &str) -> &'static str {
    let d = arrow_debug;
    if d.starts_with("Int") || d.starts_with("UInt") {
        "int"
    } else if d.starts_with("Decimal") {
        "decimal"
    } else if d.starts_with("Float") {
        "float"
    } else if d.starts_with("Date") {
        "date"
    } else if d.starts_with("Timestamp") {
        "timestamp"
    } else if d.starts_with("Time") {
        "time"
    } else if d == "Utf8" || d == "LargeUtf8" || d == "Utf8View" {
        "string"
    } else if d.starts_with("Binary") || d.starts_with("LargeBinary") || d.starts_with("FixedSizeBinary") {
        "binary"
    } else if d == "Boolean" {
        "int" // MySQL bool/tinyint(1) round-trips through the int family
    } else {
        "other"
    }
}

/// Column metadata for a single table column (used by L0 schema reconciliation).
#[derive(Debug, Clone)]
pub struct ColumnMeta {
    pub name: String,
    pub type_str: String,
    pub nullable: bool,
    /// NUMERIC_SCALE from information_schema (source only; None on the Delta side and for
    /// non-decimal columns). Drives the native-scale DECIMAL(65,scale) used in value
    /// aggregates so a source column declared at e.g. scale 12 isn't silently truncated to
    /// the historical fixed scale of 10 (VA2).
    pub numeric_scale: Option<u32>,
}

/// What kind of value-aggregate fingerprint to compute for a column.
#[derive(Debug, Clone, PartialEq)]
pub enum AggKind {
    Integer,            // exact SUM/MIN/MAX
    Decimal { scale: u32 }, // SUM/MIN/MAX at the column's native scale (VA2)
    DatetimeSec,        // MIN/MAX truncated to whole seconds
    DateOnly,           // MIN/MAX date only
    TextMass,           // SUM(CHAR_LENGTH) + non-null COUNT (collation-independent)
}

#[derive(Debug, Clone)]
pub struct ColumnAgg {
    pub name: String,
    pub kind: AggKind,
}

/// The raw aggregate values a probe reads for one column, before fingerprint assembly.
/// `sum` is None for DatetimeSec/DateOnly (no sum computed); for TextMass it carries
/// SUM(CHAR_LENGTH) as a string. `non_null_count` closes the value->NULL-swap blind spot
/// (VA5): every fingerprint now carries it, so a value overwritten with NULL (same sum/min/max
/// otherwise) still shows up as a count mismatch.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnAggValues {
    pub sum: Option<String>,
    pub min: Option<String>,
    pub max: Option<String>,
    pub non_null_count: i64,
}

/// Classify a MariaDB DATA_TYPE string into an AggKind, or None to skip
/// (tinyint/bool, float/double, time, json/blob/binary/enum/set, and anything unknown).
/// The `Decimal` scale here is a placeholder (0) — callers must override it from the
/// column's `numeric_scale` before building the final `ColumnAgg` (see `run()`).
pub(crate) fn agg_kind(type_str: &str) -> Option<AggKind> {
    match type_str.to_ascii_lowercase().as_str() {
        "smallint" | "mediumint" | "int" | "integer" | "bigint" => Some(AggKind::Integer),
        "decimal" | "numeric" => Some(AggKind::Decimal { scale: 0 }),
        "datetime" | "timestamp" => Some(AggKind::DatetimeSec),
        "date" => Some(AggKind::DateOnly),
        "varchar" | "char" | "text" | "tinytext" | "mediumtext" | "longtext" => Some(AggKind::TextMass),
        _ => None,
    }
}

/// Max total decimal digits DataFusion/MariaDB DECIMAL(65,x) can hold — MariaDB's DECIMAL
/// max precision, and within DataFusion's Decimal256 range (precision <= 76). VA1-r: widened
/// from 38 so a sum-only corruption on a wide-decimal column isn't hidden by the overflow
/// guard skipping the SUM.
const DECIMAL_TOTAL_DIGITS: u32 = 65;

/// Number of digits before the decimal point in a numeric string (ignoring a leading '-').
/// Used by the VA1 overflow guard to bound how many digits a SUM could grow to.
fn int_digit_count(s: &str) -> usize {
    let s = s.strip_prefix('-').unwrap_or(s);
    let int_part = s.split('.').next().unwrap_or(s);
    int_part.len().max(1)
}

/// The decimal capacity (total digits available for the integer part of a SUM) for a
/// numeric AggKind: 65 for Integer (summed as DECIMAL(65,0)), 65-scale for Decimal.
fn decimal_capacity(kind: &AggKind) -> u32 {
    match kind {
        AggKind::Integer => DECIMAL_TOTAL_DIGITS,
        AggKind::Decimal { scale } => DECIMAL_TOTAL_DIGITS.saturating_sub(*scale),
        _ => DECIMAL_TOTAL_DIGITS,
    }
}

/// VA1: would summing `non_null_count` values whose magnitude is bounded by `min`/`max`
/// risk exceeding the DECIMAL(65,scale) capacity DataFusion sums into? DataFusion silently
/// wraps/corrupts an overflowing decimal sum instead of erroring (unlike MariaDB, which
/// saturates), so a false Discrepancy would otherwise fire on huge-but-healthy sums. An
/// empty column (no min/max) never overflows.
fn sum_would_overflow(kind: &AggKind, min: Option<&str>, max: Option<&str>, non_null_count: i64) -> bool {
    if non_null_count <= 0 {
        return false;
    }
    let int_digits = [min, max]
        .into_iter()
        .flatten()
        .map(int_digit_count)
        .max();
    let Some(int_digits) = int_digits else {
        return false;
    };
    let capacity = decimal_capacity(kind) as usize;
    let count_digits = non_null_count.to_string().len();
    int_digits + count_digits > capacity
}

/// FA2 follow-up: surplus rows over distinct keys on the Delta side, i.e. DUPLICATE KEYS.
///
/// In two-stream mode the Delta table is a *current-state mirror* — both write strategies
/// (`delete_then_append` and `merge_batch`) maintain exactly one row per key — so
/// `count > distinct` is not a normal state, it is the FA2 cross-window-duplication
/// signature. It is deliberately NOT derived from any source comparison: both numbers come
/// from the same Delta-side aggregate, so this is a single-engine invariant check with no
/// cross-engine parity risk (unlike the V5/V6-r diagnostics).
///
/// Why this exists at all: `two_stream_key_stats_outcome` grades a Delta surplus as `Drift`,
/// which `run()` folds into `Clean` — precisely what made the original FA2 duplication
/// invisible to the exit code. FA2's cause is fixed (`c6ea932`), so this guards against
/// future variants of the same class.
///
/// NOTE for incremental mode this is expected and normal (an append-log legitimately holds
/// many versions per id), which is why callers must only apply it on the two-stream path.
pub(crate) fn duplicate_key_surplus(delta_stats: &KeyStats) -> Option<i64> {
    (delta_stats.count > delta_stats.distinct).then(|| delta_stats.count - delta_stats.distinct)
}

/// Canonical fingerprint assembly — the ONLY place that turns raw `ColumnAggValues` from
/// both sides into the (source, delta) fingerprint-string pairs compared in `run()`. Kept
/// centralized so both probes' differing raw reads always compare byte-for-byte, and so the
/// VA1 overflow guard (skip SUM on both sides identically) and the VA5 `n=` count component
/// are applied uniformly regardless of which adapter (real or mock) produced the values.
pub(crate) fn assemble_fingerprints(
    specs: &[ColumnAgg],
    src: &[ColumnAggValues],
    dlt: &[ColumnAggValues],
) -> Vec<(String, String)> {
    let empty = ColumnAggValues {
        sum: None,
        min: None,
        max: None,
        non_null_count: 0,
    };
    specs
        .iter()
        .enumerate()
        .map(|(i, spec)| {
            let s = src.get(i).unwrap_or(&empty);
            let d = dlt.get(i).unwrap_or(&empty);
            match &spec.kind {
                AggKind::Integer | AggKind::Decimal { .. } => {
                    let overflow = sum_would_overflow(&spec.kind, s.min.as_deref(), s.max.as_deref(), s.non_null_count)
                        || sum_would_overflow(&spec.kind, d.min.as_deref(), d.max.as_deref(), d.non_null_count);
                    if overflow {
                        println!(
                            "verify column `{}`: SUM skipped on both sides (decimal precision overflow guard — DataFusion would silently corrupt an overflowing DECIMAL sum)",
                            spec.name
                        );
                    }
                    let sum_s = if overflow { "skipped".to_string() } else { s.sum.clone().unwrap_or_else(|| "∅".to_string()) };
                    let sum_d = if overflow { "skipped".to_string() } else { d.sum.clone().unwrap_or_else(|| "∅".to_string()) };
                    let fp_s = format!(
                        "sum={}|min={}|max={}|n={}",
                        sum_s, s.min.as_deref().unwrap_or("∅"), s.max.as_deref().unwrap_or("∅"), s.non_null_count
                    );
                    let fp_d = format!(
                        "sum={}|min={}|max={}|n={}",
                        sum_d, d.min.as_deref().unwrap_or("∅"), d.max.as_deref().unwrap_or("∅"), d.non_null_count
                    );
                    (fp_s, fp_d)
                }
                AggKind::DatetimeSec | AggKind::DateOnly => {
                    let fp_s = format!("min={}|max={}|n={}", s.min.as_deref().unwrap_or("∅"), s.max.as_deref().unwrap_or("∅"), s.non_null_count);
                    let fp_d = format!("min={}|max={}|n={}", d.min.as_deref().unwrap_or("∅"), d.max.as_deref().unwrap_or("∅"), d.non_null_count);
                    (fp_s, fp_d)
                }
                AggKind::TextMass => {
                    let fp_s = format!("len={}|n={}", s.sum.as_deref().unwrap_or("0"), s.non_null_count);
                    let fp_d = format!("len={}|n={}", d.sum.as_deref().unwrap_or("0"), d.non_null_count);
                    (fp_s, fp_d)
                }
            }
        })
        .collect()
}

/// L2 key-set fingerprint over a table's PK column.
///
/// `min`/`max` are `i128` (not `i64`) so a `BIGINT UNSIGNED` key value above
/// `i64::MAX` (2^63-1) can be represented exactly instead of wrapping negative
/// under a SIGNED/bigint cast (V8). `xor`/`distinct_xor` stay `i64`: XOR is a
/// bit-pattern (not a magnitude), so a bit-preserving cast is correct on both
/// probes regardless of key range.
#[derive(Debug, Clone, PartialEq)]
pub struct KeyStats {
    pub count: i64,
    pub distinct: i64,
    pub min: Option<i128>,
    pub max: Option<i128>,
    pub xor: i64,
    pub distinct_xor: i64,
    pub sum: i128,
}

/// V3-r Tier 2: key-set fingerprint for a single-column NON-integer (string) PRIMARY key.
/// `min`/`max` are BINARY-normalized on the MySQL side (`MIN(BINARY \`k\`)`) so they compare
/// byte-for-byte against DataFusion's byte-ordered Utf8 `min`/`max` regardless of the column's
/// MySQL collation (often case-insensitive) — the N8 collation lesson applied to the key.
#[derive(Debug, Clone, PartialEq)]
pub struct StringKeyStats {
    pub count: i64,
    pub distinct: i64,
    pub min: Option<String>,
    pub max: Option<String>,
}

/// Source-side probe (the live DB).
#[cfg_attr(test, mockall::automock)]
#[allow(async_fn_in_trait)]
pub trait SourceProbe: Send + Sync {
    async fn row_count(&self, table: &str) -> Result<i64>;
    async fn row_count_scoped(&self, table: &str, scope: &SourceScope) -> Result<i64>;
    async fn max_cursor(&self, table: &str, cursor_col: &str) -> Result<Option<String>>;
    async fn columns(&self, table: &str) -> Result<Vec<ColumnMeta>>;
    /// V3: the table's single-column integer PRIMARY key, if it has one — used to derive
    /// the key-set verdict's key column instead of requiring a column literally named `id`.
    async fn integer_pk(&self, table: &str) -> Result<Option<String>>;
    /// V3-r Tier 2: the table's single-column NON-integer PRIMARY key, if it has one (returns
    /// None for an integer PK — that's handled by `integer_pk` — or a composite/absent PK).
    async fn string_pk(&self, table: &str) -> Result<Option<String>>;
    async fn key_stats(&self, table: &str, key_col: &str) -> Result<KeyStats>;
    async fn string_key_stats(&self, table: &str, key_col: &str) -> Result<StringKeyStats>;
    async fn key_stats_scoped(
        &self,
        table: &str,
        key_col: &str,
        scope: &SourceScope,
    ) -> Result<KeyStats>;
    async fn non_null_counts(&self, table: &str, columns: &[String]) -> Result<Vec<i64>>;
    async fn sample_ids(&self, table: &str, id_col: &str, limit: i64) -> Result<Vec<i64>>;
    async fn sample_rows(
        &self,
        table: &str,
        id_col: &str,
        columns: &[String],
        ids: &[i64],
    ) -> Result<HashMap<i64, Vec<Option<String>>>>;
    async fn value_aggregates(&self, table: &str, columns: &[ColumnAgg]) -> Result<Vec<ColumnAggValues>>;
    async fn value_aggregates_scoped(&self, table: &str, columns: &[ColumnAgg], scope: &SourceScope) -> Result<Vec<ColumnAggValues>>;
}

/// Delta-side probe (the synced output).
#[cfg_attr(test, mockall::automock)]
#[allow(async_fn_in_trait)]
pub trait DeltaProbe: Send + Sync {
    async fn row_count(&self, table: &str) -> Result<i64>;
    async fn max_cursor(&self, table: &str, cursor_col: &str) -> Result<Option<String>>;
    async fn columns(&self, table: &str) -> Result<Vec<ColumnMeta>>;
    async fn key_stats(&self, table: &str, key_col: &str) -> Result<KeyStats>;
    async fn string_key_stats(&self, table: &str, key_col: &str) -> Result<StringKeyStats>;
    async fn latest_key_stats(
        &self,
        table: &str,
        key_col: &str,
        cursor_col: &str,
    ) -> Result<KeyStats>;
    async fn non_null_counts(&self, table: &str, columns: &[String]) -> Result<Vec<i64>>;
    async fn sample_rows(
        &self,
        table: &str,
        id_col: &str,
        columns: &[String],
        ids: &[i64],
    ) -> Result<HashMap<i64, Vec<Option<String>>>>;
    async fn value_aggregates(&self, table: &str, columns: &[ColumnAgg]) -> Result<Vec<ColumnAggValues>>;
    async fn value_aggregates_latest(&self, table: &str, columns: &[ColumnAgg], cursor_col: &str, scope: &SourceScope) -> Result<Vec<ColumnAggValues>>;
}

pub struct VerifyCommand<S, D> {
    source: S,
    delta: D,
    tables: Vec<String>,
    table_plans: std::collections::HashMap<String, TablePlan>,
    deep: bool,
    row_cap: i64,
}
impl<S: SourceProbe, D: DeltaProbe> VerifyCommand<S, D> {
    pub fn new(source: S, delta: D, tables: Vec<String>) -> Self {
        let table_plans = tables
            .iter()
            .cloned()
            .map(|table| {
                let plan = TablePlan::basic(table.clone());
                (table, plan)
            })
            .collect();
        Self {
            source,
            delta,
            tables,
            table_plans,
            deep: false,
            row_cap: DEFAULT_ROW_CAP,
        }
    }

    pub fn with_table_plans(mut self, table_plans: Vec<TablePlan>) -> Self {
        self.table_plans = table_plans
            .into_iter()
            .map(|plan| (plan.table.clone(), plan))
            .collect();
        self
    }

    pub fn with_deep(mut self, deep: bool) -> Self {
        self.deep = deep;
        self
    }

    /// Override the size-guard row cap (default 1_000_000). Below `row_cap` source rows a
    /// non-deep verify runs fully; above it (and !deep) the table is Skipped. Exposed for tests
    /// exercising the size-guard tier without seeding a million rows.
    pub fn with_row_cap(mut self, row_cap: i64) -> Self {
        self.row_cap = row_cap;
        self
    }

    fn key_stats_outcome(
        source_label: &str,
        delta_label: &str,
        source_stats: &KeyStats,
        delta_stats: &KeyStats,
    ) -> TableOutcome {
        if source_stats == delta_stats
            || (source_stats.distinct == delta_stats.distinct
                && source_stats.min == delta_stats.min
                && source_stats.max == delta_stats.max
                && source_stats.distinct_xor == delta_stats.distinct_xor
                && source_stats.sum == delta_stats.sum)
        {
            TableOutcome::Pass
        } else if delta_stats.distinct < source_stats.distinct
            && (delta_stats.max.is_none()
                || (source_stats.max.is_some() && delta_stats.max <= source_stats.max))
            && (delta_stats.min.is_none()
                || (source_stats.min.is_some() && delta_stats.min >= source_stats.min))
        {
            TableOutcome::Drift {
                reason: format!(
                    "{source_label} advanced past sync: {source_label} distinct={} {delta_label} distinct={} — likely new/changed rows since sync, not a sync error",
                    source_stats.distinct, delta_stats.distinct
                ),
            }
        } else {
            TableOutcome::Discrepancy {
                reason: format!(
                    "{delta_label} has ids/rows not in {source_label}: {source_label}(distinct={} min={:?} max={:?}) {delta_label}(distinct={} min={:?} max={:?})",
                    source_stats.distinct,
                    source_stats.min,
                    source_stats.max,
                    delta_stats.distinct,
                    delta_stats.min,
                    delta_stats.max
                ),
            }
        }
    }

    /// PS-M1: incremental-scoped verdict for `source_scoped` vs `delta_latest`. Mirrors
    /// `key_stats_outcome`'s Pass and delta-lagging-Drift cases, but ALSO treats a
    /// `delta_latest` **superset** (more distinct keys than `source_scoped`) whose key range
    /// still sits *inside* `source_scoped`'s own [min, max] envelope as benign `Drift`, not
    /// `Discrepancy`.
    ///
    /// Why this asymmetry is safe here specifically: `source_scoped` is re-scoped to
    /// `updated_at <= HWM` on every verify run. A row that was already synced can have its
    /// source `updated_at` advance past the stored HWM *after* the sync (an ordinary update on
    /// an update-active table) — that row then drops out of `source_scoped` even though its
    /// already-synced version legitimately remains in `delta_latest`. Because that row's key
    /// pre-dates the sync, its value is necessarily within `source_scoped`'s own key range, so
    /// `delta_latest`'s superset range never needs to extend beyond `source_scoped`'s bounds to
    /// explain it. That is expected steady-state behavior, not a sync error, so it must not
    /// trip the DISCREPANCY exit code. (This is a different asymmetry direction than
    /// `two_stream_key_stats_outcome`'s, which allows `delta` to have a *wider* range than
    /// `source` for its own, unrelated reason — two-stream's `source` is deliberately a
    /// narrower recent window, not a superset-explaining envelope.)
    ///
    /// Discrepancy is still returned when the surplus is NOT explained by range containment
    /// (`delta_latest` has a key strictly below `source_scoped`'s min or strictly above its
    /// max — a genuine phantom or extra key that the HWM-advance explanation cannot account
    /// for), or when the stats disagree in a way unrelated to distinct-count direction (e.g.
    /// equal distinct/min/max but a mismatched `sum`/`xor`, indicating corrupted values).
    ///
    /// NOTE: `key_stats_outcome` (used by the full_refresh/basic, non-scoped path) is
    /// deliberately left untouched — there a delta superset IS a real bug, since the source
    /// there is not scoped and represents the full authoritative row/key-set.
    fn incremental_scoped_key_stats_outcome(
        source_label: &str,
        delta_label: &str,
        source_stats: &KeyStats,
        delta_stats: &KeyStats,
    ) -> TableOutcome {
        if source_stats == delta_stats
            || (source_stats.distinct == delta_stats.distinct
                && source_stats.min == delta_stats.min
                && source_stats.max == delta_stats.max
                && source_stats.distinct_xor == delta_stats.distinct_xor
                && source_stats.sum == delta_stats.sum)
        {
            return TableOutcome::Pass;
        }

        // delta_latest's key range sits within source_scoped's own [min, max] envelope (or
        // either side has no keys at all) — true regardless of which side has more distinct
        // keys, so it's shared by both the lagging and the superset branches below.
        let delta_range_within_source = (delta_stats.max.is_none()
            || (source_stats.max.is_some() && delta_stats.max <= source_stats.max))
            && (delta_stats.min.is_none()
                || (source_stats.min.is_some() && delta_stats.min >= source_stats.min));

        if delta_stats.distinct < source_stats.distinct && delta_range_within_source {
            // Delta lagging behind source_scoped: source_scoped advanced past the last sync
            // (new/changed rows since sync), not a sync error. Unchanged from
            // `key_stats_outcome`.
            TableOutcome::Drift {
                reason: format!(
                    "{source_label} advanced past sync: {source_label} distinct={} {delta_label} distinct={} — likely new/changed rows since sync, not a sync error",
                    source_stats.distinct, delta_stats.distinct
                ),
            }
        } else if delta_stats.distinct > source_stats.distinct && delta_range_within_source {
            // Delta superset, but its range is still contained by source_scoped's own bounds:
            // benign HWM-scope drift (see doc comment above).
            TableOutcome::Drift {
                reason: format!(
                    "{delta_label} retains rows whose {source_label} updated_at advanced past the HWM scope after sync: {source_label}(count={} distinct={} min={:?} max={:?}) {delta_label}(count={} distinct={} min={:?} max={:?}) — delta_latest's extra keys fall within source_scoped's own range, consistent with already-synced rows that were updated after the sync; benign steady-state drift on an update-active incremental table, not a sync error",
                    source_stats.count,
                    source_stats.distinct,
                    source_stats.min,
                    source_stats.max,
                    delta_stats.count,
                    delta_stats.distinct,
                    delta_stats.min,
                    delta_stats.max
                ),
            }
        } else {
            TableOutcome::Discrepancy {
                reason: format!(
                    "{delta_label} has ids/rows not in {source_label}: {source_label}(distinct={} min={:?} max={:?}) {delta_label}(distinct={} min={:?} max={:?})",
                    source_stats.distinct,
                    source_stats.min,
                    source_stats.max,
                    delta_stats.distinct,
                    delta_stats.min,
                    delta_stats.max
                ),
            }
        }
    }

    /// V3-r Tier 2: key-set verdict for a single-column string PRIMARY key, mirroring
    /// `key_stats_outcome`'s Drift-vs-Discrepancy split but over the string fingerprint
    /// (count/distinct/BINARY-normalized min/max).
    fn string_key_stats_outcome(
        source_label: &str,
        delta_label: &str,
        s: &StringKeyStats,
        d: &StringKeyStats,
    ) -> TableOutcome {
        if s == d {
            return TableOutcome::Pass;
        }
        // Delta lagging (fewer distinct, its [min,max] within source's byte range) -> Drift
        // (source advanced past sync), mirroring the integer key_stats_outcome Drift rule.
        let delta_range_inside_source = match (&s.min, &s.max, &d.min, &d.max) {
            (Some(smin), Some(smax), Some(dmin), Some(dmax)) => dmin >= smin && dmax <= smax,
            (None, None, None, None) => true,
            _ => false,
        };
        if d.distinct < s.distinct && delta_range_inside_source {
            TableOutcome::Drift {
                reason: format!(
                    "{source_label} advanced past sync: {source_label} distinct={} {delta_label} distinct={} (string key) — likely new rows since sync",
                    s.distinct, d.distinct
                ),
            }
        } else {
            TableOutcome::Discrepancy {
                reason: format!(
                    "string key-set mismatch: {source_label}(count={} distinct={} min={:?} max={:?}) {delta_label}(count={} distinct={} min={:?} max={:?})",
                    s.count, s.distinct, s.min, s.max, d.count, d.distinct, d.min, d.max
                ),
            }
        }
    }

    fn two_stream_key_stats_outcome(
        source_label: &str,
        delta_label: &str,
        source_stats: &KeyStats,
        delta_stats: &KeyStats,
    ) -> TableOutcome {
        if source_stats == delta_stats {
            return TableOutcome::Pass;
        }

        let source_range_contained_by_delta = match (source_stats.min, source_stats.max) {
            (None, None) => true,
            (Some(source_min), Some(source_max)) => match (delta_stats.min, delta_stats.max) {
                (Some(delta_min), Some(delta_max)) => {
                    delta_min <= source_min && source_max <= delta_max
                }
                _ => false,
            },
            _ => false,
        };

        let delta_has_extra_evidence =
            source_stats.count < delta_stats.count || source_stats.distinct < delta_stats.distinct;

        if delta_has_extra_evidence
            && source_stats.count <= delta_stats.count
            && source_stats.distinct <= delta_stats.distinct
            && source_range_contained_by_delta
        {
            TableOutcome::Drift {
                reason: format!(
                    "{delta_label} may legitimately retain extra ids/rows in two_stream mode: aggregate stats show {source_label}(count={} distinct={} min={:?} max={:?}) within {delta_label}(count={} distinct={} min={:?} max={:?}), but this does not prove exact set equality",
                    source_stats.count,
                    source_stats.distinct,
                    source_stats.min,
                    source_stats.max,
                    delta_stats.count,
                    delta_stats.distinct,
                    delta_stats.min,
                    delta_stats.max
                ),
            }
        } else {
            TableOutcome::Discrepancy {
                reason: format!(
                    "{source_label} appears to have ids/rows missing from {delta_label} in two_stream mode: {source_label}(count={} distinct={} min={:?} max={:?}) {delta_label}(count={} distinct={} min={:?} max={:?})",
                    source_stats.count,
                    source_stats.distinct,
                    source_stats.min,
                    source_stats.max,
                    delta_stats.count,
                    delta_stats.distinct,
                    delta_stats.min,
                    delta_stats.max
                ),
            }
        }
    }

    /// VS2b: drift-gated tiered verdict + exit codes.
    /// Per-table verdict logic:
    /// 1. Schema: if missing_in_delta -> Discrepancy
    /// 2. Size guard: if !deep && source_row_count > row_cap -> Skipped
    /// 3. Count/key-set (if id column exists):
    ///    - Full match (all fields equal) -> Pass
    ///    - Distinct fallback (distinct+min+max+distinct_xor match) -> Pass
    ///    - Drift (delta range inside source range, delta smaller) -> Drift (not a failure)
    ///    - Two-stream uses a separate conservative asymmetry path
    ///    - Else -> Discrepancy
    pub async fn run(&self) -> Result<VerifyVerdict> {
        let mut outcomes = Vec::new();

        for table in &self.tables {
            let default_plan = TablePlan::basic(table.clone());
            let plan = self.table_plans.get(table).unwrap_or(&default_plan);
            // VA4: a probe error on one table (garbage data, transient connectivity, etc.)
            // must not abort the whole run — capture it as a per-table Skipped outcome so
            // every other table still gets verified and `run()` still returns Ok.
            match self.run_one_table(table, plan).await {
                Ok(outcome) => outcomes.push(outcome),
                Err(e) => {
                    println!("verify {table} VERDICT: SKIPPED: probe error: {e:#}");
                    outcomes.push(TableOutcome::Skipped {
                        reason: format!("probe error: {e:#}"),
                    });
                }
            }
        }

        let pass_count = outcomes
            .iter()
            .filter(|o| **o == TableOutcome::Pass)
            .count();
        let drift_count = outcomes
            .iter()
            .filter(|o| matches!(o, TableOutcome::Drift { .. }))
            .count();
        let discrepancy_count = outcomes
            .iter()
            .filter(|o| matches!(o, TableOutcome::Discrepancy { .. }))
            .count();
        let skipped_count = outcomes
            .iter()
            .filter(|o| matches!(o, TableOutcome::Skipped { .. }))
            .count();
        let partially_verified_count = outcomes
            .iter()
            .filter(|o| matches!(o, TableOutcome::PartiallyVerified { .. }))
            .count();

        println!(
            "verify summary: pass={} drift={} discrepancy={} skipped={} partially_verified={}",
            pass_count, drift_count, discrepancy_count, skipped_count, partially_verified_count
        );

        if outcomes
            .iter()
            .any(|o| matches!(o, TableOutcome::Discrepancy { .. }))
        {
            Ok(VerifyVerdict::Discrepancy)
        } else if outcomes
            .iter()
            .any(|o| matches!(o, TableOutcome::PartiallyVerified { .. }))
        {
            Ok(VerifyVerdict::PartiallyVerified)
        } else {
            Ok(VerifyVerdict::Clean)
        }
    }

    /// Per-table verify body, extracted from `run()` so a probe error on one table can be
    /// captured as `Skipped` by the caller instead of aborting every other table (VA4).
    pub async fn run_one_table(&self, table: &str, plan: &TablePlan) -> Result<TableOutcome> {
            println!("verify {table} plan: {}", plan.describe());

            let scols = self.source.columns(table).await?;
            let dcols = self.delta.columns(table).await?;
            let dnames: std::collections::HashSet<&str> =
                dcols.iter().map(|c| c.name.as_str()).collect();
            let snames: std::collections::HashSet<&str> =
                scols.iter().map(|c| c.name.as_str()).collect();
            let missing_in_delta: Vec<&str> = scols
                .iter()
                .map(|c| c.name.as_str())
                .filter(|n| !dnames.contains(n))
                .collect();
            let extra_in_delta: Vec<&str> = dcols
                .iter()
                .map(|c| c.name.as_str())
                .filter(|n| !snames.contains(n))
                .collect();
            let schema_flag = if missing_in_delta.is_empty() && extra_in_delta.is_empty() {
                "schema ok"
            } else {
                "SCHEMA DIFF"
            };
            println!(
                "verify {table} schema: source_cols={} delta_cols={} missing_in_delta={:?} extra_in_delta={:?}  [{schema_flag}]",
                scols.len(),
                dcols.len(),
                missing_in_delta,
                extra_in_delta
            );
            // V5 (diagnostic, does NOT affect the verdict): flag columns whose TYPE FAMILY differs
            // between source and Delta (e.g. a column that was int is now text). A genuine type
            // change can't reach here through parket (the run + --check evolution guards bail on
            // it), so this surfaces external Delta tampering / drift for the operator; it is
            // intentionally NOT a Discrepancy (cross-engine type representations differ; only the
            // parity-hardened key-set + value-aggregate checks gate the verdict).
            let type_diffs: Vec<String> = scols
                .iter()
                .filter_map(|sc| {
                    let dc = dcols.iter().find(|d| d.name == sc.name)?;
                    let sf = source_type_family(&sc.type_str);
                    let df = delta_type_family(&dc.type_str);
                    (sf != df).then(|| format!("{} (source {}=[{}] vs Delta {}=[{}])", sc.name, sc.type_str, sf, dc.type_str, df))
                })
                .collect();
            if !type_diffs.is_empty() {
                println!(
                    "verify {table} schema TYPE DIFF (diagnostic — does not affect verdict): {}",
                    type_diffs.join(", ")
                );
            }
            if let Some(cursor) = plan.mode.freshness_cursor() {
                let source_max = self.source.max_cursor(table, cursor).await?;
                let delta_max = self.delta.max_cursor(table, cursor).await?;
                println!(
                    "verify {table} freshness: cursor={cursor} source_max={source_max:?} delta_max={delta_max:?}"
                );
            }
            // V3: resolve the real key column instead of requiring one literally named
            // `id`. Two-stream mode's insert_cursor IS the config-declared intent (matches
            // pipeline semantics) so it's used directly without probing. Every other mode
            // asks the source for the table's single-column integer PRIMARY key, falling
            // back to an `id` column (if present) to preserve pre-V3 behavior for
            // id-keyed tables that have no declared PK.
            let key_col: Option<String> = match &plan.mode {
                VerifyMode::TwoStream { insert_cursor, .. } => Some(insert_cursor.clone()),
                _ => match self.source.integer_pk(table).await? {
                    Some(key) => Some(key),
                    None => scols.iter().find(|c| c.name == "id").map(|c| c.name.clone()),
                },
            };
            let has_key = key_col.is_some();

            // V3-r Tier 2: no integer key (and no `id` fallback) — check for a single-column
            // NON-integer (string) PRIMARY key instead. Two-stream is excluded because its key
            // is the config-declared insert_cursor (always resolved above), and a string key
            // wouldn't work as an insert cursor anyway. Kept in its own variable, separate from
            // `key_col`, so it never flows into the integer-only scoped/two-stream/census/sample
            // paths below (which cast the key as i64).
            let string_key: Option<String> = if key_col.is_none()
                && !matches!(&plan.mode, VerifyMode::TwoStream { .. })
            {
                self.source.string_pk(table).await?
            } else {
                None
            };

            let incremental_scope = match (&plan.mode, key_col.as_ref()) {
                (
                    VerifyMode::Incremental {
                        cursor_col,
                        hwm: Some(hwm),
                    },
                    Some(key),
                ) => Some(SourceScope {
                    cursor_col: cursor_col.clone(),
                    updated_at: hwm.updated_at.clone(),
                    last_id: hwm.last_id,
                    key_col: key.clone(),
                }),
                _ => None,
            };

            let mut delta_keystats: Option<KeyStats> = None;
            let skip_pass_layers_reason = if incremental_scope.is_some() {
                Some("row-level census/sample deferred for incremental scope (value-aggregates checked separately)")
            } else {
                None
            };

            // VA3/V4: the row-cap guard must run BEFORE any full-log window sort. `row_count`
            // (source: COUNT(*); delta: count(*) over the physical log) is cheap; the
            // key-set/latest-per-id computation below (which requires a full-log window sort
            // on the Delta side) only runs once we know we're under the cap.
            let src_row_count;
            let delta_physical_row_count;
            let delta_label;
            if let Some(scope) = incremental_scope.as_ref().filter(|_| has_key) {
                src_row_count = self.source.row_count_scoped(table, scope).await?;
                delta_physical_row_count = self.delta.row_count(table).await?;
                delta_label = "delta_latest";
                let flag = if src_row_count == delta_physical_row_count {
                    "match"
                } else {
                    "differ — see verdict"
                };
                println!(
                    "verify {table} incremental scope: source_scoped={src_row_count} delta_latest={delta_physical_row_count} cursor={} hwm=updated_at={} last_id={}  [{flag}]",
                    scope.cursor_col,
                    scope.updated_at,
                    scope.last_id
                );
            } else {
                src_row_count = self.source.row_count(table).await?;
                delta_physical_row_count = self.delta.row_count(table).await?;
                delta_label = "delta";
                let flag = if src_row_count == delta_physical_row_count {
                    "match"
                } else {
                    "differ — see verdict"
                };
                println!("verify {table}: source={src_row_count} delta={delta_physical_row_count}  [{flag}]");
            }

            let mut outcome = if !missing_in_delta.is_empty() {
                TableOutcome::Discrepancy {
                    reason: format!("missing columns in Delta: {:?}", missing_in_delta),
                }
            } else if !self.deep
                && (src_row_count > self.row_cap || delta_physical_row_count > self.row_cap)
            {
                TableOutcome::Skipped {
                    reason: format!(
                        "table has {src_row_count} source rows / {delta_physical_row_count} delta rows (> cap {cap}); pass --verify-deep to force strict checks",
                        cap = self.row_cap
                    ),
                }
            } else if let Some(skey) = string_key.as_ref() {
                // V3-r Tier 2: single-column string PK — a real key-set fingerprint
                // (count/distinct/BINARY-min/max) instead of value-aggregates-only.
                let s = self.source.string_key_stats(table, skey).await?;
                let d = self.delta.string_key_stats(table, skey).await?;
                Self::string_key_stats_outcome("source", "delta", &s, &d)
            } else if !has_key {
                // V3-r Tier 1: no usable key for a key-set fingerprint, but DON'T skip everything —
                // fall through to the value-aggregate checks below (they need no key). This sentinel
                // means "key-set/row-set completeness unverified"; the value-agg block runs and, if
                // clean, the table ends PartiallyVerified (not Clean) rather than silently Skipped.
                TableOutcome::PartiallyVerified {
                    reason: "no single-column integer PRIMARY key (or `id` column): row-set completeness not key-verified; value-aggregates only".to_string(),
                }
            } else if let Some(scope) = incremental_scope.as_ref() {
                let key = key_col.as_deref().unwrap();
                let s = self.source.key_stats_scoped(table, key, scope).await?;
                let d = self
                    .delta
                    .latest_key_stats(table, key, &scope.cursor_col)
                    .await?;
                delta_keystats = Some(d.clone());
                Self::incremental_scoped_key_stats_outcome("source_scoped", "delta_latest", &s, &d)
            } else {
                let key = key_col.as_deref().unwrap();
                let s = self.source.key_stats(table, key).await?;
                let d = self.delta.key_stats(table, key).await?;
                delta_keystats = Some(d.clone());
                if matches!(&plan.mode, VerifyMode::TwoStream { .. }) {
                    // FA2 follow-up: two-stream keeps exactly one row per key, so a surplus
                    // over distinct keys means duplicated rows — the FA2 class. The verdict
                    // below grades a Delta surplus as Drift (folded to Clean by `run()`),
                    // which is what hid the original bug, so say it out loud here.
                    // Diagnostic-only deliberately: a table synced BEFORE the FA2 fix
                    // (`c6ea932`) can still carry residual duplicates, and hard-failing those
                    // runs would regress existing deployments. Promotion to Discrepancy is
                    // safe once live tables are confirmed clean (register FA2-r2) — there is
                    // no cross-engine parity risk in this check.
                    if let Some(surplus) = duplicate_key_surplus(&d) {
                        println!(
                            "verify {table} DUPLICATE KEYS in {delta_label}: count={} > distinct={} \
                             ({surplus} surplus row(s); two_stream holds one row per key — FA2-class \
                             duplication, remediate with a TABLE_RECONCILE one-shot) \
                             (diagnostic — does not affect verdict)",
                            d.count, d.distinct
                        );
                    }
                    Self::two_stream_key_stats_outcome("source", delta_label, &s, &d)
                } else {
                    Self::key_stats_outcome("source", delta_label, &s, &d)
                }
            };

            // Per-column value verification. A mismatch downgrades Pass -> Discrepancy.
            //  - incremental-with-HWM (scoped): source scoped-to-HWM vs Delta latest-per-id
            //    scoped-to-HWM (V1b-2).
            //  - non-scoped, non-append-log (full-refresh/basic/two-stream): full compare (V1b-1).
            //  - no-HWM append-log incremental: no fair window -> skip with a note.
            if matches!(outcome, TableOutcome::Pass | TableOutcome::PartiallyVerified { .. }) {
                let specs: Vec<ColumnAgg> = scols
                    .iter()
                    .filter(|c| {
                        dnames.contains(c.name.as_str())
                            && Some(c.name.as_str()) != key_col.as_deref()
                    })
                    .filter_map(|c| {
                        agg_kind(&c.type_str).map(|k| {
                            // VA2: decimals aggregate at the column's own NATIVE scale
                            // (from information_schema.NUMERIC_SCALE) instead of a fixed
                            // scale — otherwise round-then-sum (source) vs sum-then-round
                            // (fixed-scale cast) diverge deterministically for scale>10.
                            let kind = match k {
                                AggKind::Decimal { .. } => AggKind::Decimal {
                                    scale: c.numeric_scale.unwrap_or(10).min(30),
                                },
                                other => other,
                            };
                            ColumnAgg { name: c.name.clone(), kind }
                        })
                    })
                    .collect();
                if !specs.is_empty() {
                    let pair: Option<(Vec<ColumnAggValues>, Vec<ColumnAggValues>)> =
                        if let Some(scope) = incremental_scope.as_ref() {
                            Some((
                                self.source.value_aggregates_scoped(table, &specs, scope).await?,
                                self.delta
                                    .value_aggregates_latest(table, &specs, &scope.cursor_col, scope)
                                    .await?,
                            ))
                        } else if matches!(&delta_keystats, Some(d) if d.count != d.distinct) {
                            println!(
                                "verify {table} value-aggregates: skipped (append-log without HWM — no fair comparison window)"
                            );
                            None
                        } else {
                            Some((
                                self.source.value_aggregates(table, &specs).await?,
                                self.delta.value_aggregates(table, &specs).await?,
                            ))
                        };
                    if let Some((sv, dv)) = pair {
                        let fingerprints = assemble_fingerprints(&specs, &sv, &dv);
                        let mismatches: Vec<String> = specs
                            .iter()
                            .zip(fingerprints.iter())
                            .filter(|(_, (s, d))| s != d)
                            .map(|(spec, (s, d))| {
                                format!("{} ({:?}: source={} delta={})", spec.name, spec.kind, s, d)
                            })
                            .collect();
                        if mismatches.is_empty() {
                            println!("verify {table} value-aggregates: {} column(s) match", specs.len());
                        } else {
                            outcome = TableOutcome::Discrepancy {
                                reason: format!("column value mismatch: {}", mismatches.join(", ")),
                            };
                        }
                    }
                }
            }

            match &outcome {
                TableOutcome::Pass => {
                    println!("verify {table} VERDICT: PASS");
                }
                TableOutcome::Drift { reason } => {
                    println!("verify {table} VERDICT: DRIFT: {reason}");
                }
                TableOutcome::Discrepancy { reason } => {
                    println!("verify {table} VERDICT: DISCREPANCY: {reason}");
                }
                TableOutcome::Skipped { reason } => {
                    println!("verify {table} VERDICT: SKIPPED: {reason}");
                }
                TableOutcome::PartiallyVerified { reason } => {
                    println!("verify {table} VERDICT: PARTIALLY_VERIFIED: {reason}");
                }
            }

            if matches!(&outcome, TableOutcome::Pass) && key_col.is_some() {
                if let Some(reason) = skip_pass_layers_reason {
                    println!("verify {table} non-null census: skipped ({reason})");
                    println!("verify {table} sample: skipped ({reason})");
                } else {
                    let delta_appendlog =
                        matches!(&delta_keystats, Some(d) if d.count != d.distinct);
                    if delta_appendlog {
                        let d = delta_keystats.as_ref().unwrap();
                        println!(
                            "verify {table} non-null census: skipped (Delta is append-log: {} rows / {} distinct ids — needs latest-per-id dedup)",
                            d.count, d.distinct
                        );
                        println!(
                            "verify {table} sample: skipped (Delta is append-log: {} rows / {} distinct ids)",
                            d.count, d.distinct
                        );
                    } else {
                        let common: Vec<String> = scols
                            .iter()
                            .filter(|c| dnames.contains(c.name.as_str()))
                            .map(|c| c.name.clone())
                            .collect();
                        if !common.is_empty() {
                            let scounts = self.source.non_null_counts(table, &common).await?;
                            let dcounts = self.delta.non_null_counts(table, &common).await?;
                            let diffs: Vec<(String, i64, i64)> = common
                                .iter()
                                .zip(scounts.iter())
                                .zip(dcounts.iter())
                                .filter_map(|((col, s), d)| {
                                    if s != d {
                                        Some((col.clone(), *s, *d))
                                    } else {
                                        None
                                    }
                                })
                                .collect();
                            if diffs.is_empty() {
                                println!(
                                    "verify {table} non-null census: {} columns match",
                                    common.len()
                                );
                            } else {
                                let diff_str = diffs
                                    .iter()
                                    .map(|(col, s, d)| format!("{col}={s}/{d}"))
                                    .collect::<Vec<_>>()
                                    .join(", ");
                                println!(
                                    "verify {table} non-null census: DIFFERS in {} column(s): {diff_str} (diagnostic — does not affect verdict)",
                                    diffs.len()
                                );
                            }
                        }

                        let key = key_col.as_deref().unwrap();
                        let comparable: Vec<String> = scols
                            .iter()
                            .filter(|c| {
                                dnames.contains(c.name.as_str())
                                    && is_value_comparable(&c.type_str)
                                    && c.name != key
                            })
                            .map(|c| c.name.clone())
                            .collect();
                        if !comparable.is_empty() {
                            let ids = self.source.sample_ids(table, key, SAMPLE_SIZE).await?;
                            if !ids.is_empty() {
                                let srows = self
                                    .source
                                    .sample_rows(table, key, &comparable, &ids)
                                    .await?;
                                let drows = self
                                    .delta
                                    .sample_rows(table, key, &comparable, &ids)
                                    .await?;
                                let mut matched = 0;
                                let mut missing = 0;
                                let mut differing: Vec<(i64, Vec<String>)> = vec![];
                                for id in &ids {
                                    match (srows.get(id), drows.get(id)) {
                                        (Some(sv), Some(dv)) => {
                                            let diff_cols: Vec<String> = comparable
                                                .iter()
                                                .enumerate()
                                                .filter_map(|(i, c)| {
                                                    if sv.get(i) != dv.get(i) {
                                                        Some(c.clone())
                                                    } else {
                                                        None
                                                    }
                                                })
                                                .collect();
                                            if diff_cols.is_empty() {
                                                matched += 1;
                                            } else {
                                                differing.push((*id, diff_cols));
                                            }
                                        }
                                        _ => {
                                            missing += 1;
                                        }
                                    }
                                }
                                println!(
                                    "verify {table} sample: checked={} match={} differ={} missing={} (diagnostic — does not affect verdict)",
                                    ids.len(),
                                    matched,
                                    differing.len(),
                                    missing
                                );
                                for (id, cols) in differing {
                                    println!(
                                        "verify {table} sample row {id}: differing columns {cols:?}"
                                    );
                                }
                            }
                        }
                    }
                }
            } else if matches!(&outcome, TableOutcome::Pass) {
                // V3-r Tier 2: a string-keyed Pass has no integer key to sample/census by
                // (census/sample below cast the key as i64) — its row-set completeness was
                // already fully key-verified above via the string key-set fingerprint.
                println!(
                    "verify {table} sample: skipped (string key — key-set verified via count/distinct/min/max)"
                );
            }

            Ok(outcome)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// V5: pins the no-false-positive invariant — for every column type parket actually syncs,
    /// `source_type_family(source)` must equal `delta_type_family(delta)`. Pairs reflect what
    /// `orchestrator::schema::mariadb_type_to_arrow` really produces (verified against the
    /// vendored connector_arrow's MySQL reader), NOT a naive "decimal->Decimal128" assumption:
    /// decimal/date/datetime/json all round-trip through Delta as Utf8, so their family is
    /// "string" on both sides, matching every other Utf8-backed source type.
    #[test]
    fn source_type_family_and_delta_type_family_agree_on_synced_types() {
        let synced_pairs = [
            ("bigint", "Int64"),
            ("int", "Int32"),
            ("tinyint", "Int32"), // N5 widens unsigned tinyint; still same "int" family
            ("smallint", "Int16"),
            ("mediumint", "Int32"),
            ("double", "Float64"),
            ("float", "Float32"),
            ("varchar", "Utf8"),
            ("char", "Utf8View"),
            ("text", "Utf8"),
            // decimal/date/datetime/timestamp/json all map to Utf8 on the Delta side
            // (mariadb_type_to_arrow), so their family must be "string", matching Utf8's family.
            ("decimal", "Utf8"),
            ("date", "Utf8"),
            ("datetime", "Utf8"),
            ("timestamp", "Utf8"),
            ("json", "Utf8"),
            ("blob", "Binary"),
            ("varbinary", "Binary"),
        ];
        for (source_type, delta_type) in synced_pairs {
            assert_eq!(
                source_type_family(source_type),
                delta_type_family(delta_type),
                "expected matching families for synced pair ({source_type}, {delta_type})"
            );
        }
    }

    #[test]
    fn type_family_detects_category_drift() {
        // A real drift (int source column now text on the Delta side) must still be flagged.
        assert_ne!(source_type_family("int"), delta_type_family("Utf8"));
        assert_ne!(source_type_family("varchar"), delta_type_family("Int64"));
    }

    /// V5: build a mock pair where a column's type family genuinely differs between source and
    /// Delta (category drift), everything else (key-set + value-aggregates) clean, and assert
    /// `run()` still returns `Clean` — proving the type-family diagnostic is print-only and never
    /// flips the verdict. The drifted column ("notes": source `blob`, delta `Utf8`) has no
    /// `agg_kind` on the source side, so it's excluded from value-aggregates entirely and can't
    /// disturb that check.
    #[tokio::test]
    async fn verify_type_family_diff_is_diagnostic_not_discrepancy() {
        let mut source = MockSourceProbe::new();
        let mut delta = MockDeltaProbe::new();
        source.expect_integer_pk().returning(|_| Ok(None));
        source.expect_row_count().returning(|_| Ok(2));
        delta.expect_row_count().returning(|_| Ok(2));

        let scols = || {
            Ok(vec![
                ColumnMeta {
                    name: "id".to_string(),
                    type_str: "bigint".to_string(),
                    nullable: false,
                    numeric_scale: None,
                },
                ColumnMeta {
                    name: "name".to_string(),
                    type_str: "varchar".to_string(),
                    nullable: true,
                    numeric_scale: None,
                },
                ColumnMeta {
                    name: "notes".to_string(),
                    type_str: "blob".to_string(), // family "binary"
                    nullable: true,
                    numeric_scale: None,
                },
            ])
        };
        let dcols = || {
            Ok(vec![
                ColumnMeta {
                    name: "id".to_string(),
                    type_str: "Int64".to_string(),
                    nullable: false,
                    numeric_scale: None,
                },
                ColumnMeta {
                    name: "name".to_string(),
                    type_str: "Utf8".to_string(),
                    nullable: true,
                    numeric_scale: None,
                },
                ColumnMeta {
                    name: "notes".to_string(),
                    type_str: "Utf8".to_string(), // family "string" -> DRIFT vs source "binary"
                    nullable: true,
                    numeric_scale: None,
                },
            ])
        };
        source.expect_columns().returning(move |_| scols());
        delta.expect_columns().returning(move |_| dcols());

        let stats = || {
            Ok(KeyStats {
                count: 2,
                distinct: 2,
                min: Some(1),
                max: Some(2),
                xor: 3,
                distinct_xor: 3,
                sum: 3,
            })
        };
        source.expect_key_stats().returning(move |_, _| stats());
        delta.expect_key_stats().returning(move |_, _| stats());

        source
            .expect_non_null_counts()
            .returning(|_, cols: &[String]| Ok(vec![2i64; cols.len()]));
        delta
            .expect_non_null_counts()
            .returning(|_, cols: &[String]| Ok(vec![2i64; cols.len()]));

        // "name" (varchar) is the only column with an AggKind (TextMass); "notes" (blob) has
        // none, so it never reaches value_aggregates.
        source.expect_value_aggregates().returning(|_, _| {
            Ok(vec![ColumnAggValues {
                sum: Some("10".to_string()),
                min: None,
                max: None,
                non_null_count: 2,
            }])
        });
        delta.expect_value_aggregates().returning(|_, _| {
            Ok(vec![ColumnAggValues {
                sum: Some("10".to_string()),
                min: None,
                max: None,
                non_null_count: 2,
            }])
        });

        source.expect_sample_ids().returning(|_, _, _| Ok(vec![]));

        let cmd = VerifyCommand::new(source, delta, vec!["orders".to_string()]);
        let result = cmd.run().await;
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            VerifyVerdict::Clean,
            "a type-family drift must be diagnostic-only and never produce a Discrepancy"
        );
    }

    #[test]
    fn table_plan_describe_includes_mode_and_hwms() {
        let incremental = TablePlan {
            table: "orders".to_string(),
            mode: VerifyMode::Incremental {
                cursor_col: "updated_at".to_string(),
                hwm: Some(Hwm {
                    updated_at: "2026-06-30 12:00:00".to_string(),
                    last_id: 42,
                }),
            },
        };
        assert_eq!(
            incremental.describe(),
            "mode=incremental cursor=updated_at hwm=updated_at=2026-06-30 12:00:00 last_id=42"
        );

        let two_stream = TablePlan {
            table: "users".to_string(),
            mode: VerifyMode::TwoStream {
                insert_cursor: "id".to_string(),
                update_cursor: "updated_at".to_string(),
                update_hwm: None,
                insert_hwm: Some(99),
            },
        };
        assert_eq!(
            two_stream.describe(),
            "mode=two_stream insert_cursor=id update_cursor=updated_at update_hwm=none insert_hwm=99"
        );
    }

    #[test]
    fn verify_command_new_defaults_table_plans_to_basic() {
        let source = MockSourceProbe::new();
        let delta = MockDeltaProbe::new();
        let cmd = VerifyCommand::new(source, delta, vec!["orders".to_string()]);
        assert_eq!(cmd.table_plans.len(), 1);
        let plan = cmd.table_plans.get("orders").unwrap();
        assert_eq!(plan.table, "orders");
        assert!(matches!(plan.mode, VerifyMode::Basic));
    }

    #[test]
    fn verify_command_matches_table_plans_by_name() {
        let source = MockSourceProbe::new();
        let delta = MockDeltaProbe::new();
        let cmd = VerifyCommand::new(
            source,
            delta,
            vec!["orders".to_string(), "users".to_string()],
        )
        .with_table_plans(vec![
            TablePlan {
                table: "users".to_string(),
                mode: VerifyMode::FullRefresh,
            },
            TablePlan::basic("orders"),
        ]);

        assert!(matches!(
            cmd.table_plans.get("users").unwrap().mode,
            VerifyMode::FullRefresh
        ));
        assert!(matches!(
            cmd.table_plans.get("orders").unwrap().mode,
            VerifyMode::Basic
        ));
    }

    #[tokio::test]
    async fn verify_reports_counts_for_each_table() {
        let mut source = MockSourceProbe::new();
        let mut delta = MockDeltaProbe::new();
        source.expect_integer_pk().returning(|_| Ok(None));
        source.expect_row_count().returning(|_| Ok(100));
        delta.expect_row_count().returning(|_| Ok(100));
        let cols = || {
            Ok(vec![
                ColumnMeta {
                    name: "id".to_string(),
                    type_str: "bigint".to_string(),
                    nullable: false,
                    numeric_scale: None,
                },
                ColumnMeta {
                    name: "name".to_string(),
                    type_str: "varchar".to_string(),
                    nullable: true,
                    numeric_scale: None,
                },
            ])
        };
        source.expect_columns().returning(move |_| cols());
        delta.expect_columns().returning(move |_| cols());
        source.expect_key_stats().returning(|_, _| {
            Ok(KeyStats {
                count: 0,
                distinct: 0,
                min: None,
                max: None,
                xor: 0,
                distinct_xor: 0,
                sum: 0,
            })
        });
        delta.expect_key_stats().returning(|_, _| {
            Ok(KeyStats {
                count: 0,
                distinct: 0,
                min: None,
                max: None,
                xor: 0,
                distinct_xor: 0,
                sum: 0,
            })
        });
        source
            .expect_non_null_counts()
            .returning(|_, cols: &[String]| Ok(vec![0i64; cols.len()]));
        delta
            .expect_non_null_counts()
            .returning(|_, cols: &[String]| Ok(vec![0i64; cols.len()]));
        source.expect_sample_ids().returning(|_, _, _| Ok(vec![]));
        source
            .expect_value_aggregates()
            .returning(|_, _| Ok(vec![]));
        delta
            .expect_value_aggregates()
            .returning(|_, _| Ok(vec![]));
        let cmd = VerifyCommand::new(
            source,
            delta,
            vec!["orders".to_string(), "users".to_string()],
        );
        let result = cmd.run().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), VerifyVerdict::Clean);
    }

    #[tokio::test]
    async fn verify_basic_mode_does_not_probe_freshness() {
        let mut source = MockSourceProbe::new();
        let mut delta = MockDeltaProbe::new();
        source.expect_integer_pk().returning(|_| Ok(None));
        source.expect_row_count().returning(|_| Ok(2));
        delta.expect_row_count().returning(|_| Ok(2));
        let cols = || {
            Ok(vec![ColumnMeta {
                name: "id".to_string(),
                type_str: "bigint".to_string(),
                nullable: false,
                numeric_scale: None,
            }])
        };
        source.expect_columns().returning(move |_| cols());
        delta.expect_columns().returning(move |_| cols());
        let stats = || {
            Ok(KeyStats {
                count: 2,
                distinct: 2,
                min: Some(1),
                max: Some(2),
                xor: 3,
                distinct_xor: 3,
                sum: 3,
            })
        };
        source.expect_key_stats().returning(move |_, _| stats());
        delta.expect_key_stats().returning(move |_, _| stats());
        source
            .expect_non_null_counts()
            .returning(|_, cols: &[String]| Ok(vec![0i64; cols.len()]));
        delta
            .expect_non_null_counts()
            .returning(|_, cols: &[String]| Ok(vec![0i64; cols.len()]));
        source.expect_sample_ids().returning(|_, _, _| Ok(vec![]));
        source
            .expect_value_aggregates()
            .returning(|_, _| Ok(vec![]));
        delta
            .expect_value_aggregates()
            .returning(|_, _| Ok(vec![]));
        let cmd = VerifyCommand::new(source, delta, vec!["orders".to_string()]);
        let result = cmd.run().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), VerifyVerdict::Clean);
    }

    #[tokio::test]
    async fn verify_reports_schema_diff() {
        let mut source = MockSourceProbe::new();
        let mut delta = MockDeltaProbe::new();
        source.expect_integer_pk().returning(|_| Ok(None));
        source.expect_row_count().returning(|_| Ok(0));
        delta.expect_row_count().returning(|_| Ok(0));
        source.expect_columns().returning(|_| {
            Ok(vec![
                ColumnMeta {
                    name: "id".to_string(),
                    type_str: "bigint".to_string(),
                    nullable: false,
                    numeric_scale: None,
                },
                ColumnMeta {
                    name: "name".to_string(),
                    type_str: "varchar".to_string(),
                    nullable: true,
                    numeric_scale: None,
                },
                ColumnMeta {
                    name: "phone".to_string(),
                    type_str: "varchar".to_string(),
                    nullable: true,
                    numeric_scale: None,
                },
            ])
        });
        delta.expect_columns().returning(|_| {
            Ok(vec![
                ColumnMeta {
                    name: "id".to_string(),
                    type_str: "Int64".to_string(),
                    nullable: false,
                    numeric_scale: None,
                },
                ColumnMeta {
                    name: "name".to_string(),
                    type_str: "Utf8".to_string(),
                    nullable: true,
                    numeric_scale: None,
                },
            ])
        });
        source.expect_key_stats().returning(|_, _| {
            Ok(KeyStats {
                count: 0,
                distinct: 0,
                min: None,
                max: None,
                xor: 0,
                distinct_xor: 0,
                sum: 0,
            })
        });
        delta.expect_key_stats().returning(|_, _| {
            Ok(KeyStats {
                count: 0,
                distinct: 0,
                min: None,
                max: None,
                xor: 0,
                distinct_xor: 0,
                sum: 0,
            })
        });
        let cmd = VerifyCommand::new(source, delta, vec!["users".to_string()]);
        let result = cmd.run().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), VerifyVerdict::Discrepancy);
    }

    #[tokio::test]
    async fn verify_reports_key_set() {
        let mut source = MockSourceProbe::new();
        let mut delta = MockDeltaProbe::new();
        source.expect_integer_pk().returning(|_| Ok(None));
        source.expect_row_count().returning(|_| Ok(3));
        delta.expect_row_count().returning(|_| Ok(3));
        let cols = || {
            Ok(vec![ColumnMeta {
                name: "id".to_string(),
                type_str: "bigint".to_string(),
                nullable: false,
                numeric_scale: None,
            }])
        };
        source.expect_columns().returning(move |_| cols());
        delta.expect_columns().returning(move |_| cols());
        let stats = || {
            Ok(KeyStats {
                count: 3,
                distinct: 3,
                min: Some(1),
                max: Some(3),
                xor: 1,
                distinct_xor: 1,
                sum: 6,
            })
        };
        source.expect_key_stats().returning(move |_, _| stats());
        delta.expect_key_stats().returning(move |_, _| stats());
        source
            .expect_non_null_counts()
            .returning(|_, cols: &[String]| Ok(vec![0i64; cols.len()]));
        delta
            .expect_non_null_counts()
            .returning(|_, cols: &[String]| Ok(vec![0i64; cols.len()]));
        source.expect_sample_ids().returning(|_, _, _| Ok(vec![]));
        source
            .expect_value_aggregates()
            .returning(|_, _| Ok(vec![]));
        delta
            .expect_value_aggregates()
            .returning(|_, _| Ok(vec![]));
        let cmd = VerifyCommand::new(source, delta, vec!["users".to_string()]);
        let result = cmd.run().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), VerifyVerdict::Clean);
    }

    #[tokio::test]
    async fn verify_verdict_pass() {
        let mut source = MockSourceProbe::new();
        let mut delta = MockDeltaProbe::new();
        source.expect_integer_pk().returning(|_| Ok(None));
        source.expect_row_count().returning(|_| Ok(5));
        delta.expect_row_count().returning(|_| Ok(5));
        let cols = || {
            Ok(vec![ColumnMeta {
                name: "id".to_string(),
                type_str: "bigint".to_string(),
                nullable: false,
                numeric_scale: None,
            }])
        };
        source.expect_columns().returning(move |_| cols());
        delta.expect_columns().returning(move |_| cols());
        let stats = || {
            Ok(KeyStats {
                count: 5,
                distinct: 5,
                min: Some(1),
                max: Some(5),
                xor: 7,
                distinct_xor: 7,
                sum: 15,
            })
        };
        source.expect_key_stats().returning(move |_, _| stats());
        delta.expect_key_stats().returning(move |_, _| stats());
        source
            .expect_non_null_counts()
            .returning(|_, cols: &[String]| Ok(vec![0i64; cols.len()]));
        delta
            .expect_non_null_counts()
            .returning(|_, cols: &[String]| Ok(vec![0i64; cols.len()]));
        source.expect_sample_ids().returning(|_, _, _| Ok(vec![]));
        source
            .expect_value_aggregates()
            .returning(|_, _| Ok(vec![]));
        delta
            .expect_value_aggregates()
            .returning(|_, _| Ok(vec![]));
        let cmd = VerifyCommand::new(source, delta, vec!["items".to_string()]);
        let result = cmd.run().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), VerifyVerdict::Clean);
    }

    #[tokio::test]
    async fn verify_verdict_drift_on_new_ids() {
        let mut source = MockSourceProbe::new();
        let mut delta = MockDeltaProbe::new();
        source.expect_integer_pk().returning(|_| Ok(None));
        source.expect_row_count().returning(|_| Ok(10));
        delta.expect_row_count().returning(|_| Ok(5));
        let cols = || {
            Ok(vec![ColumnMeta {
                name: "id".to_string(),
                type_str: "bigint".to_string(),
                nullable: false,
                numeric_scale: None,
            }])
        };
        source.expect_columns().returning(move |_| cols());
        delta.expect_columns().returning(move |_| cols());
        source.expect_key_stats().returning(|_, _| {
            Ok(KeyStats {
                count: 10,
                distinct: 10,
                min: Some(1),
                max: Some(10),
                xor: 11,
                distinct_xor: 11,
                sum: 11,
            })
        });
        delta.expect_key_stats().returning(|_, _| {
            Ok(KeyStats {
                count: 5,
                distinct: 5,
                min: Some(1),
                max: Some(5),
                xor: 15,
                distinct_xor: 15,
                sum: 15,
            })
        });
        let cmd = VerifyCommand::new(source, delta, vec!["events".to_string()]);
        let result = cmd.run().await;
        assert!(result.is_ok());
        // Drift is Clean (not a failure)
        assert_eq!(result.unwrap(), VerifyVerdict::Clean);
    }

    #[tokio::test]
    async fn verify_verdict_discrepancy_extra_delta_ids() {
        let mut source = MockSourceProbe::new();
        let mut delta = MockDeltaProbe::new();
        source.expect_integer_pk().returning(|_| Ok(None));
        source.expect_row_count().returning(|_| Ok(5));
        delta.expect_row_count().returning(|_| Ok(5));
        let cols = || {
            Ok(vec![ColumnMeta {
                name: "id".to_string(),
                type_str: "bigint".to_string(),
                nullable: false,
                numeric_scale: None,
            }])
        };
        source.expect_columns().returning(move |_| cols());
        delta.expect_columns().returning(move |_| cols());
        source.expect_key_stats().returning(|_, _| {
            Ok(KeyStats {
                count: 5,
                distinct: 5,
                min: Some(1),
                max: Some(5),
                xor: 7,
                distinct_xor: 7,
                sum: 7,
            })
        });
        delta.expect_key_stats().returning(|_, _| {
            Ok(KeyStats {
                count: 5,
                distinct: 5,
                min: Some(1),
                max: Some(10),
                xor: 15,
                distinct_xor: 15,
                sum: 15,
            })
        });
        let cmd = VerifyCommand::new(source, delta, vec!["orders".to_string()]);
        let result = cmd.run().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), VerifyVerdict::Discrepancy);
    }

    #[tokio::test]
    async fn verify_verdict_distinct_fallback() {
        let mut source = MockSourceProbe::new();
        let mut delta = MockDeltaProbe::new();
        source.expect_integer_pk().returning(|_| Ok(None));
        source.expect_row_count().returning(|_| Ok(3));
        delta.expect_row_count().returning(|_| Ok(5));
        let cols = || {
            Ok(vec![ColumnMeta {
                name: "id".to_string(),
                type_str: "bigint".to_string(),
                nullable: false,
                numeric_scale: None,
            }])
        };
        source.expect_columns().returning(move |_| cols());
        delta.expect_columns().returning(move |_| cols());
        source.expect_key_stats().returning(|_, _| {
            Ok(KeyStats {
                count: 3,
                distinct: 3,
                min: Some(1),
                max: Some(3),
                xor: 1,
                distinct_xor: 1,
                sum: 1,
            })
        });
        delta.expect_key_stats().returning(|_, _| {
            Ok(KeyStats {
                count: 5,
                distinct: 3,
                min: Some(1),
                max: Some(3),
                xor: 5,
                distinct_xor: 1,
                sum: 1,
            })
        });
        source
            .expect_non_null_counts()
            .returning(|_, cols: &[String]| Ok(vec![0i64; cols.len()]));
        delta
            .expect_non_null_counts()
            .returning(|_, cols: &[String]| Ok(vec![0i64; cols.len()]));
        source.expect_sample_ids().returning(|_, _, _| Ok(vec![]));
        source
            .expect_value_aggregates()
            .returning(|_, _| Ok(vec![]));
        delta
            .expect_value_aggregates()
            .returning(|_, _| Ok(vec![]));
        let cmd = VerifyCommand::new(source, delta, vec!["logs".to_string()]);
        let result = cmd.run().await;
        assert!(result.is_ok());
        // Distinct fallback matches -> Clean
        assert_eq!(result.unwrap(), VerifyVerdict::Clean);
    }

    /// V3-r Tier 1: a table with no single-column integer PRIMARY key (and no `id` column)
    /// must NOT silently short-circuit to Skipped anymore — the per-column value-aggregate
    /// checks (which need no key) still run, and a clean match ends PartiallyVerified (not
    /// Clean, not Skipped) since row-set completeness was never key-verified.
    #[tokio::test]
    async fn verify_key_less_table_partially_verified() {
        let mut source = MockSourceProbe::new();
        let mut delta = MockDeltaProbe::new();
        source.expect_integer_pk().returning(|_| Ok(None));
        // No single-column string PK either — stays a Tier 1 key-less table.
        source.expect_string_pk().returning(|_| Ok(None));
        source.expect_row_count().returning(|_| Ok(3));
        delta.expect_row_count().returning(|_| Ok(3));
        let cols = || {
            Ok(vec![
                ColumnMeta {
                    name: "amount".to_string(),
                    type_str: "int".to_string(),
                    nullable: true,
                    numeric_scale: None,
                },
                ColumnMeta {
                    name: "label".to_string(),
                    type_str: "varchar".to_string(),
                    nullable: true,
                    numeric_scale: None,
                },
            ])
        };
        source.expect_columns().returning(move |_| cols());
        delta.expect_columns().returning(move |_| cols());
        // No integer/`id` key -> key_stats must never be called (has_key is false).
        source.expect_key_stats().times(0).returning(|_, _| unreachable!());
        delta.expect_key_stats().times(0).returning(|_, _| unreachable!());
        let agg_values = || {
            Ok(vec![
                ColumnAggValues {
                    sum: Some("60".to_string()),
                    min: Some("10".to_string()),
                    max: Some("30".to_string()),
                    non_null_count: 3,
                },
                ColumnAggValues {
                    sum: Some("12".to_string()),
                    min: None,
                    max: None,
                    non_null_count: 3,
                },
            ])
        };
        source
            .expect_value_aggregates()
            .returning(move |_, _| agg_values());
        delta
            .expect_value_aggregates()
            .returning(move |_, _| agg_values());
        let cmd = VerifyCommand::new(source, delta, vec!["key_less".to_string()]);
        let result = cmd.run().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), VerifyVerdict::PartiallyVerified);
    }

    /// V3-r Tier 1: even without a key, a value-aggregate mismatch on a key-less table must
    /// still be caught as Discrepancy — the whole point of running value-aggregates for
    /// key-less tables is that they catch column-value corruption a key-set check can't.
    #[tokio::test]
    async fn verify_key_less_table_value_mismatch_is_discrepancy() {
        let mut source = MockSourceProbe::new();
        let mut delta = MockDeltaProbe::new();
        source.expect_integer_pk().returning(|_| Ok(None));
        // No single-column string PK either — stays a Tier 1 key-less table.
        source.expect_string_pk().returning(|_| Ok(None));
        source.expect_row_count().returning(|_| Ok(3));
        delta.expect_row_count().returning(|_| Ok(3));
        let cols = || {
            Ok(vec![
                ColumnMeta {
                    name: "amount".to_string(),
                    type_str: "int".to_string(),
                    nullable: true,
                    numeric_scale: None,
                },
                ColumnMeta {
                    name: "label".to_string(),
                    type_str: "varchar".to_string(),
                    nullable: true,
                    numeric_scale: None,
                },
            ])
        };
        source.expect_columns().returning(move |_| cols());
        delta.expect_columns().returning(move |_| cols());
        source.expect_key_stats().times(0).returning(|_, _| unreachable!());
        delta.expect_key_stats().times(0).returning(|_, _| unreachable!());
        source.expect_value_aggregates().returning(|_, _| {
            Ok(vec![
                ColumnAggValues {
                    sum: Some("60".to_string()),
                    min: Some("10".to_string()),
                    max: Some("30".to_string()),
                    non_null_count: 3,
                },
                ColumnAggValues {
                    sum: Some("12".to_string()),
                    min: None,
                    max: None,
                    non_null_count: 3,
                },
            ])
        });
        // Delta's `amount` column got corrupted: sum differs (60 -> 61).
        delta.expect_value_aggregates().returning(|_, _| {
            Ok(vec![
                ColumnAggValues {
                    sum: Some("61".to_string()),
                    min: Some("10".to_string()),
                    max: Some("30".to_string()),
                    non_null_count: 3,
                },
                ColumnAggValues {
                    sum: Some("12".to_string()),
                    min: None,
                    max: None,
                    non_null_count: 3,
                },
            ])
        });
        let cmd = VerifyCommand::new(source, delta, vec!["key_less".to_string()]);
        let result = cmd.run().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), VerifyVerdict::Discrepancy);
    }

    #[tokio::test]
    async fn verify_census_sample_skipped_on_appendlog() {
        let mut source = MockSourceProbe::new();
        let mut delta = MockDeltaProbe::new();
        source.expect_integer_pk().returning(|_| Ok(None));
        source.expect_row_count().returning(|_| Ok(3));
        delta.expect_row_count().returning(|_| Ok(5));
        let cols = || {
            Ok(vec![
                ColumnMeta {
                    name: "id".to_string(),
                    type_str: "bigint".to_string(),
                    nullable: false,
                    numeric_scale: None,
                },
                ColumnMeta {
                    name: "data".to_string(),
                    type_str: "varchar".to_string(),
                    nullable: true,
                    numeric_scale: None,
                },
            ])
        };
        source.expect_columns().returning(move |_| cols());
        delta.expect_columns().returning(move |_| cols());
        source.expect_key_stats().returning(|_, _| {
            Ok(KeyStats {
                count: 3,
                distinct: 3,
                min: Some(1),
                max: Some(3),
                xor: 1,
                distinct_xor: 1,
                sum: 1,
            })
        });
        delta.expect_key_stats().returning(|_, _| {
            Ok(KeyStats {
                count: 5,
                distinct: 3,
                min: Some(1),
                max: Some(3),
                xor: 5,
                distinct_xor: 1,
                sum: 1,
            })
        });
        // Do NOT set expect_non_null_counts or expect_sample_ids — they must not be called
        // when Delta is append-log. If they are called, mockall will panic.
        let cmd = VerifyCommand::new(source, delta, vec!["events".to_string()]);
        let result = cmd.run().await;
        assert!(result.is_ok());
        // Pass due to distinct-fallback, but census/sample are skipped (no panic on unexpected calls)
        assert_eq!(result.unwrap(), VerifyVerdict::Clean);
    }

    #[tokio::test]
    async fn verify_two_stream_exact_key_stats_pass() {
        let mut source = MockSourceProbe::new();
        let mut delta = MockDeltaProbe::new();
        source.expect_row_count().returning(|_| Ok(4));
        delta.expect_row_count().returning(|_| Ok(4));
        let cols = || {
            Ok(vec![ColumnMeta {
                name: "id".to_string(),
                type_str: "bigint".to_string(),
                nullable: false,
                numeric_scale: None,
            }])
        };
        source.expect_columns().returning(move |_| cols());
        delta.expect_columns().returning(move |_| cols());
        source.expect_max_cursor().returning(|table, cursor_col| {
            assert_eq!(table, "users");
            assert_eq!(cursor_col, "updated_at");
            Ok(Some("2026-06-30 12:00:00".to_string()))
        });
        delta.expect_max_cursor().returning(|table, cursor_col| {
            assert_eq!(table, "users");
            assert_eq!(cursor_col, "updated_at");
            Ok(Some("2026-06-30T12:00:00.000000".to_string()))
        });
        let stats = || {
            Ok(KeyStats {
                count: 4,
                distinct: 4,
                min: Some(10),
                max: Some(13),
                xor: 3,
                distinct_xor: 3,
                sum: 3,
            })
        };
        source.expect_key_stats().returning(move |_, _| stats());
        delta.expect_key_stats().returning(move |_, _| stats());
        source
            .expect_non_null_counts()
            .returning(|_, cols: &[String]| Ok(vec![0i64; cols.len()]));
        delta
            .expect_non_null_counts()
            .returning(|_, cols: &[String]| Ok(vec![0i64; cols.len()]));
        source.expect_sample_ids().returning(|_, _, _| Ok(vec![]));
        source
            .expect_value_aggregates()
            .returning(|_, _| Ok(vec![]));
        delta
            .expect_value_aggregates()
            .returning(|_, _| Ok(vec![]));
        let cmd =
            VerifyCommand::new(source, delta, vec!["users".to_string()]).with_table_plans(vec![
                TablePlan {
                    table: "users".to_string(),
                    mode: VerifyMode::TwoStream {
                        insert_cursor: "id".to_string(),
                        update_cursor: "updated_at".to_string(),
                        update_hwm: None,
                        insert_hwm: Some(13),
                    },
                },
            ]);
        let result = cmd.run().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), VerifyVerdict::Clean);
    }

    #[tokio::test]
    async fn verify_two_stream_delta_superset_is_non_failing_drift() {
        let mut source = MockSourceProbe::new();
        let mut delta = MockDeltaProbe::new();
        source.expect_row_count().returning(|_| Ok(3));
        delta.expect_row_count().returning(|_| Ok(5));
        let cols = || {
            Ok(vec![ColumnMeta {
                name: "id".to_string(),
                type_str: "bigint".to_string(),
                nullable: false,
                numeric_scale: None,
            }])
        };
        source.expect_columns().returning(move |_| cols());
        delta.expect_columns().returning(move |_| cols());
        source
            .expect_max_cursor()
            .returning(|_, _| Ok(Some("2026-06-30 12:00:00".to_string())));
        delta
            .expect_max_cursor()
            .returning(|_, _| Ok(Some("2026-06-30T12:00:00.000000".to_string())));
        source.expect_key_stats().returning(|_, _| {
            Ok(KeyStats {
                count: 3,
                distinct: 3,
                min: Some(2),
                max: Some(4),
                xor: 4,
                distinct_xor: 4,
                sum: 4,
            })
        });
        delta.expect_key_stats().returning(|_, _| {
            Ok(KeyStats {
                count: 5,
                distinct: 5,
                min: Some(1),
                max: Some(6),
                xor: 1,
                distinct_xor: 1,
                sum: 1,
            })
        });
        let cmd =
            VerifyCommand::new(source, delta, vec!["users".to_string()]).with_table_plans(vec![
                TablePlan {
                    table: "users".to_string(),
                    mode: VerifyMode::TwoStream {
                        insert_cursor: "id".to_string(),
                        update_cursor: "updated_at".to_string(),
                        update_hwm: None,
                        insert_hwm: Some(6),
                    },
                },
            ]);
        let result = cmd.run().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), VerifyVerdict::Clean);
    }

    #[tokio::test]
    async fn verify_two_stream_equal_size_key_mismatch_is_discrepancy() {
        let mut source = MockSourceProbe::new();
        let mut delta = MockDeltaProbe::new();
        source.expect_row_count().returning(|_| Ok(3));
        delta.expect_row_count().returning(|_| Ok(3));
        let cols = || {
            Ok(vec![ColumnMeta {
                name: "id".to_string(),
                type_str: "bigint".to_string(),
                nullable: false,
                numeric_scale: None,
            }])
        };
        source.expect_columns().returning(move |_| cols());
        delta.expect_columns().returning(move |_| cols());
        source
            .expect_max_cursor()
            .returning(|_, _| Ok(Some("2026-06-30 12:00:00".to_string())));
        delta
            .expect_max_cursor()
            .returning(|_, _| Ok(Some("2026-06-30T12:00:00.000000".to_string())));
        source.expect_key_stats().returning(|_, _| {
            Ok(KeyStats {
                count: 3,
                distinct: 3,
                min: Some(2),
                max: Some(4),
                xor: 5,
                distinct_xor: 5,
                sum: 5,
            })
        });
        delta.expect_key_stats().returning(|_, _| {
            Ok(KeyStats {
                count: 3,
                distinct: 3,
                min: Some(1),
                max: Some(5),
                xor: 7,
                distinct_xor: 7,
                sum: 7,
            })
        });
        let cmd =
            VerifyCommand::new(source, delta, vec!["users".to_string()]).with_table_plans(vec![
                TablePlan {
                    table: "users".to_string(),
                    mode: VerifyMode::TwoStream {
                        insert_cursor: "id".to_string(),
                        update_cursor: "updated_at".to_string(),
                        update_hwm: None,
                        insert_hwm: Some(5),
                    },
                },
            ]);
        let result = cmd.run().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), VerifyVerdict::Discrepancy);
    }

    #[tokio::test]
    async fn verify_two_stream_source_missing_from_delta_is_discrepancy() {
        let mut source = MockSourceProbe::new();
        let mut delta = MockDeltaProbe::new();
        source.expect_row_count().returning(|_| Ok(5));
        delta.expect_row_count().returning(|_| Ok(4));
        let cols = || {
            Ok(vec![ColumnMeta {
                name: "id".to_string(),
                type_str: "bigint".to_string(),
                nullable: false,
                numeric_scale: None,
            }])
        };
        source.expect_columns().returning(move |_| cols());
        delta.expect_columns().returning(move |_| cols());
        source
            .expect_max_cursor()
            .returning(|_, _| Ok(Some("2026-06-30 12:00:00".to_string())));
        delta
            .expect_max_cursor()
            .returning(|_, _| Ok(Some("2026-06-30T12:00:00.000000".to_string())));
        source.expect_key_stats().returning(|_, _| {
            Ok(KeyStats {
                count: 5,
                distinct: 5,
                min: Some(1),
                max: Some(7),
                xor: 2,
                distinct_xor: 2,
                sum: 2,
            })
        });
        delta.expect_key_stats().returning(|_, _| {
            Ok(KeyStats {
                count: 4,
                distinct: 4,
                min: Some(1),
                max: Some(6),
                xor: 1,
                distinct_xor: 1,
                sum: 1,
            })
        });
        let cmd =
            VerifyCommand::new(source, delta, vec!["users".to_string()]).with_table_plans(vec![
                TablePlan {
                    table: "users".to_string(),
                    mode: VerifyMode::TwoStream {
                        insert_cursor: "id".to_string(),
                        update_cursor: "updated_at".to_string(),
                        update_hwm: None,
                        insert_hwm: Some(6),
                    },
                },
            ]);
        let result = cmd.run().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), VerifyVerdict::Discrepancy);
    }

    #[tokio::test]
    async fn verify_verdict_size_skip() {
        let mut source = MockSourceProbe::new();
        let mut delta = MockDeltaProbe::new();
        source.expect_integer_pk().returning(|_| Ok(None));
        source.expect_row_count().returning(|_| Ok(5_000_000));
        delta.expect_row_count().returning(|_| Ok(4_000_000));
        let cols = || {
            Ok(vec![ColumnMeta {
                name: "id".to_string(),
                type_str: "bigint".to_string(),
                nullable: false,
                numeric_scale: None,
            }])
        };
        source.expect_columns().returning(move |_| cols());
        delta.expect_columns().returning(move |_| cols());
        let cmd = VerifyCommand::new(source, delta, vec!["big_table".to_string()]).with_deep(false);
        let result = cmd.run().await;
        assert!(result.is_ok());
        // Skipped is Clean (not a failure)
        assert_eq!(result.unwrap(), VerifyVerdict::Clean);
    }

    #[tokio::test]
    async fn verify_verdict_missing_column_fails() {
        let mut source = MockSourceProbe::new();
        let mut delta = MockDeltaProbe::new();
        source.expect_integer_pk().returning(|_| Ok(None));
        source.expect_row_count().returning(|_| Ok(100));
        delta.expect_row_count().returning(|_| Ok(100));
        source.expect_columns().returning(|_| {
            Ok(vec![
                ColumnMeta {
                    name: "id".to_string(),
                    type_str: "bigint".to_string(),
                    nullable: false,
                    numeric_scale: None,
                },
                ColumnMeta {
                    name: "created_at".to_string(),
                    type_str: "timestamp".to_string(),
                    nullable: false,
                    numeric_scale: None,
                },
            ])
        });
        delta.expect_columns().returning(|_| {
            Ok(vec![ColumnMeta {
                name: "id".to_string(),
                type_str: "Int64".to_string(),
                nullable: false,
                numeric_scale: None,
            }])
        });
        let cmd = VerifyCommand::new(source, delta, vec!["events".to_string()]);
        let result = cmd.run().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), VerifyVerdict::Discrepancy);
    }

    #[tokio::test]
    async fn verify_census_match() {
        let mut source = MockSourceProbe::new();
        let mut delta = MockDeltaProbe::new();
        source.expect_integer_pk().returning(|_| Ok(None));
        source.expect_row_count().returning(|_| Ok(10));
        delta.expect_row_count().returning(|_| Ok(10));
        let cols = || {
            Ok(vec![
                ColumnMeta {
                    name: "id".to_string(),
                    type_str: "bigint".to_string(),
                    nullable: false,
                    numeric_scale: None,
                },
                ColumnMeta {
                    name: "name".to_string(),
                    type_str: "varchar".to_string(),
                    nullable: true,
                    numeric_scale: None,
                },
            ])
        };
        source.expect_columns().returning(move |_| cols());
        delta.expect_columns().returning(move |_| cols());
        let stats = || {
            Ok(KeyStats {
                count: 10,
                distinct: 10,
                min: Some(1),
                max: Some(10),
                xor: 15,
                distinct_xor: 15,
                sum: 15,
            })
        };
        source.expect_key_stats().returning(move |_, _| stats());
        delta.expect_key_stats().returning(move |_, _| stats());
        source
            .expect_non_null_counts()
            .returning(|_, _| Ok(vec![10, 8]));
        delta
            .expect_non_null_counts()
            .returning(|_, _| Ok(vec![10, 8]));
        source.expect_sample_ids().returning(|_, _, _| Ok(vec![]));
        source
            .expect_value_aggregates()
            .returning(|_, _| Ok(vec![ColumnAggValues { sum: Some("45".to_string()), min: Some("1".to_string()), max: Some("10".to_string()), non_null_count: 3 }]));
        delta
            .expect_value_aggregates()
            .returning(|_, _| Ok(vec![ColumnAggValues { sum: Some("45".to_string()), min: Some("1".to_string()), max: Some("10".to_string()), non_null_count: 3 }]));
        let cmd = VerifyCommand::new(source, delta, vec!["users".to_string()]);
        let result = cmd.run().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), VerifyVerdict::Clean);
    }

    #[tokio::test]
    async fn verify_census_differs_is_non_failing() {
        let mut source = MockSourceProbe::new();
        let mut delta = MockDeltaProbe::new();
        source.expect_integer_pk().returning(|_| Ok(None));
        source.expect_row_count().returning(|_| Ok(10));
        delta.expect_row_count().returning(|_| Ok(10));
        let cols = || {
            Ok(vec![
                ColumnMeta {
                    name: "id".to_string(),
                    type_str: "bigint".to_string(),
                    nullable: false,
                    numeric_scale: None,
                },
                ColumnMeta {
                    name: "name".to_string(),
                    type_str: "varchar".to_string(),
                    nullable: true,
                    numeric_scale: None,
                },
            ])
        };
        source.expect_columns().returning(move |_| cols());
        delta.expect_columns().returning(move |_| cols());
        let stats = || {
            Ok(KeyStats {
                count: 10,
                distinct: 10,
                min: Some(1),
                max: Some(10),
                xor: 15,
                distinct_xor: 15,
                sum: 15,
            })
        };
        source.expect_key_stats().returning(move |_, _| stats());
        delta.expect_key_stats().returning(move |_, _| stats());
        source
            .expect_non_null_counts()
            .returning(|_, _| Ok(vec![10, 8]));
        delta
            .expect_non_null_counts()
            .returning(|_, _| Ok(vec![10, 5]));
        source.expect_sample_ids().returning(|_, _, _| Ok(vec![]));
        source
            .expect_value_aggregates()
            .returning(|_, _| Ok(vec![ColumnAggValues { sum: Some("45".to_string()), min: None, max: None, non_null_count: 8 }]));
        delta
            .expect_value_aggregates()
            .returning(|_, _| Ok(vec![ColumnAggValues { sum: Some("45".to_string()), min: None, max: None, non_null_count: 8 }]));
        let cmd = VerifyCommand::new(source, delta, vec!["users".to_string()]);
        let result = cmd.run().await;
        assert!(result.is_ok());
        // Census diff must NOT cause a failure, still Clean
        assert_eq!(result.unwrap(), VerifyVerdict::Clean);
    }

    #[tokio::test]
    async fn verify_sample_match() {
        let mut source = MockSourceProbe::new();
        let mut delta = MockDeltaProbe::new();
        source.expect_integer_pk().returning(|_| Ok(None));
        source.expect_row_count().returning(|_| Ok(10));
        delta.expect_row_count().returning(|_| Ok(10));
        let cols = || {
            Ok(vec![
                ColumnMeta {
                    name: "id".to_string(),
                    type_str: "bigint".to_string(),
                    nullable: false,
                    numeric_scale: None,
                },
                ColumnMeta {
                    name: "name".to_string(),
                    type_str: "varchar".to_string(),
                    nullable: true,
                    numeric_scale: None,
                },
            ])
        };
        source.expect_columns().returning(move |_| cols());
        delta.expect_columns().returning(move |_| cols());
        let stats = || {
            Ok(KeyStats {
                count: 10,
                distinct: 10,
                min: Some(1),
                max: Some(10),
                xor: 15,
                distinct_xor: 15,
                sum: 15,
            })
        };
        source.expect_key_stats().returning(move |_, _| stats());
        delta.expect_key_stats().returning(move |_, _| stats());
        source
            .expect_non_null_counts()
            .returning(|_, _| Ok(vec![10, 8]));
        delta
            .expect_non_null_counts()
            .returning(|_, _| Ok(vec![10, 8]));
        source
            .expect_sample_ids()
            .returning(|_, _, _| Ok(vec![1, 2]));
        let mut sample_rows_map = std::collections::HashMap::new();
        sample_rows_map.insert(1i64, vec![Some("a".to_string())]);
        sample_rows_map.insert(2i64, vec![Some("b".to_string())]);
        let sample_rows_map_src = sample_rows_map.clone();
        let sample_rows_map_delta = sample_rows_map.clone();
        source
            .expect_sample_rows()
            .returning(move |_, _, _, _| Ok(sample_rows_map_src.clone()));
        delta
            .expect_sample_rows()
            .returning(move |_, _, _, _| Ok(sample_rows_map_delta.clone()));
        source
            .expect_value_aggregates()
            .returning(|_, _| Ok(vec![ColumnAggValues { sum: Some("2".to_string()), min: None, max: None, non_null_count: 2 }]));
        delta
            .expect_value_aggregates()
            .returning(|_, _| Ok(vec![ColumnAggValues { sum: Some("2".to_string()), min: None, max: None, non_null_count: 2 }]));
        let cmd = VerifyCommand::new(source, delta, vec!["users".to_string()]);
        let result = cmd.run().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), VerifyVerdict::Clean);
    }

    #[tokio::test]
    async fn verify_sample_differs_non_failing() {
        let mut source = MockSourceProbe::new();
        let mut delta = MockDeltaProbe::new();
        source.expect_integer_pk().returning(|_| Ok(None));
        source.expect_row_count().returning(|_| Ok(10));
        delta.expect_row_count().returning(|_| Ok(10));
        let cols = || {
            Ok(vec![
                ColumnMeta {
                    name: "id".to_string(),
                    type_str: "bigint".to_string(),
                    nullable: false,
                    numeric_scale: None,
                },
                ColumnMeta {
                    name: "name".to_string(),
                    type_str: "varchar".to_string(),
                    nullable: true,
                    numeric_scale: None,
                },
            ])
        };
        source.expect_columns().returning(move |_| cols());
        delta.expect_columns().returning(move |_| cols());
        let stats = || {
            Ok(KeyStats {
                count: 10,
                distinct: 10,
                min: Some(1),
                max: Some(10),
                xor: 15,
                distinct_xor: 15,
                sum: 15,
            })
        };
        source.expect_key_stats().returning(move |_, _| stats());
        delta.expect_key_stats().returning(move |_, _| stats());
        source
            .expect_non_null_counts()
            .returning(|_, _| Ok(vec![10, 8]));
        delta
            .expect_non_null_counts()
            .returning(|_, _| Ok(vec![10, 8]));
        source
            .expect_sample_ids()
            .returning(|_, _, _| Ok(vec![1, 2]));
        let mut source_rows_map = std::collections::HashMap::new();
        source_rows_map.insert(1i64, vec![Some("a".to_string())]);
        source_rows_map.insert(2i64, vec![Some("b".to_string())]);
        let mut delta_rows_map = std::collections::HashMap::new();
        delta_rows_map.insert(1i64, vec![Some("a".to_string())]);
        delta_rows_map.insert(2i64, vec![Some("X".to_string())]);
        let source_rows_for_closure = source_rows_map.clone();
        let delta_rows_for_closure = delta_rows_map.clone();
        source
            .expect_sample_rows()
            .returning(move |_, _, _, _| Ok(source_rows_for_closure.clone()));
        delta
            .expect_sample_rows()
            .returning(move |_, _, _, _| Ok(delta_rows_for_closure.clone()));
        source
            .expect_value_aggregates()
            .returning(|_, _| Ok(vec![ColumnAggValues { sum: Some("2".to_string()), min: None, max: None, non_null_count: 2 }]));
        delta
            .expect_value_aggregates()
            .returning(|_, _| Ok(vec![ColumnAggValues { sum: Some("2".to_string()), min: None, max: None, non_null_count: 2 }]));
        let cmd = VerifyCommand::new(source, delta, vec!["users".to_string()]);
        let result = cmd.run().await;
        assert!(result.is_ok());
        // Sample diff must NOT cause a failure, still Clean
        assert_eq!(result.unwrap(), VerifyVerdict::Clean);
    }

    #[tokio::test]
    async fn verify_incremental_hwm_scoped_latest_pass() {
        let mut source = MockSourceProbe::new();
        let mut delta = MockDeltaProbe::new();
        source.expect_integer_pk().returning(|_| Ok(None));
        delta.expect_row_count().returning(|_| Ok(2));
        let cols = || {
            Ok(vec![
                ColumnMeta {
                    name: "id".to_string(),
                    type_str: "bigint".to_string(),
                    nullable: false,
                    numeric_scale: None,
                },
                ColumnMeta {
                    name: "updated_at".to_string(),
                    type_str: "timestamp".to_string(),
                    nullable: false,
                    numeric_scale: None,
                },
            ])
        };
        source.expect_columns().returning(move |_| cols());
        delta.expect_columns().returning(move |_| cols());
        source.expect_max_cursor().returning(|table, cursor_col| {
            assert_eq!(table, "orders");
            assert_eq!(cursor_col, "updated_at");
            Ok(Some("2026-06-30 12:00:00".to_string()))
        });
        delta.expect_max_cursor().returning(|table, cursor_col| {
            assert_eq!(table, "orders");
            assert_eq!(cursor_col, "updated_at");
            Ok(Some("2026-06-30T12:00:00.000000".to_string()))
        });
        source.expect_row_count_scoped().returning(|_, scope| {
            assert_eq!(scope.cursor_col, "updated_at");
            assert_eq!(scope.updated_at, "2026-06-30 12:00:00");
            assert_eq!(scope.last_id, 42);
            Ok(2)
        });
        source
            .expect_key_stats_scoped()
            .returning(|_, key_col, scope| {
                assert_eq!(key_col, "id");
                assert_eq!(scope.cursor_col, "updated_at");
                Ok(KeyStats {
                    count: 2,
                    distinct: 2,
                    min: Some(43),
                    max: Some(44),
                    xor: 7,
                    distinct_xor: 7,
                    sum: 7,
                })
            });
        delta
            .expect_latest_key_stats()
            .returning(|_, key_col, cursor_col| {
                assert_eq!(key_col, "id");
                assert_eq!(cursor_col, "updated_at");
                Ok(KeyStats {
                    count: 2,
                    distinct: 2,
                    min: Some(43),
                    max: Some(44),
                    xor: 7,
                    distinct_xor: 7,
                    sum: 7,
                })
            });
        source
            .expect_value_aggregates_scoped()
            .returning(|_, _, _| Ok(vec![]));
        delta
            .expect_value_aggregates_latest()
            .returning(|_, _, _, _| Ok(vec![]));
        let cmd =
            VerifyCommand::new(source, delta, vec!["orders".to_string()]).with_table_plans(vec![
                TablePlan {
                    table: "orders".to_string(),
                    mode: VerifyMode::Incremental {
                        cursor_col: "updated_at".to_string(),
                        hwm: Some(Hwm {
                            updated_at: "2026-06-30 12:00:00".to_string(),
                            last_id: 42,
                        }),
                    },
                },
            ]);
        let result = cmd.run().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), VerifyVerdict::Clean);
    }

    #[tokio::test]
    async fn verify_incremental_hwm_scoped_latest_discrepancy() {
        let mut source = MockSourceProbe::new();
        let mut delta = MockDeltaProbe::new();
        source.expect_integer_pk().returning(|_| Ok(None));
        delta.expect_row_count().returning(|_| Ok(2));
        let cols = || {
            Ok(vec![
                ColumnMeta {
                    name: "id".to_string(),
                    type_str: "bigint".to_string(),
                    nullable: false,
                    numeric_scale: None,
                },
                ColumnMeta {
                    name: "updated_at".to_string(),
                    type_str: "timestamp".to_string(),
                    nullable: false,
                    numeric_scale: None,
                },
            ])
        };
        source.expect_columns().returning(move |_| cols());
        delta.expect_columns().returning(move |_| cols());
        source
            .expect_max_cursor()
            .returning(|_, _| Ok(Some("2026-06-30 12:00:00".to_string())));
        delta
            .expect_max_cursor()
            .returning(|_, _| Ok(Some("2026-06-30T12:00:00.000000".to_string())));
        source.expect_row_count_scoped().returning(|_, _| Ok(2));
        source.expect_key_stats_scoped().returning(|_, _, _| {
            Ok(KeyStats {
                count: 2,
                distinct: 2,
                min: Some(43),
                max: Some(44),
                xor: 7,
                distinct_xor: 7,
                sum: 7,
            })
        });
        delta.expect_latest_key_stats().returning(|_, _, _| {
            Ok(KeyStats {
                count: 2,
                distinct: 2,
                min: Some(43),
                max: Some(99),
                xor: 12,
                distinct_xor: 12,
                sum: 12,
            })
        });
        source
            .expect_value_aggregates_scoped()
            .returning(|_, _, _| Ok(vec![]));
        delta
            .expect_value_aggregates_latest()
            .returning(|_, _, _, _| Ok(vec![]));
        let cmd =
            VerifyCommand::new(source, delta, vec!["orders".to_string()]).with_table_plans(vec![
                TablePlan {
                    table: "orders".to_string(),
                    mode: VerifyMode::Incremental {
                        cursor_col: "updated_at".to_string(),
                        hwm: Some(Hwm {
                            updated_at: "2026-06-30 12:00:00".to_string(),
                            last_id: 42,
                        }),
                    },
                },
            ]);
        let result = cmd.run().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), VerifyVerdict::Discrepancy);
    }

    #[tokio::test]
    async fn verify_incremental_hwm_without_id_skips_key_set() {
        // No usable key -> incremental_scope stays None and the key-set/row-set check is
        // skipped, but (V3-r Tier 1) the value-aggregate check on `updated_at` still runs and
        // still passes -> PartiallyVerified, not silently Clean.
        let mut source = MockSourceProbe::new();
        let mut delta = MockDeltaProbe::new();
        source.expect_integer_pk().returning(|_| Ok(None));
        // No single-column string PK either — stays a Tier 1 key-less table.
        source.expect_string_pk().returning(|_| Ok(None));
        let cols = || {
            Ok(vec![ColumnMeta {
                name: "updated_at".to_string(),
                type_str: "timestamp".to_string(),
                nullable: false,
                numeric_scale: None,
            }])
        };
        source.expect_columns().returning(move |_| cols());
        delta.expect_columns().returning(move |_| cols());
        source
            .expect_max_cursor()
            .returning(|_, _| Ok(Some("2026-06-30 12:00:00".to_string())));
        delta
            .expect_max_cursor()
            .returning(|_, _| Ok(Some("2026-06-30T12:00:00.000000".to_string())));
        source.expect_row_count().returning(|_| Ok(2));
        delta.expect_row_count().returning(|_| Ok(2));
        let agg_values = || {
            Ok(vec![ColumnAggValues {
                sum: None,
                min: Some("2026-06-30 12:00:00".to_string()),
                max: Some("2026-06-30 12:00:00".to_string()),
                non_null_count: 2,
            }])
        };
        source
            .expect_value_aggregates()
            .returning(move |_, _| agg_values());
        delta
            .expect_value_aggregates()
            .returning(move |_, _| agg_values());
        let cmd =
            VerifyCommand::new(source, delta, vec!["orders".to_string()]).with_table_plans(vec![
                TablePlan {
                    table: "orders".to_string(),
                    mode: VerifyMode::Incremental {
                        cursor_col: "updated_at".to_string(),
                        hwm: Some(Hwm {
                            updated_at: "2026-06-30 12:00:00".to_string(),
                            last_id: 42,
                        }),
                    },
                },
            ]);
        let result = cmd.run().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), VerifyVerdict::PartiallyVerified);
    }

    #[tokio::test]
    async fn verify_incremental_without_hwm_uses_raw_path() {
        let mut source = MockSourceProbe::new();
        let mut delta = MockDeltaProbe::new();
        source.expect_integer_pk().returning(|_| Ok(None));
        source.expect_row_count().returning(|_| Ok(5));
        delta.expect_row_count().returning(|_| Ok(5));
        let cols = || {
            Ok(vec![
                ColumnMeta {
                    name: "id".to_string(),
                    type_str: "bigint".to_string(),
                    nullable: false,
                    numeric_scale: None,
                },
                ColumnMeta {
                    name: "updated_at".to_string(),
                    type_str: "timestamp".to_string(),
                    nullable: false,
                    numeric_scale: None,
                },
            ])
        };
        source.expect_columns().returning(move |_| cols());
        delta.expect_columns().returning(move |_| cols());
        source
            .expect_max_cursor()
            .returning(|_, _| Ok(Some("2026-06-30 12:00:00".to_string())));
        delta
            .expect_max_cursor()
            .returning(|_, _| Ok(Some("2026-06-30T12:00:00.000000".to_string())));
        source.expect_key_stats().returning(|_, _| {
            Ok(KeyStats {
                count: 5,
                distinct: 5,
                min: Some(1),
                max: Some(5),
                xor: 7,
                distinct_xor: 7,
                sum: 7,
            })
        });
        delta.expect_key_stats().returning(|_, _| {
            Ok(KeyStats {
                count: 5,
                distinct: 5,
                min: Some(1),
                max: Some(5),
                xor: 7,
                distinct_xor: 7,
                sum: 7,
            })
        });
        source
            .expect_non_null_counts()
            .returning(|_, cols: &[String]| Ok(vec![0i64; cols.len()]));
        delta
            .expect_non_null_counts()
            .returning(|_, cols: &[String]| Ok(vec![0i64; cols.len()]));
        source.expect_sample_ids().returning(|_, _, _| Ok(vec![]));
        source
            .expect_value_aggregates()
            .returning(|_, _| Ok(vec![]));
        delta
            .expect_value_aggregates()
            .returning(|_, _| Ok(vec![]));
        let cmd =
            VerifyCommand::new(source, delta, vec!["orders".to_string()]).with_table_plans(vec![
                TablePlan {
                    table: "orders".to_string(),
                    mode: VerifyMode::Incremental {
                        cursor_col: "updated_at".to_string(),
                        hwm: None,
                    },
                },
            ]);
        let result = cmd.run().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), VerifyVerdict::Clean);
    }

    #[test]
    fn key_stats_outcome_sum_breaks_xor_collision() {
        // Test that sum breaks XOR collisions. Two key sets {1,3,5,8} and {1,2,4,8}
        // both have count=4, distinct=4, min=1, max=8, xor=15, distinct_xor=15,
        // but different sums (17 vs 15). Outcome must NOT be Pass.
        let s = KeyStats {
            count: 4,
            distinct: 4,
            min: Some(1),
            max: Some(8),
            xor: 15,
            distinct_xor: 15,
            sum: 17,
        };
        let d = KeyStats {
            count: 4,
            distinct: 4,
            min: Some(1),
            max: Some(8),
            xor: 15,
            distinct_xor: 15,
            sum: 15,
        };
        let outcome = VerifyCommand::<MockSourceProbe, MockDeltaProbe>::key_stats_outcome("source", "delta", &s, &d);
        assert!(!matches!(outcome, TableOutcome::Pass), "distinct-sum must break the xor collision");
    }

    #[test]
    fn key_stats_outcome_handles_u64_range_min_max() {
        // V8: min/max above i64::MAX (2^63-1) must be handled correctly by the range/Drift
        // logic now that KeyStats.min/max are i128 — this proves the wider type doesn't just
        // compile but actually participates in the comparisons unchanged.
        let u64_max: i128 = 18_446_744_073_709_551_615; // 2^64 - 1
        let above_i63: i128 = 9_300_000_000_000_000_000; // > i64::MAX, < u64::MAX

        // Exact match on both sides (including a u64-range max) -> Pass.
        let s = KeyStats {
            count: 2,
            distinct: 2,
            min: Some(above_i63),
            max: Some(u64_max),
            xor: 5,
            distinct_xor: 5,
            sum: above_i63 + u64_max,
        };
        let d = s.clone();
        let outcome = VerifyCommand::<MockSourceProbe, MockDeltaProbe>::key_stats_outcome(
            "source", "delta", &s, &d,
        );
        assert_eq!(outcome, TableOutcome::Pass);

        // Delta lagging behind (smaller distinct, range still inside source's u64-range
        // bounds) -> Drift, not Discrepancy.
        let d_lagging = KeyStats {
            count: 1,
            distinct: 1,
            min: Some(above_i63),
            max: Some(above_i63),
            xor: above_i63 as i64,
            distinct_xor: above_i63 as i64,
            sum: above_i63,
        };
        let outcome = VerifyCommand::<MockSourceProbe, MockDeltaProbe>::key_stats_outcome(
            "source", "delta", &s, &d_lagging,
        );
        assert!(
            matches!(outcome, TableOutcome::Drift { .. }),
            "expected Drift, got {outcome:?}"
        );

        // Delta claims a max beyond the source's u64-range max -> Discrepancy (delta has
        // ids/rows not in source).
        let d_extra = KeyStats {
            count: 2,
            distinct: 2,
            min: Some(above_i63),
            max: Some(u64_max + 1),
            xor: 5,
            distinct_xor: 5,
            sum: above_i63 + u64_max + 1,
        };
        let outcome = VerifyCommand::<MockSourceProbe, MockDeltaProbe>::key_stats_outcome(
            "source", "delta", &s, &d_extra,
        );
        assert!(
            matches!(outcome, TableOutcome::Discrepancy { .. }),
            "expected Discrepancy, got {outcome:?}"
        );
    }

    // --- PS-M1: incremental_scoped_key_stats_outcome -------------------------------------

    #[test]
    fn incremental_scoped_outcome_delta_superset_contained_range_is_drift() {
        // The exact false-alarm reproduced live on `multi_course_tokens`: delta_latest has
        // MORE distinct keys than source_scoped (17 "extra" rows), but its min/max range is
        // identical to source_scoped's. These are rows that were updated in the source AFTER
        // the sync — their `updated_at` advanced past the stored HWM, dropping them out of
        // `source_scoped`, while their already-synced version legitimately remains in
        // `delta_latest`. This is benign steady-state drift, NOT a sync error, so it must NOT
        // trip Discrepancy (the previous, buggy behavior via `key_stats_outcome`).
        let source_scoped = KeyStats {
            count: 1_623_980,
            distinct: 1_623_980,
            min: Some(1),
            max: Some(6_765_879),
            xor: 0,
            distinct_xor: 0,
            sum: 0,
        };
        let delta_latest = KeyStats {
            count: 1_623_997,
            distinct: 1_623_997,
            min: Some(1),
            max: Some(6_765_879),
            xor: 0,
            distinct_xor: 0,
            sum: 0,
        };
        let outcome = VerifyCommand::<MockSourceProbe, MockDeltaProbe>::incremental_scoped_key_stats_outcome(
            "source_scoped",
            "delta_latest",
            &source_scoped,
            &delta_latest,
        );
        assert!(
            matches!(outcome, TableOutcome::Drift { .. }),
            "expected Drift for a delta_latest superset with a contained key range, got {outcome:?}"
        );
    }

    #[test]
    fn incremental_scoped_outcome_equal_stats_is_pass() {
        let s = KeyStats {
            count: 10,
            distinct: 10,
            min: Some(1),
            max: Some(10),
            xor: 11,
            distinct_xor: 11,
            sum: 55,
        };
        let d = s.clone();
        let outcome = VerifyCommand::<MockSourceProbe, MockDeltaProbe>::incremental_scoped_key_stats_outcome(
            "source_scoped",
            "delta_latest",
            &s,
            &d,
        );
        assert_eq!(outcome, TableOutcome::Pass);
    }

    #[test]
    fn incremental_scoped_outcome_delta_lagging_is_drift() {
        // delta_latest has FEWER distinct keys than source_scoped, and its range is inside
        // source_scoped's -> source_scoped advanced past the last sync. Still Drift, mirroring
        // key_stats_outcome's existing lagging case.
        let s = KeyStats {
            count: 10,
            distinct: 10,
            min: Some(1),
            max: Some(10),
            xor: 11,
            distinct_xor: 11,
            sum: 55,
        };
        let d = KeyStats {
            count: 8,
            distinct: 8,
            min: Some(1),
            max: Some(8),
            xor: 9,
            distinct_xor: 9,
            sum: 36,
        };
        let outcome = VerifyCommand::<MockSourceProbe, MockDeltaProbe>::incremental_scoped_key_stats_outcome(
            "source_scoped",
            "delta_latest",
            &s,
            &d,
        );
        assert!(
            matches!(outcome, TableOutcome::Drift { .. }),
            "expected Drift for a lagging delta_latest, got {outcome:?}"
        );
    }

    #[test]
    fn incremental_scoped_outcome_superset_with_uncontained_range_is_discrepancy() {
        // delta_latest is a superset (more distinct keys) but ALSO claims a min below
        // source_scoped's min -- a genuine phantom/extra key that HWM-advance can't explain.
        // Must stay Discrepancy.
        let s = KeyStats {
            count: 10,
            distinct: 10,
            min: Some(5),
            max: Some(20),
            xor: 1,
            distinct_xor: 1,
            sum: 100,
        };
        let d_min_below = KeyStats {
            count: 12,
            distinct: 12,
            min: Some(1),
            max: Some(20),
            xor: 2,
            distinct_xor: 2,
            sum: 110,
        };
        let outcome = VerifyCommand::<MockSourceProbe, MockDeltaProbe>::incremental_scoped_key_stats_outcome(
            "source_scoped",
            "delta_latest",
            &s,
            &d_min_below,
        );
        assert!(
            matches!(outcome, TableOutcome::Discrepancy { .. }),
            "expected Discrepancy when delta's min is below source's, got {outcome:?}"
        );

        // ...and the same when delta's max exceeds source_scoped's max.
        let d_max_above = KeyStats {
            count: 12,
            distinct: 12,
            min: Some(5),
            max: Some(25),
            xor: 2,
            distinct_xor: 2,
            sum: 130,
        };
        let outcome = VerifyCommand::<MockSourceProbe, MockDeltaProbe>::incremental_scoped_key_stats_outcome(
            "source_scoped",
            "delta_latest",
            &s,
            &d_max_above,
        );
        assert!(
            matches!(outcome, TableOutcome::Discrepancy { .. }),
            "expected Discrepancy when delta's max exceeds source's, got {outcome:?}"
        );
    }

    #[test]
    fn key_stats_outcome_full_refresh_superset_stays_discrepancy_regression_guard() {
        // Regression guard: the untouched `key_stats_outcome` (used by the full_refresh/basic,
        // non-scoped path) must STILL return Discrepancy for a delta superset with a contained
        // range -- PS-M1's Drift carve-out is exclusive to `incremental_scoped_key_stats_outcome`
        // and must never leak into the full_refresh/basic verdict, where an unscoped source is
        // authoritative and a delta superset is always a real bug.
        let source = KeyStats {
            count: 1_623_980,
            distinct: 1_623_980,
            min: Some(1),
            max: Some(6_765_879),
            xor: 0,
            distinct_xor: 0,
            sum: 0,
        };
        let delta = KeyStats {
            count: 1_623_997,
            distinct: 1_623_997,
            min: Some(1),
            max: Some(6_765_879),
            xor: 0,
            distinct_xor: 0,
            sum: 0,
        };
        let outcome = VerifyCommand::<MockSourceProbe, MockDeltaProbe>::key_stats_outcome(
            "source", "delta", &source, &delta,
        );
        assert!(
            matches!(outcome, TableOutcome::Discrepancy { .. }),
            "expected Discrepancy for the unscoped full_refresh/basic path, got {outcome:?}"
        );
    }

    #[tokio::test]
    async fn verify_value_aggregates_match_stays_pass() {
        let mut source = MockSourceProbe::new();
        let mut delta = MockDeltaProbe::new();
        source.expect_integer_pk().returning(|_| Ok(None));
        source.expect_row_count().returning(|_| Ok(3));
        delta.expect_row_count().returning(|_| Ok(3));
        let cols = || {
            Ok(vec![
                ColumnMeta {
                    name: "id".to_string(),
                    type_str: "bigint".to_string(),
                    nullable: false,
                    numeric_scale: None,
                },
                ColumnMeta {
                    name: "amount".to_string(),
                    type_str: "int".to_string(),
                    nullable: true,
                    numeric_scale: None,
                },
            ])
        };
        source.expect_columns().returning(move |_| cols());
        delta.expect_columns().returning(move |_| cols());
        let stats = || {
            Ok(KeyStats {
                count: 3,
                distinct: 3,
                min: Some(1),
                max: Some(3),
                xor: 1,
                distinct_xor: 1,
                sum: 6,
            })
        };
        source.expect_key_stats().returning(move |_, _| stats());
        delta.expect_key_stats().returning(move |_, _| stats());
        source
            .expect_non_null_counts()
            .returning(|_, cols: &[String]| Ok(vec![0i64; cols.len()]));
        delta
            .expect_non_null_counts()
            .returning(|_, cols: &[String]| Ok(vec![0i64; cols.len()]));
        source.expect_sample_ids().returning(|_, _, _| Ok(vec![]));
        // Value aggregates match on both sides
        source
            .expect_value_aggregates()
            .returning(|_, _| Ok(vec![ColumnAggValues { sum: Some("6".to_string()), min: Some("1".to_string()), max: Some("3".to_string()), non_null_count: 3 }]));
        delta
            .expect_value_aggregates()
            .returning(|_, _| Ok(vec![ColumnAggValues { sum: Some("6".to_string()), min: Some("1".to_string()), max: Some("3".to_string()), non_null_count: 3 }]));
        let cmd = VerifyCommand::new(source, delta, vec!["orders".to_string()]);
        let result = cmd.run().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), VerifyVerdict::Clean);
    }

    #[tokio::test]
    async fn verify_value_aggregates_mismatch_downgrades_to_discrepancy() {
        let mut source = MockSourceProbe::new();
        let mut delta = MockDeltaProbe::new();
        source.expect_integer_pk().returning(|_| Ok(None));
        source.expect_row_count().returning(|_| Ok(3));
        delta.expect_row_count().returning(|_| Ok(3));
        let cols = || {
            Ok(vec![
                ColumnMeta {
                    name: "id".to_string(),
                    type_str: "bigint".to_string(),
                    nullable: false,
                    numeric_scale: None,
                },
                ColumnMeta {
                    name: "amount".to_string(),
                    type_str: "int".to_string(),
                    nullable: true,
                    numeric_scale: None,
                },
            ])
        };
        source.expect_columns().returning(move |_| cols());
        delta.expect_columns().returning(move |_| cols());
        let stats = || {
            Ok(KeyStats {
                count: 3,
                distinct: 3,
                min: Some(1),
                max: Some(3),
                xor: 1,
                distinct_xor: 1,
                sum: 6,
            })
        };
        source.expect_key_stats().returning(move |_, _| stats());
        delta.expect_key_stats().returning(move |_, _| stats());
        source
            .expect_non_null_counts()
            .returning(|_, cols: &[String]| Ok(vec![0i64; cols.len()]));
        delta
            .expect_non_null_counts()
            .returning(|_, cols: &[String]| Ok(vec![0i64; cols.len()]));
        source.expect_sample_ids().returning(|_, _, _| Ok(vec![]));
        // Value aggregates differ
        source
            .expect_value_aggregates()
            .returning(|_, _| Ok(vec![ColumnAggValues { sum: Some("6".to_string()), min: Some("1".to_string()), max: Some("3".to_string()), non_null_count: 3 }]));
        delta
            .expect_value_aggregates()
            .returning(|_, _| Ok(vec![ColumnAggValues { sum: Some("9".to_string()), min: Some("2".to_string()), max: Some("3".to_string()), non_null_count: 3 }]));
        let cmd = VerifyCommand::new(source, delta, vec!["orders".to_string()]);
        let result = cmd.run().await;
        assert!(result.is_ok());
        // Mismatch downgrades to Discrepancy
        assert_eq!(result.unwrap(), VerifyVerdict::Discrepancy);
    }

    #[tokio::test]
    async fn verify_incremental_scoped_value_match_stays_pass() {
        let mut source = MockSourceProbe::new();
        let mut delta = MockDeltaProbe::new();
        source.expect_integer_pk().returning(|_| Ok(None));
        delta.expect_row_count().returning(|_| Ok(2));
        let cols = || {
            Ok(vec![
                ColumnMeta {
                    name: "id".to_string(),
                    type_str: "bigint".to_string(),
                    nullable: false,
                    numeric_scale: None,
                },
                ColumnMeta {
                    name: "updated_at".to_string(),
                    type_str: "timestamp".to_string(),
                    nullable: false,
                    numeric_scale: None,
                },
                ColumnMeta {
                    name: "amount".to_string(),
                    type_str: "int".to_string(),
                    nullable: true,
                    numeric_scale: None,
                },
            ])
        };
        source.expect_columns().returning(move |_| cols());
        delta.expect_columns().returning(move |_| cols());
        source.expect_max_cursor().returning(|_, _| Ok(Some("2026-06-30 12:00:00".to_string())));
        delta.expect_max_cursor().returning(|_, _| Ok(Some("2026-06-30T12:00:00.000000".to_string())));
        source.expect_row_count_scoped().returning(|_, _| Ok(2));
        source.expect_key_stats_scoped().returning(|_, _, _| {
            Ok(KeyStats {
                count: 2,
                distinct: 2,
                min: Some(43),
                max: Some(44),
                xor: 7,
                distinct_xor: 7,
                sum: 87,
            })
        });
        delta.expect_latest_key_stats().returning(|_, _, _| {
            Ok(KeyStats {
                count: 2,
                distinct: 2,
                min: Some(43),
                max: Some(44),
                xor: 7,
                distinct_xor: 7,
                sum: 87,
            })
        });
        // Value aggregates match on both sides (scoped)
        source
            .expect_value_aggregates_scoped()
            .returning(|_, _, _| Ok(vec![ColumnAggValues { sum: Some("6".to_string()), min: Some("1".to_string()), max: Some("3".to_string()), non_null_count: 3 }]));
        delta
            .expect_value_aggregates_latest()
            .returning(|_, _, _, _| Ok(vec![ColumnAggValues { sum: Some("6".to_string()), min: Some("1".to_string()), max: Some("3".to_string()), non_null_count: 3 }]));
        let cmd =
            VerifyCommand::new(source, delta, vec!["orders".to_string()]).with_table_plans(vec![
                TablePlan {
                    table: "orders".to_string(),
                    mode: VerifyMode::Incremental {
                        cursor_col: "updated_at".to_string(),
                        hwm: Some(Hwm {
                            updated_at: "2026-06-30 12:00:00".to_string(),
                            last_id: 42,
                        }),
                    },
                },
            ]);
        let result = cmd.run().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), VerifyVerdict::Clean);
    }

    #[tokio::test]
    async fn verify_incremental_scoped_value_mismatch_downgrades() {
        let mut source = MockSourceProbe::new();
        let mut delta = MockDeltaProbe::new();
        source.expect_integer_pk().returning(|_| Ok(None));
        delta.expect_row_count().returning(|_| Ok(2));
        let cols = || {
            Ok(vec![
                ColumnMeta {
                    name: "id".to_string(),
                    type_str: "bigint".to_string(),
                    nullable: false,
                    numeric_scale: None,
                },
                ColumnMeta {
                    name: "updated_at".to_string(),
                    type_str: "timestamp".to_string(),
                    nullable: false,
                    numeric_scale: None,
                },
                ColumnMeta {
                    name: "amount".to_string(),
                    type_str: "int".to_string(),
                    nullable: true,
                    numeric_scale: None,
                },
            ])
        };
        source.expect_columns().returning(move |_| cols());
        delta.expect_columns().returning(move |_| cols());
        source.expect_max_cursor().returning(|_, _| Ok(Some("2026-06-30 12:00:00".to_string())));
        delta.expect_max_cursor().returning(|_, _| Ok(Some("2026-06-30T12:00:00.000000".to_string())));
        source.expect_row_count_scoped().returning(|_, _| Ok(2));
        source.expect_key_stats_scoped().returning(|_, _, _| {
            Ok(KeyStats {
                count: 2,
                distinct: 2,
                min: Some(43),
                max: Some(44),
                xor: 7,
                distinct_xor: 7,
                sum: 87,
            })
        });
        delta.expect_latest_key_stats().returning(|_, _, _| {
            Ok(KeyStats {
                count: 2,
                distinct: 2,
                min: Some(43),
                max: Some(44),
                xor: 7,
                distinct_xor: 7,
                sum: 87,
            })
        });
        // Value aggregates differ (scoped)
        source
            .expect_value_aggregates_scoped()
            .returning(|_, _, _| Ok(vec![ColumnAggValues { sum: Some("6".to_string()), min: Some("1".to_string()), max: Some("3".to_string()), non_null_count: 3 }]));
        delta
            .expect_value_aggregates_latest()
            .returning(|_, _, _, _| Ok(vec![ColumnAggValues { sum: Some("9".to_string()), min: Some("2".to_string()), max: Some("3".to_string()), non_null_count: 3 }]));
        let cmd =
            VerifyCommand::new(source, delta, vec!["orders".to_string()]).with_table_plans(vec![
                TablePlan {
                    table: "orders".to_string(),
                    mode: VerifyMode::Incremental {
                        cursor_col: "updated_at".to_string(),
                        hwm: Some(Hwm {
                            updated_at: "2026-06-30 12:00:00".to_string(),
                            last_id: 42,
                        }),
                    },
                },
            ]);
        let result = cmd.run().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), VerifyVerdict::Discrepancy);
    }

    #[test]
    fn duplicate_key_surplus_detects_two_stream_duplication() {
        // FA2 follow-up. Helper for building a KeyStats with a given count/distinct; the
        // other fields are irrelevant to this invariant.
        let ks = |count: i64, distinct: i64| KeyStats {
            count,
            distinct,
            min: Some(1),
            max: Some(i128::from(distinct.max(1))),
            xor: 0,
            distinct_xor: 0,
            sum: 0,
        };

        // Healthy two-stream current-state mirror: exactly one row per key.
        assert_eq!(duplicate_key_surplus(&ks(100, 100)), None);
        // Empty table.
        assert_eq!(duplicate_key_surplus(&ks(0, 0)), None);
        // FA2-class duplication: 3 rows beyond the distinct key count.
        assert_eq!(duplicate_key_surplus(&ks(103, 100)), Some(3));
        // A single duplicated row must still be reported (no threshold/rounding).
        assert_eq!(duplicate_key_surplus(&ks(101, 100)), Some(1));
        // distinct > count is not representable in practice; must not underflow/panic.
        assert_eq!(duplicate_key_surplus(&ks(100, 101)), None);
    }

    #[test]
    fn sum_overflow_guard_boundaries() {
        // Integer capacity = 65 digits (VA1-r: widened from 38 — MariaDB's DECIMAL max /
        // DataFusion Decimal256 range): int_digits + digits(count) > 65 → skip.
        let d64 = "9".repeat(64);
        let d65 = "9".repeat(65);
        assert!(
            !sum_would_overflow(&AggKind::Integer, Some("1"), Some(&d64), 9),
            "64 + 1 = 65 fits exactly"
        );
        assert!(
            sum_would_overflow(&AggKind::Integer, Some("1"), Some(&d65), 9),
            "65 + 1 = 66 exceeds"
        );
        // count contributes its own digit length: 10 rows (2 digits) tips the same magnitude.
        assert!(sum_would_overflow(&AggKind::Integer, Some("1"), Some(&d64), 10));
        // Decimal at scale 10 → capacity 55 integer digits.
        let dec = AggKind::Decimal { scale: 10 };
        let i54 = format!("{}.5", "9".repeat(54));
        let i55 = format!("{}.5", "9".repeat(55));
        assert!(!sum_would_overflow(&dec, Some("0.1"), Some(&i54), 9));
        assert!(sum_would_overflow(&dec, Some("0.1"), Some(&i55), 9));
        // Sign is ignored for the digit count (min can carry the magnitude).
        let neg = format!("-{d65}");
        assert!(sum_would_overflow(&AggKind::Integer, Some(&neg), Some("1"), 9));
        // Empty column / zero rows never trip the guard.
        assert!(!sum_would_overflow(&AggKind::Integer, None, None, 9));
        assert!(!sum_would_overflow(&AggKind::Integer, Some(&d65), Some(&d65), 0));

        // VA1-r regression: a value that WOULD have overflowed the old 38-digit capacity
        // (40 int digits + 1 count digit = 41 > 38) no longer trips the guard now that the
        // capacity is 65 — the sum is computed and compared instead of being skipped.
        let d40 = "9".repeat(40);
        assert!(
            !sum_would_overflow(&AggKind::Integer, Some("1"), Some(&d40), 9),
            "40 + 1 = 41 fit within the new 65-digit capacity (would have overflowed at 38)"
        );
    }

    #[test]
    fn overflow_guard_skips_sum_on_both_sides() {
        // One side's magnitude trips the guard → BOTH fingerprints carry sum=skipped,
        // while min/max/n stay compared (and here still mismatch).
        let specs = vec![ColumnAgg {
            name: "amount".to_string(),
            kind: AggKind::Decimal { scale: 10 },
        }];
        // Decimal{scale:10} capacity is 65-10=55 integer digits (VA1-r); 56 nines + 1
        // count digit trips the guard.
        let big = "9".repeat(56);
        let src = vec![ColumnAggValues {
            sum: Some("1".to_string()),
            min: Some("0".to_string()),
            max: Some(big),
            non_null_count: 5,
        }];
        let dlt = vec![ColumnAggValues {
            sum: Some("2".to_string()),
            min: Some("0".to_string()),
            max: Some("1".to_string()),
            non_null_count: 5,
        }];
        let fps = assemble_fingerprints(&specs, &src, &dlt);
        assert!(fps[0].0.starts_with("sum=skipped|"));
        assert!(fps[0].1.starts_with("sum=skipped|"));
        assert_ne!(fps[0].0, fps[0].1, "min/max must still be compared");
    }

    #[test]
    fn value_null_swap_detected_via_count() {
        // VA5: a non-extremal 0 → NULL swap leaves sum/min/max identical; the n=
        // component must break the tie.
        let specs = vec![ColumnAgg {
            name: "qty".to_string(),
            kind: AggKind::Integer,
        }];
        let src = vec![ColumnAggValues {
            sum: Some("10".to_string()),
            min: Some("0".to_string()),
            max: Some("7".to_string()),
            non_null_count: 5,
        }];
        let dlt = vec![ColumnAggValues {
            sum: Some("10".to_string()),
            min: Some("0".to_string()),
            max: Some("7".to_string()),
            non_null_count: 4,
        }];
        let fps = assemble_fingerprints(&specs, &src, &dlt);
        assert_ne!(fps[0].0, fps[0].1, "count component must detect the NULL swap");
    }

    #[tokio::test]
    async fn probe_error_on_one_table_skips_it_and_continues() {
        // VA4: a probe failure on one table must yield Skipped for that table and leave
        // the rest of the run intact (previously the whole run aborted with Err).
        let mut source = MockSourceProbe::new();
        let mut delta = MockDeltaProbe::new();
        source.expect_integer_pk().returning(|_| Ok(None));
        source
            .expect_row_count()
            .withf(|t| t == "bad")
            .returning(|_| Err(anyhow::anyhow!("simulated probe failure")));
        source
            .expect_row_count()
            .withf(|t| t == "good")
            .returning(|_| Ok(5));
        delta.expect_row_count().returning(|_| Ok(5));
        let cols = || {
            Ok(vec![ColumnMeta {
                name: "id".to_string(),
                type_str: "bigint".to_string(),
                nullable: false,
                numeric_scale: None,
            }])
        };
        source.expect_columns().returning(move |_| cols());
        delta.expect_columns().returning(move |_| cols());
        let stats = || {
            Ok(KeyStats {
                count: 5,
                distinct: 5,
                min: Some(1),
                max: Some(5),
                xor: 7,
                distinct_xor: 7,
                sum: 15,
            })
        };
        source.expect_key_stats().returning(move |_, _| stats());
        delta.expect_key_stats().returning(move |_, _| stats());
        source
            .expect_non_null_counts()
            .returning(|_, cols: &[String]| Ok(vec![0i64; cols.len()]));
        delta
            .expect_non_null_counts()
            .returning(|_, cols: &[String]| Ok(vec![0i64; cols.len()]));
        source.expect_sample_ids().returning(|_, _, _| Ok(vec![]));
        source.expect_value_aggregates().returning(|_, _| Ok(vec![]));
        delta.expect_value_aggregates().returning(|_, _| Ok(vec![]));
        let cmd = VerifyCommand::new(
            source,
            delta,
            vec!["bad".to_string(), "good".to_string()],
        );
        let result = cmd.run().await;
        assert!(result.is_ok(), "one bad table must not abort the run");
        assert_eq!(
            result.unwrap(),
            VerifyVerdict::Clean,
            "bad → Skipped, good → Pass ⇒ Clean overall"
        );
    }

    // --- V3: key resolution beyond a literal `id` column --------------------------------

    #[tokio::test]
    async fn verify_non_id_integer_pk_drives_key_stats() {
        // A table keyed by `order_id` (no `id` column at all) must still get a real
        // key-set verdict instead of being Skipped-as-Clean (V3's false-confidence trap).
        let mut source = MockSourceProbe::new();
        let mut delta = MockDeltaProbe::new();
        source.expect_row_count().returning(|_| Ok(3));
        delta.expect_row_count().returning(|_| Ok(3));
        let cols = || {
            Ok(vec![ColumnMeta {
                name: "order_id".to_string(),
                type_str: "bigint".to_string(),
                nullable: false,
                numeric_scale: None,
            }])
        };
        source.expect_columns().returning(move |_| cols());
        delta.expect_columns().returning(move |_| cols());
        source
            .expect_integer_pk()
            .returning(|_| Ok(Some("order_id".to_string())));
        let stats = || {
            Ok(KeyStats {
                count: 3,
                distinct: 3,
                min: Some(1),
                max: Some(3),
                xor: 1,
                distinct_xor: 1,
                sum: 6,
            })
        };
        source
            .expect_key_stats()
            .withf(|_, key_col| key_col == "order_id")
            .returning(move |_, _| stats());
        delta
            .expect_key_stats()
            .withf(|_, key_col| key_col == "order_id")
            .returning(move |_, _| stats());
        source
            .expect_non_null_counts()
            .returning(|_, cols: &[String]| Ok(vec![0i64; cols.len()]));
        delta
            .expect_non_null_counts()
            .returning(|_, cols: &[String]| Ok(vec![0i64; cols.len()]));
        source.expect_sample_ids().returning(|_, _, _| Ok(vec![]));
        source
            .expect_value_aggregates()
            .returning(|_, _| Ok(vec![]));
        delta
            .expect_value_aggregates()
            .returning(|_, _| Ok(vec![]));
        let plan = TablePlan::basic("orders");
        let cmd = VerifyCommand::new(source, delta, vec!["orders".to_string()]);
        let outcome = cmd.run_one_table("orders", &plan).await.unwrap();
        assert_eq!(outcome, TableOutcome::Pass);
    }

    #[tokio::test]
    async fn verify_no_pk_and_no_id_column_is_partially_verified_with_honest_reason() {
        // No discovered integer PK and no `id` column: the key-set/row-set check can't run
        // (there's no fair key to compare), but (V3-r Tier 1) that must NOT skip everything —
        // the value-aggregate check on `note` still runs and, matching, ends
        // PartiallyVerified with a reason naming the real gap instead of hardcoding
        // "no `id` column" — a table that legitimately has no usable key shouldn't read as
        // if `id` were the only thing considered.
        let mut source = MockSourceProbe::new();
        let mut delta = MockDeltaProbe::new();
        source.expect_row_count().returning(|_| Ok(3));
        delta.expect_row_count().returning(|_| Ok(3));
        let cols = || {
            Ok(vec![ColumnMeta {
                name: "note".to_string(),
                type_str: "varchar".to_string(),
                nullable: true,
                numeric_scale: None,
            }])
        };
        source.expect_columns().returning(move |_| cols());
        delta.expect_columns().returning(move |_| cols());
        source.expect_integer_pk().returning(|_| Ok(None));
        // No single-column string PK either — stays a Tier 1 key-less table.
        source.expect_string_pk().returning(|_| Ok(None));
        let agg_values = || {
            Ok(vec![ColumnAggValues {
                sum: Some("30".to_string()),
                min: None,
                max: None,
                non_null_count: 3,
            }])
        };
        source
            .expect_value_aggregates()
            .returning(move |_, _| agg_values());
        delta
            .expect_value_aggregates()
            .returning(move |_, _| agg_values());
        let plan = TablePlan::basic("events");
        let cmd = VerifyCommand::new(source, delta, vec!["events".to_string()]);
        let outcome = cmd.run_one_table("events", &plan).await.unwrap();
        match outcome {
            TableOutcome::PartiallyVerified { reason } => {
                assert!(
                    reason.contains("no single-column integer PRIMARY key (or `id` column)"),
                    "unexpected reason: {reason}"
                );
            }
            other => panic!("expected PartiallyVerified, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn verify_two_stream_key_is_insert_cursor_without_probing() {
        // Two-stream mode's key is the config-declared insert_cursor (pipeline intent),
        // not a probed PRIMARY key — integer_pk must NOT be called at all (no expectation
        // is registered for it below; mockall panics on an unexpected call, which is
        // exactly the assertion here).
        let mut source = MockSourceProbe::new();
        let mut delta = MockDeltaProbe::new();
        source.expect_row_count().returning(|_| Ok(3));
        delta.expect_row_count().returning(|_| Ok(3));
        let cols = || {
            Ok(vec![ColumnMeta {
                name: "order_id".to_string(),
                type_str: "bigint".to_string(),
                nullable: false,
                numeric_scale: None,
            }])
        };
        source.expect_columns().returning(move |_| cols());
        delta.expect_columns().returning(move |_| cols());
        source
            .expect_max_cursor()
            .returning(|_, _| Ok(Some("2026-06-30 12:00:00".to_string())));
        delta
            .expect_max_cursor()
            .returning(|_, _| Ok(Some("2026-06-30T12:00:00.000000".to_string())));
        let stats = || {
            Ok(KeyStats {
                count: 3,
                distinct: 3,
                min: Some(1),
                max: Some(3),
                xor: 1,
                distinct_xor: 1,
                sum: 6,
            })
        };
        source
            .expect_key_stats()
            .withf(|_, key_col| key_col == "order_id")
            .returning(move |_, _| stats());
        delta
            .expect_key_stats()
            .withf(|_, key_col| key_col == "order_id")
            .returning(move |_, _| stats());
        source
            .expect_non_null_counts()
            .returning(|_, cols: &[String]| Ok(vec![0i64; cols.len()]));
        delta
            .expect_non_null_counts()
            .returning(|_, cols: &[String]| Ok(vec![0i64; cols.len()]));
        source.expect_sample_ids().returning(|_, _, _| Ok(vec![]));
        source
            .expect_value_aggregates()
            .returning(|_, _| Ok(vec![]));
        delta
            .expect_value_aggregates()
            .returning(|_, _| Ok(vec![]));
        let plan = TablePlan {
            table: "orders".to_string(),
            mode: VerifyMode::TwoStream {
                insert_cursor: "order_id".to_string(),
                update_cursor: "updated_at".to_string(),
                update_hwm: None,
                insert_hwm: Some(3),
            },
        };
        let cmd = VerifyCommand::new(source, delta, vec!["orders".to_string()]);
        let outcome = cmd.run_one_table("orders", &plan).await.unwrap();
        assert_eq!(outcome, TableOutcome::Pass);
    }

    #[tokio::test]
    async fn verify_incremental_scoped_uses_resolved_non_id_key() {
        // The incremental-scoped path (SourceScope.key_col threading) must also use the
        // resolved non-id key end to end: row_count_scoped, key_stats_scoped, and
        // latest_key_stats all see `order_id`, not a literal `id`.
        let mut source = MockSourceProbe::new();
        let mut delta = MockDeltaProbe::new();
        delta.expect_row_count().returning(|_| Ok(2));
        let cols = || {
            Ok(vec![
                ColumnMeta {
                    name: "order_id".to_string(),
                    type_str: "bigint".to_string(),
                    nullable: false,
                    numeric_scale: None,
                },
                ColumnMeta {
                    name: "updated_at".to_string(),
                    type_str: "timestamp".to_string(),
                    nullable: false,
                    numeric_scale: None,
                },
            ])
        };
        source.expect_columns().returning(move |_| cols());
        delta.expect_columns().returning(move |_| cols());
        source
            .expect_max_cursor()
            .returning(|_, _| Ok(Some("2026-06-30 12:00:00".to_string())));
        delta
            .expect_max_cursor()
            .returning(|_, _| Ok(Some("2026-06-30T12:00:00.000000".to_string())));
        source
            .expect_integer_pk()
            .returning(|_| Ok(Some("order_id".to_string())));
        source.expect_row_count_scoped().returning(|_, scope| {
            assert_eq!(scope.key_col, "order_id");
            Ok(2)
        });
        source
            .expect_key_stats_scoped()
            .returning(|_, key_col, scope| {
                assert_eq!(key_col, "order_id");
                assert_eq!(scope.key_col, "order_id");
                Ok(KeyStats {
                    count: 2,
                    distinct: 2,
                    min: Some(43),
                    max: Some(44),
                    xor: 7,
                    distinct_xor: 7,
                    sum: 87,
                })
            });
        delta
            .expect_latest_key_stats()
            .returning(|_, key_col, cursor_col| {
                assert_eq!(key_col, "order_id");
                assert_eq!(cursor_col, "updated_at");
                Ok(KeyStats {
                    count: 2,
                    distinct: 2,
                    min: Some(43),
                    max: Some(44),
                    xor: 7,
                    distinct_xor: 7,
                    sum: 87,
                })
            });
        source
            .expect_value_aggregates_scoped()
            .returning(|_, _, _| Ok(vec![]));
        delta
            .expect_value_aggregates_latest()
            .returning(|_, _, _, _| Ok(vec![]));
        let plan = TablePlan {
            table: "orders".to_string(),
            mode: VerifyMode::Incremental {
                cursor_col: "updated_at".to_string(),
                hwm: Some(Hwm {
                    updated_at: "2026-06-30 12:00:00".to_string(),
                    last_id: 42,
                }),
            },
        };
        let cmd = VerifyCommand::new(source, delta, vec!["orders".to_string()]);
        let outcome = cmd.run_one_table("orders", &plan).await.unwrap();
        assert_eq!(outcome, TableOutcome::Pass);
    }

    // --- V3-r Tier 2: single-column string PRIMARY key ----------------------------------

    #[test]
    fn string_key_stats_outcome_pass_drift_discrepancy() {
        let s = StringKeyStats {
            count: 3,
            distinct: 3,
            min: Some("a".to_string()),
            max: Some("z".to_string()),
        };

        // Equal on both sides -> Pass.
        let d_equal = s.clone();
        let outcome = VerifyCommand::<MockSourceProbe, MockDeltaProbe>::string_key_stats_outcome(
            "source", "delta", &s, &d_equal,
        );
        assert_eq!(outcome, TableOutcome::Pass);

        // Delta lagging (fewer distinct, its [min,max] within source's byte range) -> Drift,
        // not a failure (source advanced past sync).
        let d_lagging = StringKeyStats {
            count: 2,
            distinct: 2,
            min: Some("b".to_string()),
            max: Some("y".to_string()),
        };
        let outcome = VerifyCommand::<MockSourceProbe, MockDeltaProbe>::string_key_stats_outcome(
            "source",
            "delta",
            &s,
            &d_lagging,
        );
        assert!(
            matches!(outcome, TableOutcome::Drift { .. }),
            "expected Drift, got {outcome:?}"
        );

        // Delta has a key beyond the source's range (e.g. an extra/missing-in-source key) ->
        // Discrepancy.
        let d_extra = StringKeyStats {
            count: 4,
            distinct: 4,
            min: Some("a".to_string()),
            max: Some("zz".to_string()),
        };
        let outcome = VerifyCommand::<MockSourceProbe, MockDeltaProbe>::string_key_stats_outcome(
            "source", "delta", &s, &d_extra,
        );
        assert!(
            matches!(outcome, TableOutcome::Discrepancy { .. }),
            "expected Discrepancy, got {outcome:?}"
        );
    }

    #[tokio::test]
    async fn verify_string_key_table_pass_and_discrepancy() {
        // A single-column string PK (no integer PK, no `id` column) must get a real key-set
        // fingerprint via string_pk/string_key_stats instead of Tier 1's value-aggregates-only
        // PartiallyVerified — a full match rolls up to Clean, not PartiallyVerified.
        let mut source = MockSourceProbe::new();
        let mut delta = MockDeltaProbe::new();
        source.expect_integer_pk().returning(|_| Ok(None));
        source
            .expect_string_pk()
            .returning(|_| Ok(Some("token".to_string())));
        source.expect_row_count().returning(|_| Ok(3));
        delta.expect_row_count().returning(|_| Ok(3));
        let cols = || {
            Ok(vec![ColumnMeta {
                name: "token".to_string(),
                type_str: "varchar".to_string(),
                nullable: false,
                numeric_scale: None,
            }])
        };
        source.expect_columns().returning(move |_| cols());
        delta.expect_columns().returning(move |_| cols());
        // The integer key-set path must never be touched for a string-keyed table.
        source.expect_key_stats().times(0).returning(|_, _| unreachable!());
        delta.expect_key_stats().times(0).returning(|_, _| unreachable!());
        let matching_stats = || {
            Ok(StringKeyStats {
                count: 3,
                distinct: 3,
                min: Some("aaa".to_string()),
                max: Some("zzz".to_string()),
            })
        };
        source
            .expect_string_key_stats()
            .returning(move |_, _| matching_stats());
        delta
            .expect_string_key_stats()
            .returning(move |_, _| matching_stats());
        let agg_values = || {
            Ok(vec![ColumnAggValues {
                sum: Some("9".to_string()),
                min: None,
                max: None,
                non_null_count: 3,
            }])
        };
        source
            .expect_value_aggregates()
            .returning(move |_, _| agg_values());
        delta
            .expect_value_aggregates()
            .returning(move |_, _| agg_values());
        let cmd = VerifyCommand::new(source, delta, vec!["tokens".to_string()]);
        let result = cmd.run().await;
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            VerifyVerdict::Clean,
            "a fully matching string key-set must roll up to Clean, not PartiallyVerified"
        );
    }

    #[tokio::test]
    async fn verify_string_key_table_max_mismatch_is_discrepancy() {
        // Same setup as the Pass case, but delta's string key stats show a key range beyond
        // the source's (an extra row Delta has that source doesn't) -> Discrepancy.
        let mut source = MockSourceProbe::new();
        let mut delta = MockDeltaProbe::new();
        source.expect_integer_pk().returning(|_| Ok(None));
        source
            .expect_string_pk()
            .returning(|_| Ok(Some("token".to_string())));
        source.expect_row_count().returning(|_| Ok(3));
        delta.expect_row_count().returning(|_| Ok(3));
        let cols = || {
            Ok(vec![ColumnMeta {
                name: "token".to_string(),
                type_str: "varchar".to_string(),
                nullable: false,
                numeric_scale: None,
            }])
        };
        source.expect_columns().returning(move |_| cols());
        delta.expect_columns().returning(move |_| cols());
        source.expect_key_stats().times(0).returning(|_, _| unreachable!());
        delta.expect_key_stats().times(0).returning(|_, _| unreachable!());
        source.expect_string_key_stats().returning(|_, _| {
            Ok(StringKeyStats {
                count: 3,
                distinct: 3,
                min: Some("aaa".to_string()),
                max: Some("zzz".to_string()),
            })
        });
        delta.expect_string_key_stats().returning(|_, _| {
            Ok(StringKeyStats {
                count: 3,
                distinct: 3,
                min: Some("aaa".to_string()),
                max: Some("zzzz".to_string()),
            })
        });
        let agg_values = || {
            Ok(vec![ColumnAggValues {
                sum: Some("9".to_string()),
                min: None,
                max: None,
                non_null_count: 3,
            }])
        };
        source
            .expect_value_aggregates()
            .returning(move |_, _| agg_values());
        delta
            .expect_value_aggregates()
            .returning(move |_, _| agg_values());
        let cmd = VerifyCommand::new(source, delta, vec!["tokens".to_string()]);
        let result = cmd.run().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), VerifyVerdict::Discrepancy);
    }
}

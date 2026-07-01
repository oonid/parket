use anyhow::{Context, Result};
use deltalake::arrow::array::{Array, Int64Array, StringArray, StringViewArray};
use deltalake::datafusion::prelude::SessionContext;
use std::collections::HashMap;

use super::{ColumnMeta, DeltaProbe, KeyStats};
use crate::writer::DeltaWriter;

pub struct DeltaProbeAdapter {
    writer: DeltaWriter,
}
impl DeltaProbeAdapter {
    pub fn new(writer: DeltaWriter) -> Self {
        Self { writer }
    }
}
impl DeltaProbe for DeltaProbeAdapter {
    async fn row_count(&self, table: &str) -> Result<i64> {
        let t = self.writer.open_table(table).await?;
        let ctx = SessionContext::new();
        ctx.register_table("t", t.table_provider().await?)?;
        let batches = ctx
            .sql("SELECT count(*) AS n FROM t")
            .await?
            .collect()
            .await?;
        let n = batches
            .first()
            .and_then(|b| b.column(0).as_any().downcast_ref::<Int64Array>())
            .map(|a| a.value(0))
            .unwrap_or(0);
        Ok(n)
    }

    async fn columns(&self, table: &str) -> Result<Vec<ColumnMeta>> {
        let t = self.writer.open_table(table).await?;
        let schema = t.table_provider().await?.schema();
        Ok(schema
            .fields()
            .iter()
            .map(|f| ColumnMeta {
                name: f.name().clone(),
                type_str: format!("{:?}", f.data_type()),
                nullable: f.is_nullable(),
            })
            .collect())
    }

    async fn key_stats(&self, table: &str, key_col: &str) -> Result<KeyStats> {
        let t = self.writer.open_table(table).await?;
        let ctx = SessionContext::new();
        ctx.register_table("t", t.table_provider().await?)?;
        let sql = format!(
            "SELECT count(*) AS c, count(distinct `{key_col}`) AS d, \
             min(cast(`{key_col}` as bigint)) AS mn, max(cast(`{key_col}` as bigint)) AS mx, \
             bit_xor(cast(`{key_col}` as bigint)) AS x, bit_xor(distinct cast(`{key_col}` as bigint)) AS dx FROM t"
        );
        let batches = ctx.sql(&sql).await?.collect().await?;
        let b = batches.first().context("delta key_stats: empty result")?;
        // helper: read column `i` as Int64, returning Option (None if null)
        let col_opt = |i: usize| -> Option<i64> {
            b.column(i)
                .as_any()
                .downcast_ref::<Int64Array>()
                .and_then(|a| {
                    if a.is_empty() || a.is_null(0) {
                        None
                    } else {
                        Some(a.value(0))
                    }
                })
        };
        Ok(KeyStats {
            count: col_opt(0).unwrap_or(0),
            distinct: col_opt(1).unwrap_or(0),
            min: col_opt(2),
            max: col_opt(3),
            xor: col_opt(4).unwrap_or(0),
            distinct_xor: col_opt(5).unwrap_or(0),
        })
    }

    async fn latest_key_stats(
        &self,
        table: &str,
        key_col: &str,
        cursor_col: &str,
    ) -> Result<KeyStats> {
        let t = self.writer.open_table(table).await?;
        let ctx = SessionContext::new();
        ctx.register_table("t", t.table_provider().await?)?;
        let sql = format!(
            "WITH ranked AS (              SELECT cast(`{key_col}` as bigint) AS key_value,                     row_number() OVER (PARTITION BY cast(`{key_col}` as bigint) ORDER BY `{cursor_col}` DESC) AS rn              FROM t              )              SELECT count(*) AS c, count(distinct key_value) AS d,                     min(key_value) AS mn, max(key_value) AS mx,                     bit_xor(key_value) AS x, bit_xor(distinct key_value) AS dx              FROM ranked WHERE rn = 1"
        );
        let batches = ctx.sql(&sql).await?.collect().await?;
        let b = batches
            .first()
            .context("delta latest_key_stats: empty result")?;
        let col_opt = |i: usize| -> Option<i64> {
            b.column(i)
                .as_any()
                .downcast_ref::<Int64Array>()
                .and_then(|a| {
                    if a.is_empty() || a.is_null(0) {
                        None
                    } else {
                        Some(a.value(0))
                    }
                })
        };
        Ok(KeyStats {
            count: col_opt(0).unwrap_or(0),
            distinct: col_opt(1).unwrap_or(0),
            min: col_opt(2),
            max: col_opt(3),
            xor: col_opt(4).unwrap_or(0),
            distinct_xor: col_opt(5).unwrap_or(0),
        })
    }

    async fn non_null_counts(&self, table: &str, columns: &[String]) -> Result<Vec<i64>> {
        if columns.is_empty() {
            return Ok(vec![]);
        }
        let t = self.writer.open_table(table).await?;
        let ctx = SessionContext::new();
        ctx.register_table("t", t.table_provider().await?)?;
        let select_list = columns
            .iter()
            .enumerate()
            .map(|(i, c)| format!("count(`{c}`) AS c{i}"))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!("SELECT {select_list} FROM t");
        let batches = ctx.sql(&sql).await?.collect().await?;
        let b = batches
            .first()
            .context("delta non_null_counts: empty result")?;
        let mut out = Vec::with_capacity(columns.len());
        for i in 0..columns.len() {
            let val = b
                .column(i)
                .as_any()
                .downcast_ref::<Int64Array>()
                .and_then(|a| {
                    if a.is_empty() || a.is_null(0) {
                        None
                    } else {
                        Some(a.value(0))
                    }
                })
                .unwrap_or(0);
            out.push(val);
        }
        Ok(out)
    }

    async fn sample_rows(
        &self,
        table: &str,
        id_col: &str,
        columns: &[String],
        ids: &[i64],
    ) -> Result<HashMap<i64, Vec<Option<String>>>> {
        if columns.is_empty() || ids.is_empty() {
            return Ok(HashMap::new());
        }
        let t = self.writer.open_table(table).await?;
        let ctx = SessionContext::new();
        ctx.register_table("t", t.table_provider().await?)?;
        let mut select_parts = vec![format!("cast(`{id_col}` as bigint) AS k")];
        for (i, c) in columns.iter().enumerate() {
            select_parts.push(format!("cast(`{c}` as varchar) AS c{i}"));
        }
        let select_list = select_parts.join(", ");
        let ids_list = ids
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        let sql =
            format!("SELECT {select_list} FROM t WHERE cast(`{id_col}` as bigint) IN ({ids_list})");
        let batches = ctx.sql(&sql).await?.collect().await?;
        let mut map = HashMap::new();
        for b in batches {
            let id_col_arr = b
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .context("delta sample_rows: id column is not Int64Array")?;
            for r in 0..b.num_rows() {
                let id = id_col_arr.value(r);
                let mut vals = Vec::with_capacity(columns.len());
                for i in 0..columns.len() {
                    let col = b.column(i + 1);
                    let val = if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                        if arr.is_null(r) {
                            None
                        } else {
                            Some(arr.value(r).to_string())
                        }
                    } else if let Some(arr) = col.as_any().downcast_ref::<StringViewArray>() {
                        if arr.is_null(r) {
                            None
                        } else {
                            Some(arr.value(r).to_string())
                        }
                    } else {
                        return Err(anyhow::anyhow!(
                            "delta sample_rows: value column {} has unsupported type {:?} (expected StringArray or StringViewArray)",
                            i,
                            col.data_type()
                        ));
                    };
                    vals.push(val);
                }
                map.insert(id, vals);
            }
        }
        Ok(map)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltalake::arrow::array::StringArray;
    use deltalake::arrow::datatypes::{DataType, Field};
    use deltalake::arrow::record_batch::RecordBatch;

    #[tokio::test]
    async fn delta_probe_row_count_real() {
        let temp = tempfile::tempdir().unwrap();
        let writer = DeltaWriter::new_local(temp.path().to_str().unwrap());
        let schema = std::sync::Arc::new(deltalake::arrow::datatypes::Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("qty", DataType::Int64, false),
        ]));
        writer.ensure_table("orders", schema.clone()).await.unwrap();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                std::sync::Arc::new(Int64Array::from(vec![1i64, 2i64, 3i64])),
                std::sync::Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
                std::sync::Arc::new(Int64Array::from(vec![10i64, 20i64, 30i64])),
            ],
        )
        .unwrap();
        writer
            .append_batch("orders", vec![batch], None)
            .await
            .unwrap();

        let probe = DeltaProbeAdapter::new(writer);
        let count = probe.row_count("orders").await.unwrap();
        assert_eq!(count, 3);
    }

    #[tokio::test]
    async fn delta_probe_columns_real() {
        let temp = tempfile::tempdir().unwrap();
        let writer = DeltaWriter::new_local(temp.path().to_str().unwrap());
        let schema = std::sync::Arc::new(deltalake::arrow::datatypes::Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("qty", DataType::Int64, false),
        ]));
        writer.ensure_table("orders", schema.clone()).await.unwrap();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                std::sync::Arc::new(Int64Array::from(vec![1i64, 2i64, 3i64])),
                std::sync::Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
                std::sync::Arc::new(Int64Array::from(vec![10i64, 20i64, 30i64])),
            ],
        )
        .unwrap();
        writer
            .append_batch("orders", vec![batch], None)
            .await
            .unwrap();

        let probe = DeltaProbeAdapter::new(writer);
        let cols = probe.columns("orders").await.unwrap();
        assert!(cols.iter().any(|c| c.name == "id"));
        assert!(cols.iter().any(|c| c.name == "name"));
        assert!(cols.iter().any(|c| c.name == "qty"));
    }

    #[tokio::test]
    async fn delta_probe_key_stats_real() {
        let temp = tempfile::tempdir().unwrap();
        let writer = DeltaWriter::new_local(temp.path().to_str().unwrap());
        let schema = std::sync::Arc::new(deltalake::arrow::datatypes::Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("qty", DataType::Int64, false),
        ]));
        writer.ensure_table("orders", schema.clone()).await.unwrap();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                std::sync::Arc::new(Int64Array::from(vec![1i64, 2i64, 3i64])),
                std::sync::Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
                std::sync::Arc::new(Int64Array::from(vec![10i64, 20i64, 30i64])),
            ],
        )
        .unwrap();
        writer
            .append_batch("orders", vec![batch], None)
            .await
            .unwrap();

        let probe = DeltaProbeAdapter::new(writer);
        let ks = probe.key_stats("orders", "id").await.unwrap();
        assert_eq!(ks.count, 3);
        assert_eq!(ks.distinct, 3);
        assert_eq!(ks.min, Some(1));
        assert_eq!(ks.max, Some(3));
        assert_eq!(ks.xor, 0); // 1 ^ 2 ^ 3 = 0
        assert_eq!(ks.distinct_xor, 0); // 1 ^ 2 ^ 3 = 0
    }

    #[tokio::test]
    async fn delta_probe_non_null_counts_real() {
        let temp = tempfile::tempdir().unwrap();
        let writer = DeltaWriter::new_local(temp.path().to_str().unwrap());
        let schema = std::sync::Arc::new(deltalake::arrow::datatypes::Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("qty", DataType::Int64, false),
        ]));
        writer.ensure_table("orders", schema.clone()).await.unwrap();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                std::sync::Arc::new(Int64Array::from(vec![1i64, 2i64, 3i64])),
                std::sync::Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
                std::sync::Arc::new(Int64Array::from(vec![10i64, 20i64, 30i64])),
            ],
        )
        .unwrap();
        writer
            .append_batch("orders", vec![batch], None)
            .await
            .unwrap();

        let probe = DeltaProbeAdapter::new(writer);
        let counts = probe
            .non_null_counts(
                "orders",
                &["id".to_string(), "name".to_string(), "qty".to_string()],
            )
            .await
            .unwrap();
        assert_eq!(counts, vec![3, 2, 3]); // name has 1 NULL
    }

    #[tokio::test]
    async fn delta_probe_sample_rows_real() {
        let temp = tempfile::tempdir().unwrap();
        let writer = DeltaWriter::new_local(temp.path().to_str().unwrap());
        let schema = std::sync::Arc::new(deltalake::arrow::datatypes::Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("qty", DataType::Int64, false),
        ]));
        writer.ensure_table("orders", schema.clone()).await.unwrap();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                std::sync::Arc::new(Int64Array::from(vec![1i64, 2i64, 3i64])),
                std::sync::Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
                std::sync::Arc::new(Int64Array::from(vec![10i64, 20i64, 30i64])),
            ],
        )
        .unwrap();
        writer
            .append_batch("orders", vec![batch], None)
            .await
            .unwrap();

        let probe = DeltaProbeAdapter::new(writer);
        let rows = probe
            .sample_rows(
                "orders",
                "id",
                &["name".to_string(), "qty".to_string()],
                &[1, 2, 3],
            )
            .await
            .unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(
            rows.get(&1),
            Some(&vec![Some("a".to_string()), Some("10".to_string())])
        );
        assert_eq!(rows.get(&2), Some(&vec![None, Some("20".to_string())]));
        assert_eq!(
            rows.get(&3),
            Some(&vec![Some("c".to_string()), Some("30".to_string())])
        );
    }
}

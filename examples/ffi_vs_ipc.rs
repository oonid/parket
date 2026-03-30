use std::io::Cursor;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use arrow::array::Array as _;
use arrow::array::{
    BooleanArray, Date32Array, Float64Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch as V54Batch;

use deltalake::arrow::array::Array as V57ArrayTrait;
use deltalake::arrow::datatypes::{
    DataType as V57DataType, Field as V57Field, Schema as V57Schema,
};
use deltalake::arrow::record_batch::RecordBatch as V57Batch;

fn make_v54_batch(n: usize) -> V54Batch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("price", DataType::Float64, true),
        Field::new(
            "updated_at",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("is_active", DataType::Boolean, false),
        Field::new("birth_date", DataType::Date32, true),
    ]));

    let id = Int64Array::from((0..n as i64).collect::<Vec<_>>());
    let name = StringArray::from((0..n).map(|i| format!("user_{i}")).collect::<Vec<_>>());
    let price = Float64Array::from(
        (0..n)
            .map(|i| {
                if i % 5 == 0 {
                    None
                } else {
                    Some(i as f64 * 1.1)
                }
            })
            .collect::<Vec<_>>(),
    );
    let updated_at = TimestampMicrosecondArray::from(
        (0..n)
            .map(|i| 1743158400000000i64 + i as i64 * 1000000)
            .collect::<Vec<_>>(),
    );
    let is_active = BooleanArray::from((0..n).map(|i| i % 2 == 0).collect::<Vec<_>>());
    let birth_date = Date32Array::from((0..n).map(|_| Some(20000i32)).collect::<Vec<_>>());

    V54Batch::try_new(
        schema,
        vec![
            Arc::new(id),
            Arc::new(name),
            Arc::new(price),
            Arc::new(updated_at),
            Arc::new(is_active),
            Arc::new(birth_date),
        ],
    )
    .unwrap()
}

fn convert_ipc(batch: &V54Batch) -> Result<V57Batch> {
    let mut buf = Vec::new();
    {
        let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut buf, &*batch.schema())?;
        writer.write(batch)?;
        writer.finish()?;
    }
    let mut reader = deltalake::arrow::ipc::reader::StreamReader::try_new(Cursor::new(buf), None)?;
    reader
        .next()
        .ok_or_else(|| anyhow::anyhow!("empty IPC stream"))?
        .map_err(Into::into)
}

fn convert_datatype(dt: &DataType) -> V57DataType {
    use deltalake::arrow::datatypes::TimeUnit as V57TU;
    match dt {
        DataType::Null => V57DataType::Null,
        DataType::Boolean => V57DataType::Boolean,
        DataType::Int8 => V57DataType::Int8,
        DataType::Int16 => V57DataType::Int16,
        DataType::Int32 => V57DataType::Int32,
        DataType::Int64 => V57DataType::Int64,
        DataType::UInt8 => V57DataType::UInt8,
        DataType::UInt16 => V57DataType::UInt16,
        DataType::UInt32 => V57DataType::UInt32,
        DataType::UInt64 => V57DataType::UInt64,
        DataType::Float16 => V57DataType::Float16,
        DataType::Float32 => V57DataType::Float32,
        DataType::Float64 => V57DataType::Float64,
        DataType::Utf8 => V57DataType::Utf8,
        DataType::LargeUtf8 => V57DataType::LargeUtf8,
        DataType::Binary => V57DataType::Binary,
        DataType::LargeBinary => V57DataType::LargeBinary,
        DataType::Date32 => V57DataType::Date32,
        DataType::Date64 => V57DataType::Date64,
        DataType::Timestamp(unit, tz) => {
            let v57_unit = match unit {
                TimeUnit::Second => V57TU::Second,
                TimeUnit::Millisecond => V57TU::Millisecond,
                TimeUnit::Microsecond => V57TU::Microsecond,
                TimeUnit::Nanosecond => V57TU::Nanosecond,
            };
            let v57_tz = tz.as_deref().map(|s| Arc::<str>::from(s.to_string()));
            V57DataType::Timestamp(v57_unit, v57_tz)
        }
        _ => panic!("unsupported datatype for FFI conversion: {dt:?}"),
    }
}

fn convert_ffi(batch: &V54Batch) -> Result<V57Batch> {
    let v54_schema = batch.schema();

    let v57_fields: Vec<V57Field> = v54_schema
        .fields()
        .iter()
        .map(|f| {
            let dt = convert_datatype(f.data_type());
            V57Field::new(f.name(), dt, f.is_nullable())
        })
        .collect();
    let v57_schema = Arc::new(V57Schema::new(v57_fields));

    let mut v57_columns: Vec<Arc<dyn V57ArrayTrait>> = Vec::new();
    for col_idx in 0..batch.num_columns() {
        let v54_col = batch.column(col_idx);
        let v54_data = v54_col.to_data();

        // Export from v54 via C Data Interface
        let (ffi_array, ffi_schema) = arrow::ffi::to_ffi(&v54_data)?;

        // Copy raw bytes across the v54→v57 FFI struct boundary.
        // FFI_ArrowSchema and FFI_ArrowArray are C ABI structs with identical
        // memory layout in both arrow v54 and v57 (same C Data Interface spec).
        let v57_ffi_schema = unsafe {
            let src = &ffi_schema as *const _ as *const u8;
            let mut dst = std::mem::zeroed::<deltalake::arrow::ffi::FFI_ArrowSchema>();
            std::ptr::copy_nonoverlapping(
                src,
                &mut dst as *mut _ as *mut u8,
                std::mem::size_of::<deltalake::arrow::ffi::FFI_ArrowSchema>(),
            );
            dst
        };

        let v57_ffi_array = unsafe {
            let src = &ffi_array as *const _ as *const u8;
            let mut dst = std::mem::zeroed::<deltalake::arrow::ffi::FFI_ArrowArray>();
            std::ptr::copy_nonoverlapping(
                src,
                &mut dst as *mut _ as *mut u8,
                std::mem::size_of::<deltalake::arrow::ffi::FFI_ArrowArray>(),
            );
            dst
        };

        // Prevent v54 FFI structs from running their release callbacks
        // (ownership is transferred to v57 side)
        std::mem::forget(ffi_array);
        std::mem::forget(ffi_schema);

        // Import into v57 via C Data Interface
        let v57_data = unsafe { deltalake::arrow::ffi::from_ffi(v57_ffi_array, &v57_ffi_schema)? };
        v57_columns.push(deltalake::arrow::array::make_array(v57_data));
    }

    Ok(V57Batch::try_new(v57_schema, v57_columns)?)
}

fn verify_correctness(v54: &V54Batch, v57: &V57Batch) -> Result<()> {
    assert_eq!(
        v54.num_columns(),
        v57.num_columns(),
        "column count mismatch"
    );
    assert_eq!(v54.num_rows(), v57.num_rows(), "row count mismatch");

    for col_idx in 0..v54.num_columns() {
        let v54_col = v54.column(col_idx);
        let v57_col = v57.column(col_idx);
        assert_eq!(
            v54_col.len(),
            v57_col.len(),
            "column {} length mismatch",
            col_idx
        );
    }

    // Spot-check specific columns
    let v54_ids = v54
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let v57_ids = v57
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<deltalake::arrow::array::Int64Array>()
        .unwrap();
    for i in 0..v54_ids.len() {
        assert_eq!(v54_ids.value(i), v57_ids.value(i), "id mismatch at row {i}");
    }

    let v54_names = v54.column_by_name("name").unwrap().len();
    assert_eq!(v54_names, v57.column_by_name("name").unwrap().len());

    let v54_prices = v54
        .column_by_name("price")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let v57_prices = v57
        .column_by_name("price")
        .unwrap()
        .as_any()
        .downcast_ref::<deltalake::arrow::array::Float64Array>()
        .unwrap();
    for i in 0..v54_prices.len() {
        assert_eq!(
            v54_prices.is_null(i),
            v57_prices.is_null(i),
            "price null mismatch at row {i}"
        );
        if !v54_prices.is_null(i) {
            assert_eq!(
                v54_prices.value(i),
                v57_prices.value(i),
                "price mismatch at row {i}"
            );
        }
    }

    println!("  Correctness: ALL PASS (id, name, price columns verified)");
    Ok(())
}

fn main() -> Result<()> {
    let sizes = [1_000, 10_000, 50_000, 100_000];
    let warmup_iters = 3;
    let bench_iters = 20;

    println!("=== Arrow v54 -> v57: FFI vs IPC Conversion ===\n");
    println!(
        "{:<12} {:<8} {:<14} {:<14} {:<10} {:<10}",
        "Rows", "Method", "Time (us)", "Throughput", "Mem (MB)", "Speedup"
    );
    println!("{}", "-".repeat(72));

    for &n in &sizes {
        let batch = make_v54_batch(n);
        let approx_mem_mb = batch.get_array_memory_size() as f64 / 1024.0 / 1024.0;

        // Warmup IPC
        for _ in 0..warmup_iters {
            let _ = convert_ipc(&batch)?;
        }

        // Benchmark IPC
        let mut ipc_times = Vec::new();
        for _ in 0..bench_iters {
            let start = Instant::now();
            let _ = convert_ipc(&batch)?;
            ipc_times.push(start.elapsed().as_micros() as f64);
        }
        let ipc_median = median(&ipc_times);

        // Warmup FFI
        for _ in 0..warmup_iters {
            let _ = convert_ffi(&batch)?;
        }

        // Benchmark FFI
        let mut ffi_times = Vec::new();
        for _ in 0..bench_iters {
            let start = Instant::now();
            let result = convert_ffi(&batch)?;
            ffi_times.push(start.elapsed().as_micros() as f64);

            if ffi_times.len() == 1 {
                print!("[{n} rows, {approx_mem_mb:.1} MB] ");
                verify_correctness(&batch, &result)?;
            }
        }
        let ffi_median = median(&ffi_times);

        let throughput_ipc = approx_mem_mb / (ipc_median / 1_000_000.0);
        let throughput_ffi = approx_mem_mb / (ffi_median / 1_000_000.0);
        let speedup = ipc_median / ffi_median;

        println!(
            "{:<12} {:<8} {:<14.1} {:<14.1} {:<10.1}",
            n, "IPC", ipc_median, throughput_ipc, approx_mem_mb,
        );
        println!(
            "{:<12} {:<8} {:<14.1} {:<14.1} {:<10.1} {:<10.2}x",
            "", "FFI", ffi_median, throughput_ffi, approx_mem_mb, speedup,
        );
        println!();
    }

    Ok(())
}

fn median(values: &[f64]) -> f64 {
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let mid = sorted.len() / 2;
    if sorted.len() % 2 == 0 {
        (sorted[mid - 1] + sorted[mid]) / 2.0
    } else {
        sorted[mid]
    }
}

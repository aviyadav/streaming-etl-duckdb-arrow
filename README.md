# Streaming ETL with DuckDB and Arrow

A high-performance streaming ETL pipeline that demonstrates zero-copy data transfer between PyArrow and DuckDB for real-time data processing.

## Features

- **Zero-Copy Integration**: Seamless data transfer between PyArrow and DuckDB without memory overhead
- **Batch Processing**: Configurable batch sizes (50,000 rows) and time-based checkpoints (30 seconds)
- **Partitioned Storage**: Automatic Parquet file partitioning by region with hourly timestamps
- **Real-time Analytics**: Live aggregation and monitoring using Polars DataFrames
- **Memory Efficient**: Dictionary encoding for categorical data and optimized data types

## Architecture

```
Event Source → Buffer → PyArrow RecordBatch → DuckDB ETL → Arrow Table → Parquet Files
                                                    ↓
                                            Polars Analytics
```

## Data Flow

1. **Ingestion**: Events are buffered in memory until batch size or checkpoint time is reached
2. **Batch Conversion**: Buffered events are converted to PyArrow RecordBatch with optimized types
3. **ETL Processing**: DuckDB performs transformations (timestamp conversion, bucketing, rounding)
4. **Persistence**: Transformed data is written to region-partitioned Parquet files
5. **Monitoring**: Real-time aggregates computed and displayed using Polars

## Configuration

- `BATCH_ROWS`: 50,000 - Number of events per batch
- `CHECKPOINT_SECONDS`: 30 - Maximum time before flushing buffer
- `OUT_PATH`: "checkpoints/" - Directory for Parquet output

## Usage

```bash
python main.py
```

The pipeline will:
- Generate simulated events (timestamp, user_id, region, amount, sku)
- Process them in batches
- Write partitioned Parquet files to `checkpoints/region=<region>/`
- Display real-time statistics by bucket

## Dependencies

- **DuckDB** (≥1.4.2): In-memory analytical database
- **PyArrow** (≥22.0.0): Apache Arrow Python bindings
- **Polars** (≥1.35.2): Fast DataFrame library

No pandas or numpy dependencies - uses Polars for analytics.

## Output

Parquet files are organized by:
- Region partition: `region={us,eu,apac,latam,africa}`
- Hourly timestamp: `events-YYYY-MM-DD-HH-{i}.parquet`

Console output shows aggregated statistics:
```
bucket | n     | avg_amount
high   | 10123 | 89.45
medium | 24876 | 55.12
low    | 15001 | 15.34
```

## Performance Notes

- Uses dictionary encoding for region field to reduce memory footprint
- DuckDB configured with 8 threads and 4GB memory limit
- Zero-copy Arrow integration eliminates serialization overhead
- Batch processing optimizes I/O and computation
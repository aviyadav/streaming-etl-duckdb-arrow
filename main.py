import time
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

BATCH_ROWS = 50_000
CHECKPOINT_SECONDS = 30
OUT_PATH = "checkpoints/"

con = duckdb.connect()
con.execute("PRAGMA threads=8;")
con.execute("SET memory_limit='4GB';")

def source():
    """
    Yield dictionaries simulating events.
    Swap for Kafka messages or any source.
    """

    import random, string
    while True:
        yield {
            "ts": int(time.time()),
            "user_id": random.randint(1, 100_000),
            "region": random.choice(["us", "eu", "apac", "latam", "africa"]),
            "amount": random.random() * 100.0,
            "sku": "".join(random.choices(string.ascii_uppercase + string.digits, k=8))
        }

def to_record_batch(buffer):
    return pa.RecordBatch.from_arrays(
        [
            pa.array([r["ts"] for r in buffer], type=pa.int64()),
            pa.array([r["user_id"] for r in buffer], type=pa.int64()),
            pa.array([r["region"] for r in buffer], type=pa.dictionary(pa.int32(), pa.string())),
            pa.array([r["amount"] for r in buffer], type=pa.float32()),
            pa.array([r["sku"] for r in buffer], type=pa.string()),
        ],
        names=["ts", "user_id", "region", "amount", "sku"],
    )

buffer, last_checkpoint = [], time.time()
for event in source():
    buffer.append(event)
    if len(buffer) >= BATCH_ROWS or (time.time() - last_checkpoint) >= CHECKPOINT_SECONDS:
        record_batch = to_record_batch(buffer)
        table = pa.Table.from_batches([record_batch])

        # ETL in DuckDB, going Arrow → DuckDB zero-copy
        rel = con.from_arrow(table)
        transformed_arrow = con.execute("""
            SELECT
                to_timestamp(ts) AS ts,
                user_id,
                region::VARCHAR AS region,
                ROUND(amount, 2) AS amount,
                sku,
                -- exampe enhancement
                CASE
                    WHEN amount > 80.0 THEN 'high'
                    WHEN amount > 30.0 THEN 'medium'
                    ELSE 'low'
                END AS bucket
            FROM rel
        """).fetch_arrow_table() # back to arrow as Table

        # Durability: append to hourly partitioned Parquet
        hour_result = con.execute("SELECT strftime(CURRENT_TIMESTAMP, '%Y-%m-%d-%H')").fetchone()
        hour = hour_result[0] if hour_result else time.strftime('%Y-%m-%d-%H')
        pq.write_to_dataset(transformed_arrow, root_path=OUT_PATH, partition_cols=["region"], 
                            basename_template=f"events-{hour}-{{i}}.parquet")

        # Small, immediate aggregate for monitoring
        transformed_rel = con.from_arrow(transformed_arrow)
        quick = con.execute("""
            SELECT bucket, COUNT(*) AS n, AVG(amount) AS avg_amount
            FROM transformed_rel GROUP BY 1 ORDER BY n DESC
        """).pl()
        print(quick)

        buffer.clear()
        last_checkpoint = time.time()
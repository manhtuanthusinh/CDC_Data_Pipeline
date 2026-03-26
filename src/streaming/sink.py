from configs.settings import ClickHouseConfig
import time

def write_to_clickhouse(df, batch_id):

    start = time.time()
    count = df.count()

    if df.count() > 0:
        print(f"Writing {df.count()} rows to ClickHouse (Batch {batch_id})...")
        df.write \
            .format("jdbc") \
            .option("url", "jdbc:clickhouse://localhost:8123/inventory_dwh?jdbcCompliant=false") \
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
            .option("dbtable", "orders_warehouse") \
            .option("user", "default") \
            .option("password", "1111") \
            .mode("append") \
            .save()
        
        duration = time.time() - start
        print(f"[Batch {batch_id}] Finished writing {count} rows in {duration:.2f}s")

    else:
        print(f"Batch {batch_id} is empty. Skipping...")
from configs.settings import ClickHouseConfig

def write_to_clickhouse(df, batch_id):
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
    else:
        print(f"Batch {batch_id} is empty. Skipping...")
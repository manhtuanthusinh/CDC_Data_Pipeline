from pyspark.sql import functions as F

def transform_cdc_data(df, schema):
    # 1. Parse JSON
    parsed_df = df.select(
        F.from_json(F.col("value").cast("string"), schema).alias("data")
    )
    
    # 2. Flatten and Explicitly Alias
    final_df = parsed_df.select(
        F.col("data.id").cast("int").alias("id"),
        F.col("data.customer_id").cast("int").alias("customer_id"),
        F.col("data.product_name").alias("product_name"),
        F.col("data.price").cast("decimal(10,2)").alias("price"),
        F.col("data.status").alias("status"),
        # Use to_timestamp for ISO strings or division for ms; alias is mandatory
        F.to_timestamp(F.col("data.created_at")).alias("created_at")
    )
    
    # 3. Add the ingestion timestamp
    return final_df.withColumn("inserted_at", F.current_timestamp())
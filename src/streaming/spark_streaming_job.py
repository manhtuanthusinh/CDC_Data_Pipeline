# import logging
# import os
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json, col
# from pyspark.sql.types import LongType, TimestampType
# from configs.settings import KafkaConfig, SparkConfig, Schemas, ClickHouseConfig
# from pyspark.sql.functions import col, from_json

# # Configure logging to match repo standards
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# def get_spark_session(app_name: str) -> SparkSession:
#     """Creates a Spark session with necessary Kafka connectors."""
#     spark_settings = SparkConfig.get_conf()
    
#     # This finds the absolute path of the directory containing THIS script
#     script_dir = os.path.dirname(os.path.abspath(__file__))
    
#     # Adjust the number of '../' based on where this script is 
#     # (e.g., if script is in src/streaming/, one '../' gets you to src/, two gets you to root)
#     jar_path = os.path.join(script_dir, "../../jars/clickhouse-jdbc-all.jar")
#     jar_path = os.path.normpath(jar_path) # Cleans up the ../.. parts

#     print(f"DEBUG: Looking for ClickHouse JAR at: {jar_path}")

#     return SparkSession.builder \
#         .appName(app_name)      \
#         .config(conf=spark_settings) \
#         .config("spark.jars.packages", SparkConfig.KAFKA_JAR_PACKAGE) \
#         .config("spark.jars", jar_path) \
#         .getOrCreate()

# from pyspark.sql import functions as F

# from pyspark.sql import functions as F

# def transform_cdc_data(df, schema):
#     # 1. Parse JSON
#     parsed_df = df.select(
#         F.from_json(F.col("value").cast("string"), schema).alias("data")
#     )
    
#     # 2. Flatten and Explicitly Alias
#     final_df = parsed_df.select(
#         F.col("data.id").cast("int").alias("id"),
#         F.col("data.customer_id").cast("int").alias("customer_id"),
#         F.col("data.product_name").alias("product_name"),
#         F.col("data.price").cast("decimal(10,2)").alias("price"),
#         F.col("data.status").alias("status"),
#         # Use to_timestamp for ISO strings or division for ms; alias is mandatory
#         F.to_timestamp(F.col("data.created_at")).alias("created_at")
#     )
    
#     # 3. Add the ingestion timestamp
#     return final_df.withColumn("inserted_at", F.current_timestamp())


# def write_to_clickhouse(df, batch_id):
#     if df.count() > 0:
#         print(f"Writing {df.count()} rows to ClickHouse (Batch {batch_id})...")
#         df.write \
#             .format("jdbc") \
#             .option("url", "jdbc:clickhouse://localhost:8123/default?jdbcCompliant=false")  \
#             .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
#             .option("dbtable", "orders_warehouse") \
#             .option("user", "default") \
#             .option("password", "1111") \
#             .mode("append") \
#             .save()
#     else:
#         print(f"Batch {batch_id} is empty. Skipping...")


# def run_pipeline():
#     """Main execution flow for the ClickHouse streaming job."""
#     # Ensure SparkConfig.KAFKA_JAR_PACKAGE includes both Kafka and ClickHouse JARs
#     spark = get_spark_session(SparkConfig.APP_NAME)
    
#     try:
#         logger.info(f"Subscribing to Kafka Topic: {KafkaConfig.TOPICS}")
        
#         # 1. EXTRACT
#         raw_stream_df = spark.readStream \
#             .format("kafka") \
#             .option("kafka.bootstrap.servers", KafkaConfig.SERVERS) \
#             .option("subscribe", KafkaConfig.TOPICS) \
#             .option("startingOffsets", KafkaConfig.STARTING_OFFSETS) \
#             .option("failOnDataLoss", "false") \
#             .option("kafka.allow.auto.create.topics", "true") \
#             .load()

#         # 2. TRANSFORM
#         # Ensure transform_cdc_data is imported and handles your 'orders' schema
#         structured_df = transform_cdc_data(raw_stream_df, Schemas.ORDER_FLAT_SCHEMA)
#         # 3. LOAD (ClickHouse JDBC Sink)
#         # We use foreachBatch to handle the JDBC connection per micro-batch
#         query = structured_df.writeStream \
#             .foreachBatch(write_to_clickhouse) \
#             .option("checkpointLocation", SparkConfig.CHECKPOINT_DIR) \
#             .start()

#         logger.info("Streaming started. Awaiting data from Debezium...")
#         query.awaitTermination()
        
#     except Exception as e:
#         logger.error(f"Pipeline failed: {str(e)}")
#         raise
#     finally:
#         spark.stop()
    


# if __name__ == "__main__":
#     run_pipeline()
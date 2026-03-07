import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import LongType, TimestampType
from configs.settings import KafkaConfig, SparkConfig, Schemas, ClickHouseConfig


# Configure logging to match repo standards
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_spark_session(app_name: str, jar_package: str) -> SparkSession:
    """Creates a Spark session with necessary Kafka connectors."""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars.packages", jar_package) \
        .getOrCreate()


from pyspark.sql.functions import col, from_json

def transform_cdc_data(df, schema):
    # 1. Cast the Kafka binary 'value' to String
    raw_df = df.selectExpr("CAST(value AS STRING) as json_payload")
    
    # 2. FILTER: Ignore Schema Change/DDL messages (they contain a 'ddl' key)
    # We only want 'Data' messages (which usually have an 'after' key)
    data_only_df = raw_df.filter("json_payload LIKE '%\"after\"%'")
    
    # 3. Parse the JSON
    # NOTE: Your 'schema' must match the 'after' block structure
    parsed_df = data_only_df.select(
        from_json(col("json_payload"), schema).alias("payload")
    ).select("payload.after.*") # This reaches inside the CDC envelope
    
    # 4. Handle transformations on the extracted columns
    if "created_at" in parsed_df.columns:
        parsed_df = parsed_df.withColumn(
            "created_at", 
            (col("created_at").cast("long") / 1000000).cast("timestamp")
        )
        
    if "price" in parsed_df.columns:
        parsed_df = parsed_df.withColumn(
            "price", 
            col("price").cast("decimal(10,2)")
        )

    return parsed_df

def write_to_clickhouse(batch_df, batch_id):
    """
    Function called for every micro-batch of data.
    """
    if batch_df.count() > 0:
        logger.info(f"Writing micro-batch {batch_id} to ClickHouse...")

        try:
            batch_df.write \
                .format("jdbc") \
                .option("url", ClickHouseConfig.URL) \
                .option("dbtable", ClickHouseConfig.TABLE) \
                .option("user", ClickHouseConfig.USER) \
                .option("password", ClickHouseConfig.PASSWORD) \
                .option("driver", ClickHouseConfig.DRIVER) \
                .mode("append") \
                .save()
        except Exception as e:
            logger.error(f"Failed to write batch {batch_id} to ClickHouse: {e}")


def run_pipeline():
    """Main execution flow for the ClickHouse streaming job."""
    # Ensure SparkConfig.KAFKA_JAR_PACKAGE includes both Kafka and ClickHouse JARs
    spark = get_spark_session(SparkConfig.APP_NAME, SparkConfig.KAFKA_JAR_PACKAGE)
    
    try:
        logger.info(f"Subscribing to Kafka Topic: {KafkaConfig.TOPICS}")
        
        # 1. EXTRACT
        raw_stream_df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KafkaConfig.SERVERS) \
            .option("subscribe", KafkaConfig.TOPICS) \
            .option("startingOffsets", KafkaConfig.STARTING_OFFSETS) \
            .load()

        # 2. TRANSFORM
        # Ensure transform_cdc_data is imported and handles your 'orders' schema
        structured_df = transform_cdc_data(raw_stream_df, Schemas.ORDER_SCHEMA)

        # 3. LOAD (ClickHouse JDBC Sink)
        # We use foreachBatch to handle the JDBC connection per micro-batch
        query = structured_df.writeStream \
            .foreachBatch(write_to_clickhouse) \
            .option("checkpointLocation", SparkConfig.CHECKPOINT_DIR) \
            .start()

        logger.info("Streaming started. Awaiting data from Debezium...")
        query.awaitTermination()
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    run_pipeline()
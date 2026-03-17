import logging
from streaming.session import get_spark_session
from streaming.transformation import transform_cdc_data
from streaming.sink import write_to_clickhouse
from configs.settings import KafkaConfig, SparkConfig, Schemas

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_pipeline():
    spark = get_spark_session(SparkConfig.APP_NAME)
    try:
        # 1. EXTRACT
        raw_stream_df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KafkaConfig.SERVERS) \
            .option("subscribe", KafkaConfig.TOPICS) \
            .load()

        # 2. TRANSFORM
        structured_df = transform_cdc_data(raw_stream_df, Schemas.ORDER_FLAT_SCHEMA)

        # 3. LOAD
        query = structured_df.writeStream \
            .foreachBatch(write_to_clickhouse) \
            .option("checkpointLocation", SparkConfig.CHECKPOINT_DIR) \
            .start()

        query.awaitTermination()
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    run_pipeline()
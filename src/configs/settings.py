import os
from pathlib import Path
from dotenv import load_dotenv
from pyspark.conf import SparkConf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType

# Resolve path to .env in the project root
base_dir = Path(__file__).resolve().parent.parent.parent
load_dotenv(dotenv_path=base_dir / ".env")

class KafkaConfig:
    SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    TOPICS = os.getenv("KAFKA_TOPIC")
    STARTING_OFFSETS = os.getenv("KAFKA_STARTING_OFFSETS", "earliest")

    if not SERVERS or not TOPICS:
        raise EnvironmentError("KAFKA_BOOTSTRAP_SERVERS or KAFKA_TOPIC missing")
    
class SparkConfig:
    APP_NAME = "CDC_Inventory_Stream"
    CHECKPOINT_DIR = os.getenv("CHECKPOINT_PATH")
    KAFKA_JAR_PACKAGE = os.getenv("KAFKA_JAR_PACKAGE")
    CLICKHOUSE_JAR_PACKAGE = os.getenv("CLICKHOUSE_JAR_PACKAGE")

    @staticmethod
    def get_conf():
        conf = SparkConf()
        conf.setAppName(SparkConfig.APP_NAME)
        
        conf.set("spark.executor.memory", "4g")       
        conf.set("spark.executor.cores", "2")        
        conf.set("spark.driver.memory", "2g")         
        conf.set("spark.cores.max", "4")              
        
        # --- Performance & Streaming Optimization ---
        # Essential for CDC/Streaming to handle small files and shuffle partitions
        conf.set("spark.sql.shuffle.partitions", "20") 
        conf.set("spark.streaming.stopGracefullyOnShutdown", "true")
        
        # --- Memory Management ---
        # Adjusts how much RAM is used for execution vs. storage
        conf.set("spark.memory.fraction", "0.6")
        
        # --- Dynamic Allocation (Optional/Recommended) ---
        # Allows Spark to scale executors up/down based on workload
        # conf.set("spark.dynamicAllocation.enabled", "true")
        # conf.set("spark.dynamicAllocation.minExecutors", "1")
        # conf.set("spark.dynamicAllocation.maxExecutors", "10")

        return conf

class ClickHouseConfig:
    URL = os.getenv("CLICKHOUSE_URL")
    TABLE = "orders_warehouse"
    USER = os.getenv("CLICKHOUSE_USER")
    PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")
    DRIVER = "com.clickhouse.jdbc.ClickHouseDriver"

class Schemas:
    
    INTERNAL_ORDER_DATA = StructType([
        StructField("id", IntegerType(), False),
        StructField("customer_id", IntegerType(), True),
        StructField("product_name", StringType(), True),
        StructField("price", LongType(), True), # Debezium often sends decimals as longs/ints
        StructField("status", StringType(), True),
        StructField("created_at", LongType(), True) # Raw microseconds from DB
    ])

    ORDER_CDC_ENVELOPE = StructType([
        StructField("before", INTERNAL_ORDER_DATA, True),
        StructField("after", INTERNAL_ORDER_DATA, True),
        StructField("op", StringType(), True), 
        StructField("ts_ms", LongType(), True) # Timestamp of the event
    ])

    ORDER_FLAT_SCHEMA = StructType([
        StructField("id", IntegerType(), False),
        StructField("customer_id", IntegerType(), True),
        StructField("product_name", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("status", StringType(), True),
        StructField("created_at", StringType(), True) 
    ])
from pyspark.sql import SparkSession
from configs.settings import SparkConfig
import os

def get_spark_session(app_name: str) -> SparkSession:
    """Creates a Spark session with necessary Kafka connectors."""
    spark_settings = SparkConfig.get_conf()
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    jar_path = os.path.join(script_dir, "../../jars/clickhouse-jdbc-all.jar")
    jar_path = os.path.normpath(jar_path) 

    print(f"DEBUG: Looking for ClickHouse JAR at: {jar_path}")

    return SparkSession.builder \
        .appName(app_name)      \
        .config(conf=spark_settings) \
        .config("spark.jars.packages", SparkConfig.KAFKA_JAR_PACKAGE) \
        .config("spark.jars", jar_path) \
        .getOrCreate()

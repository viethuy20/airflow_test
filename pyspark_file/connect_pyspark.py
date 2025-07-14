import socket
from pyspark.sql import SparkSession
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_dataframe():
    try:
        driver_host = socket.gethostname()
        logger.info(f"Driver host: {driver_host}")
        spark = SparkSession.builder \
            .appName("Airflow Spark Job") \
            .master("spark://spark-master:7077") \
            .config("spark.driver.host", driver_host) \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .config("spark.logConf", "true") \
            .getOrCreate()
        logger.info(f"Spark version: {spark.version}")
        return spark
    except Exception as e:
        logger.error(f"Error creating Spark session: {e}", exc_info=True)
        raise
    
    
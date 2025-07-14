from airflow import DAG
from airflow.operators.python import PythonVirtualenvOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import pendulum

def spark_task():
    import logging
    import subprocess # Thư viện để chạy lệnh shell

    # --- Phần thêm vào để kiểm tra thư viện ---
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    logger.info("--- LIBS IN ISOLATED VENV ---")
    try:
        # Chạy lệnh 'pip freeze' từ bên trong code Python
        result = subprocess.run(['pip', 'freeze'], capture_output=True, text=True, check=True)
        # In kết quả ra log
        logger.info(result.stdout)
    except Exception as e:
        logger.error(f"Could not run 'pip freeze': {e}")
    logger.info("-----------------------------")
    # --- Kết thúc phần thêm vào ---
    from pyspark.sql import SparkSession
    import logging

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    try:
        spark = SparkSession.builder \
            .appName("Airflow Spark Job") \
            .master("spark://spark-master:7077") \
            .config("spark.driver.host", "172.19.0.6") \
            .config("spark.driver.bindAddress", "0.0.0.0") \
            .config("spark.ui.port", "4056") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .config("spark.logConf", "true") \
            .getOrCreate()
        logger.info(f"Spark version: {spark.version}")
        print(f"Spark version: {spark.version}")
        spark.stop()
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        raise

with DAG(
    'example_dagaa',
    start_date=pendulum.datetime(2025, 7, 8, tz="UTC"),
    schedule=None,
) as dag:
    spark_task_op = PythonOperator(
        task_id='print_hello',
        python_callable=spark_task,

   
    )
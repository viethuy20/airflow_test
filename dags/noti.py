from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from pyspark.sql import SparkSession

# --- CẤU HÌNH CHO KẾT NỐI ---
# Trong thực tế, bạn nên lấy các thông tin này từ Airflow Connections/Variables
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"
MINIO_BUCKET = "datalake"

MYSQL_HOST = "host.docker.internal"
MYSQL_PORT = "3306"
MYSQL_DATABASE = "sakila"
MYSQL_USER = "root"
MYSQL_PASSWORD = "root"
MYSQL_JDBC_URL = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"

# Phiên bản JARs, bạn có thể thay đổi nếu cần
MYSQL_DRIVER_VERSION = "8.0.28"
HADOOP_AWS_VERSION = "3.3.4"
AWS_SDK_VERSION = "1.12.515"

@dag(
    dag_id="etl_mysql_to_minio_spark",
    start_date=pendulum.datetime(2025, 7, 8, tz="UTC"),
    schedule=None,
    catchup=False,
    doc_md="DAG chuẩn để chạy ETL từ MySQL sang MinIO bằng Spark.",
)
def etl_mysql_to_minio_dag():
    @task
    def run_mysql_to_minio_spark_job() -> str:
        """
        Một task duy nhất để khởi chạy Spark job, làm toàn bộ công việc:
        1. Đọc từ MySQL.
        2. Ghi vào MinIO.
        """
        print("Initializing Spark Session with MySQL and S3 connectors...")

        # Cấu hình SparkSession với các gói cần thiết
        spark = SparkSession.builder \
            .appName("MySQL_to_MinIO_ETL") \
            .master("spark://spark-master:7077") \
            .config("spark.jars.packages", 
                    f"mysql:mysql-connector-java:{MYSQL_DRIVER_VERSION},"
                    f"org.apache.hadoop:hadoop-aws:{HADOOP_AWS_VERSION}") \
            .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
            .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.driver.memory", "512m") \
            .config("spark.executor.memory", "512m") \
            .config("spark.executor.cores", "1") \
            .config("spark.sql.shuffle.partitions", "1") \
            .getOrCreate()
        
        print("Spark Session created. Reading from MySQL...")

        # 1. Đọc dữ liệu từ MySQL
        mysql_df = spark.read \
            .format("jdbc") \
            .option("url", MYSQL_JDBC_URL) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("dbtable", "actor") \
            .option("user", MYSQL_USER) \
            .option("password", MYSQL_PASSWORD) \
            .load()
         
        print(f"Successfully read {mysql_df.count()} rows from MySQL.")
        
        # 2. Xử lý dữ liệu (ví dụ)
        processed_df = mysql_df.select("*")
        
        # 3. Ghi dữ liệu vào MinIO
        output_path = f"s3a://{MINIO_BUCKET}/data/processed/{pendulum.now().to_date_string()}"
        print(f"Writing data to MinIO path: {output_path}")
        
        processed_df.write.mode("overwrite").parquet(output_path)
        spark.stop()
        print("Write to MinIO completed. Stopping Spark session.")
        
        
        return output_path

    # Khởi chạy task
    run_mysql_to_minio_spark_job()

# Khởi tạo DAG
etl_mysql_to_minio_dag()
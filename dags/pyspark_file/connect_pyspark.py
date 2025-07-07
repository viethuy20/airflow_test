from pyspark.sql import SparkSession


def create_spark_session():
    """
    Create a Spark session with the necessary configurations.
    """
    spark = SparkSession.builder \
        .appName("Connect to Spark") \
        .master("local[*]") \
        .config("spark.driver.host", "host.docker.internal") \
        .getOrCreate()
    return spark

def create_dataframe(data=None, columns=None):
    """
    Create a sample DataFrame with some data.
    """
    spark = create_spark_session()
    if data :
        df = spark.createDataFrame(data, columns)
    else:   
        df = None
    return df


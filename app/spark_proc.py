from typing import List, Dict

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("sparkcomputejob") \
    .config("spark.driver.bindAddress", "localhost") \
    .getOrCreate()

def create_dataframe(data: List[Dict], schema=None):
    return spark.createDataFrame(data, schema=schema)


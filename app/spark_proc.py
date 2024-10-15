from typing import List, Dict

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("sparkcomputejob") \
    .getOrCreate()

def create_dataframe(data: List[Dict], schema=None):
    return spark.createDataFrame(data, schema=schema)


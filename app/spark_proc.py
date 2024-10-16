from typing import List, Dict

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType

spark = SparkSession.builder \
    .appName("sparkcomputejob") \
    .config("spark.driver.bindAddress", "localhost") \
    .getOrCreate()


def create_dataframe(data, schema):
    return spark.createDataFrame(data, schema=schema)


claims_schema = StructType([
    StructField("id", StringType(), True),
    StructField("ndc", StringType(), True),
    StructField("npi", StringType(), True),
    StructField("quantity", FloatType(), True),
    StructField("price", FloatType(), True),
    StructField("timestamp", StringType(), True)
])

pharmacies_schema = StructType([
    StructField("chain", StringType(), True),
    StructField("npi", StringType(), True)
])

reverts_schema = StructType([
    StructField("id", StringType(), True),
    StructField("claim_id", StringType(), True),
    StructField("timestamp", StringType(), True)
])

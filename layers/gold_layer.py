from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = (
    SparkSession.builder
    .appName("goldLayer")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

silverDf = spark.read.format("delta").load(
    "/Volumes/pyspark_pipeline/source/transdata/silver/orders/"
)

goldDf = (
    silverDf.groupBy("customer_id")
    .agg(count("*").alias("total_orders"))
    .withColumn("processed_at", current_timestamp())
)

goldDf.write.format("delta").mode("append").save(
    "/Volumes/pyspark_pipeline/source/transdata/gold/customers"
)

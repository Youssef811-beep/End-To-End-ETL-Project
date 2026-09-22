from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = (
    SparkSession.builder
    .appName("silver_layer")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

bronze_data = spark.read.format("delta").load(
    "/Volumes/pyspark_pipeline/source/transdata/bronze/orders/"
)

silver_data = (
    bronze_data
    .dropDuplicates(["order_id"])
    .filter(col("order_id").isNotNull())
    .withColumn("order_timestamp", to_timestamp("order_timestamp"))
    .withColumn(
        "order_status_clean",
        when(
            col("order_status").isin("shipped", "canceled", "pending"),
            col("order_status")
        ).otherwise("unknown")
    )
    .withColumn("processed_at", current_timestamp())
)

silver_data.write.format("delta").mode("overwrite").save(
    "/Volumes/pyspark_pipeline/source/transdata/silver/orders"
)

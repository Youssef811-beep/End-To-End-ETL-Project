from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp
import uuid

spark = (
    SparkSession.builder
    .appName("bronze_layer1")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

batch_id = str(uuid.uuid4())

orders_df = (
    spark.read.options(header=True, inferSchema=True)
    .csv("/Volumes/pyspark_pipeline/source/transdata/bronze/orders_practice_400_rows.csv")
    .withColumn("batch_id", lit(batch_id))
    .withColumn("source_file", lit("orders_practice_400_rows.csv"))
    .withColumn("ingesttime", current_timestamp())
)

display(orders_df)

orders_df.write.format("delta").mode("overwrite").save(
    "/Volumes/pyspark_pipeline/source/transdata/bronze/orders/"
)

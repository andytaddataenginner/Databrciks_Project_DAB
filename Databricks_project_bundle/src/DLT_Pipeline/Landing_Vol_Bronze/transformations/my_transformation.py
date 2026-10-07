from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col,
    explode,
    current_timestamp,
    trim,
    lower,
    regexp_replace,
    when,
    lit,
    to_date,
    round
)
# volume path
VOLUME_PATH = "/Volumes/youtube_dev/bronze/landing"

@dp.table(
    name="landing_bronze_products",
    comment="Raw product data ingested from product.json"
)
def bronze_products():

    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("multiLine", "true")
            .option("pathGlobFilter", "Product.json")
            .load(VOLUME_PATH)
            .withColumn("_ingestion_timestamp", current_timestamp())
            .withColumn("_source_file", col("_metadata.file_path"))
    )
@dp.table(
    name="landing_bronze_customers",
    comment="Raw customer data ingested from customer.json"
)
def bronze_customers():

    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("multiLine", "true")
            .option("pathGlobFilter", "customer.json")
            .load(VOLUME_PATH)
            .withColumn("_ingestion_timestamp", current_timestamp())
            .withColumn("_source_file", col("_metadata.file_path"))
    )

@dp.table(
    name="landing_bronze_sales",
    comment="Raw sales data ingested from sales_data.json"
)
def bronze_sales():

    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("multiLine", "true")
            .option("pathGlobFilter", "Sales_Data.json")
            .load(VOLUME_PATH)
            .withColumn("_ingestion_timestamp", current_timestamp())
            .withColumn("_source_file", col("_metadata.file_path"))
    )

    






import logging
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)
from src.pyspark.spark_session import get_spark_session

# Strict CSV schema specification matching the source data
CSV_SCHEMA = StructType([
    StructField("title", StringType(), True),
    StructField("magnitude", DoubleType(), True),
    StructField("date_time", StringType(), True),
    StructField("cdi", DoubleType(), True),
    StructField("mmi", DoubleType(), True),
    StructField("alert", StringType(), True),
    StructField("tsunami", IntegerType(), True),
    StructField("sig", IntegerType(), True),
    StructField("net", StringType(), True),
    StructField("nst", IntegerType(), True),
    StructField("dmin", DoubleType(), True),
    StructField("gap", DoubleType(), True),
    StructField("magType", StringType(), True),
    StructField("depth", DoubleType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("location", StringType(), True),
    StructField("continent", StringType(), True),
    StructField("country", StringType(), True),
])

def process_bronze(source_path: str, bronze_path: str):
    # Ingests raw earthquake CSV data, appends ingestion timestamp,
    # and saves the dataset as Parquet in the Bronze layer.
    # Partitioned by ingest_date for data retention and auditing.
    logging.info(f"Ingesting raw CSV data from {source_path}")
    
    spark = get_spark_session()
    
    # Read raw CSV using the defined schema
    df = spark.read \
        .option("header", "true") \
        .option("quote", "\"") \
        .option("escape", "\"") \
        .schema(CSV_SCHEMA) \
        .csv(source_path)
    
    row_count = df.count()
    logging.info(f"Loaded {row_count} records from source CSV.")
        
    # Append ingestion metadata
    df = df.withColumn("ingest_timestamp", F.current_timestamp())
    df = df.withColumn("ingest_date", F.current_date())
    
    # Write to bronze parquet path, partitioned by ingest_date for data retention
    logging.info(f"Writing parquet to {bronze_path} (partitioned by ingest_date)")
    df.write.mode("append").partitionBy("ingest_date").parquet(bronze_path)
    
    logging.info(f"Successfully processed Bronze data. {row_count} records ingested.")



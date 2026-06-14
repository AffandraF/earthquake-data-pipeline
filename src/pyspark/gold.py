import logging
from pyspark.sql import functions as F
from src.pyspark.spark_session import get_spark_session
from src.pyspark.postgres_loader import save_postgres, parse_db_con_str
from src.pyspark.schemas import (
    get_gold_col_names, get_gold_cast_map,
    get_country_stats_col_names, get_country_stats_cast_map,
    DATA_KEY_COLS
)

def read_silver(con_str: str, table_name: str):
    # Reads Silver table from PostgreSQL using Spark JDBC.
    spark = get_spark_session()
    jdbc_url, user, password = parse_db_con_str(con_str)
    
    logging.info(f"Reading Silver data from PostgreSQL table: {table_name}")
    df = spark.read.format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("user", user) \
        .option("password", password) \
        .option("driver", "org.postgresql.Driver") \
        .load()
    return df

def process_gold(con_str: str, silver_table: str, gold_table: str):
    # Gold stage: categorizes magnitude, generates country-level statistics,
    # and upserts the output into gold.gold_data and gold.country_stats tables.
    logging.info(f"Starting Gold ETL process. Input table: {silver_table}, Output table: {gold_table}")
    
    # Read data from Silver layer
    df = read_silver(con_str, silver_table)
    initial_count = df.count()
    logging.info(f"Loaded {initial_count} records from Silver layer.")
    
    # Classify earthquakes based on magnitude scale
    df = df.withColumn(
        'mag_class',
        F.when(F.col('magnitude') < 3.0, 'Minor')
         .when((F.col('magnitude') >= 3.0) & (F.col('magnitude') < 5.0), 'Light')
         .when((F.col('magnitude') >= 5.0) & (F.col('magnitude') < 7.0), 'Moderate')
         .otherwise('Strong')
    )
    
    # Generate aggregate metrics per country
    df_stats = df.groupBy('country').agg(
        F.count('magnitude').alias('eq_count'),
        F.round(F.mean('magnitude'), 2).alias('avg_magnitude'),
        F.max('magnitude').alias('max_magnitude'),
        F.min('magnitude').alias('min_magnitude'),
        F.sum('tsunami').alias('tsunami_count')
    )
    
    # Select and cast columns using centralized schema definitions
    gold_cols = get_gold_col_names()
    gold_cast = get_gold_cast_map()
    df_gold = df.select(*[
        F.col(col).cast(gold_cast[col]) if col != "date_time" else F.col(col)
        for col in gold_cols
    ])
    
    # Cast country_stats columns using centralized schema definitions
    stats_cols = get_country_stats_col_names()
    stats_cast = get_country_stats_cast_map()
    df_stats = df_stats.select(*[
        F.col(col).cast(stats_cast[col]) for col in stats_cols
    ])
    
    # Upsert data to gold tables
    logging.info(f"Upserting data into Gold table: {gold_table}")
    save_postgres(df_gold, con_str, gold_table, key_cols=DATA_KEY_COLS)
    
    logging.info("Upserting statistics into country stats table: gold.country_stats")
    save_postgres(df_stats, con_str, "gold.country_stats", key_cols=["country"])
    
    logging.info("Gold layer processing completed successfully.")

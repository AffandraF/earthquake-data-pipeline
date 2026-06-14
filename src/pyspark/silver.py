import logging
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType
)
from src.pyspark.spark_session import get_spark_session
from src.pyspark.postgres_loader import save_postgres
from src.pyspark.schemas import get_silver_col_names, get_silver_cast_map, DATA_KEY_COLS

# Schema for UDF return
ENRICH_SCHEMA = StructType([
    StructField("continent", StringType(), True),
    StructField("country", StringType(), True)
])

@F.udf(returnType=ENRICH_SCHEMA)
def enrich_location_udf(lat, lon, continent, country):
    # Spark UDF that enriches missing continent and country values
    # using coordinates (latitude and longitude).
    # If both continent and country are already present, return them
    if continent is not None and country is not None:
        return (continent, country)
        
    if lat is None or lon is None:
        return (continent, country)
        
    try:
        # Import libraries inside worker process to avoid serialization issues
        import reverse_geocoder as rg
        import pycountry_convert as pc
        
        # Lookup location by coordinates
        results = rg.search([(float(lat), float(lon))], mode=1)
        country_code = results[0]['cc']
        
        # Timor Leste edge case
        if country_code == 'TL':
            return ('Oceania', 'Timor Leste')
            
        continent_code = pc.country_alpha2_to_continent_code(country_code)
        country_name = pc.country_alpha2_to_country_name(country_code)
        return (continent_code, country_name)
    except Exception:
        # Return None, None on failures to match the pandas pipeline behavior
        return (None, None)

def process_silver(bronze_path: str, con_str: str, silver_table: str):
    # Silver transformation stage: cleans, deduplicates, formats dates,
    # enriches missing locations, and inserts the data into silver.silver_data.
    logging.info(f"Processing Silver layer. Reading from {bronze_path}")
    spark = get_spark_session()
    
    # Read Parquet from Bronze
    df = spark.read.parquet(bronze_path)
    initial_count = df.count()
    logging.info(f"Loaded {initial_count} records from Bronze layer.")
    
    # Rename magType to mag_type
    df = df.withColumnRenamed("magType", "mag_type")
    
    # Standardize column names (lowercase and replacing spaces with underscores)
    cleaned_columns = [col.lower().replace(' ', '_') for col in df.columns]
    df = df.toDF(*cleaned_columns)
    
    # Deduplicate on keys
    df = df.dropDuplicates(subset=['magnitude', 'date_time', 'latitude', 'longitude'])
    dedup_count = df.count()
    logging.info(f"Deduplicated records from {initial_count} to {dedup_count} (dropped {initial_count - dedup_count}).")
    
    # Drop rows where critical fields are null
    df = df.dropna(subset=['magnitude', 'date_time', 'latitude', 'longitude'])
    non_null_count = df.count()
    logging.info(f"Filtered out null keys: {dedup_count - non_null_count} records dropped.")
    
    # Validate magnitude (magnitude must be > 0)
    df = df.filter(F.col("magnitude") > 0)
    valid_magnitude_count = df.count()
    logging.info(f"Filtered invalid magnitudes (<= 0): {non_null_count - valid_magnitude_count} records dropped. Remaining: {valid_magnitude_count}")
    
    if valid_magnitude_count == 0:
        raise ValueError("All records filtered out — aborting Silver processing")
    
    # Standardize format date_time from dd-MM-yyyy HH:mm to yyyy-MM-dd HH:mm
    df = df.withColumn(
        "date_time",
        F.date_format(
            F.to_timestamp(F.col("date_time"), "dd-MM-yyyy HH:mm"),
            "yyyy-MM-dd HH:mm"
        )
    )
    
    # Fill null alert values with 'not available'
    df = df.fillna({"alert": "not available"})
    
    # Optimize UDF execution by only running geocoding on records missing continent/country
    df_needs_enrich = df.filter(F.col("continent").isNull() | F.col("country").isNull())
    df_already_enriched = df.filter(F.col("continent").isNotNull() & F.col("country").isNotNull())
    
    needs_enrich_count = df_needs_enrich.count()
    logging.info(f"Records needing location enrichment: {needs_enrich_count}")
    
    if needs_enrich_count > 0:
        df_enriched = df_needs_enrich.withColumn(
            "enriched",
            enrich_location_udf(F.col("latitude"), F.col("longitude"), F.col("continent"), F.col("country"))
        )
        df_enriched = df_enriched.withColumn("continent", F.col("enriched.continent"))
        df_enriched = df_enriched.withColumn("country", F.col("enriched.country"))
        df_enriched = df_enriched.drop("enriched")
        df = df_already_enriched.unionByName(df_enriched)
    else:
        df = df_already_enriched
    
    # Map continent abbreviation/codes to full names using create_map for clarity and performance
    from itertools import chain
    continent_mapping = {
        'AF': 'Africa',
        'AS': 'Asia',
        'EU': 'Europe',
        'NA': 'North America',
        'OC': 'Oceania',
        'SA': 'South America',
        'AN': 'Antarctica'
    }
    
    mapping_pairs = list(chain.from_iterable(
        (F.lit(k), F.lit(v)) for k, v in continent_mapping.items()
    ))
    continent_map = F.create_map(*mapping_pairs)
    df = df.withColumn(
        "continent",
        F.coalesce(continent_map[F.col("continent")], F.col("continent"))
    )
    
    # Convert date_time to TimestampType to align with PostgreSQL DB Schema
    df_db = df.withColumn("date_time", F.to_timestamp(F.col("date_time"), "yyyy-MM-dd HH:mm"))
    
    # Select and cast columns using centralized schema definitions
    schema_cols = get_silver_col_names()
    cast_map = get_silver_cast_map()
    df_db = df_db.select(*[
        F.col(col).cast(cast_map[col]) if col != "date_time" else F.col(col)
        for col in schema_cols
    ])
    
    logging.info(f"Writing Silver data into PostgreSQL target table: {silver_table}")
    save_postgres(df_db, con_str, silver_table, key_cols=DATA_KEY_COLS)
    logging.info("Silver layer processing completed successfully.")

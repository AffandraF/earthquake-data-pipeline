# Databricks Notebook Template: Earthquake Data Pipeline (PySpark)
# 
# This file can be imported into a Databricks Notebook to execute the PySpark ETL pipeline.
#
# STEP 1: Install required external libraries for coordinate-based location enrichment
# (Execute this in the first cell of your Databricks Notebook)
# 
# %pip install reverse_geocoder pycountry-convert psycopg2-binary
#
# STEP 2: Restart Python execution context to load the installed libraries
# (Execute this in the second cell if using %pip)
#
# dbutils.library.restartPython()

import logging
from src.pyspark.bronze import process_bronze
from src.pyspark.silver import process_silver
from src.pyspark.gold import process_gold

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# STEP 3: Configure widgets for pipeline parameters
# (This allows running the pipeline with different inputs dynamically)
dbutils.widgets.text("step", "all", "ETL Step (bronze | silver | gold | all)")
dbutils.widgets.text("source_path", "/dbfs/FileStore/earthquake_data.csv", "Source CSV Path")
dbutils.widgets.text("bronze_path", "/dbfs/FileStore/bronze/earthquake_data.parquet", "Bronze Parquet Path")
dbutils.widgets.text("con_str", "postgresql://eq_user:eq_pass@<db-host>:5432/eq_db", "SQLAlchemy Postgres URL")
dbutils.widgets.text("silver_table", "silver.silver_data", "Silver Table Name")
dbutils.widgets.text("gold_table", "gold.gold_data", "Gold Table Name")

# Read parameter values from widgets
step = dbutils.widgets.get("step")
source_path = dbutils.widgets.get("source_path")
bronze_path = dbutils.widgets.get("bronze_path")
con_str = dbutils.widgets.get("con_str")
silver_table = dbutils.widgets.get("silver_table")
gold_table = dbutils.widgets.get("gold_table")

logging.info(f"Running Databricks job for step: {step}")

# Run ETL Stages
try:
    if step in ["bronze", "all"]:
        logging.info("Executing Bronze Layer...")
        process_bronze(source_path, bronze_path)
        
    if step in ["silver", "all"]:
        logging.info("Executing Silver Layer...")
        process_silver(bronze_path, con_str, silver_table)
        
    if step in ["gold", "all"]:
        logging.info("Executing Gold Layer...")
        process_gold(con_str, silver_table, gold_table)
        
    logging.info("Pipeline executed successfully on Databricks!")
except Exception as e:
    logging.error(f"Pipeline execution failed: {e}")
    raise e

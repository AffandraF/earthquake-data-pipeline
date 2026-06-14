import os
import sys
import logging
from urllib.parse import quote_plus
from src.pyspark.bronze import process_bronze
from src.pyspark.silver import process_silver
from src.pyspark.gold import process_gold

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Hardcoded parameters
STEP = "all"
SOURCE_PATH = "./data/earthquake_data.csv"
BRONZE_PATH = "./data/bronze/earthquake_data.parquet"
SILVER_TABLE = "silver.silver_data"
GOLD_TABLE = "gold.gold_data"

def main():
    # Construct database connection string from environment variables.
    # Password is URL-encoded to handle special characters safely.
    user = os.getenv('POSTGRES_USER', 'eq_user')
    password = os.getenv('POSTGRES_PASSWORD', 'eq_pass')
    db = os.getenv('POSTGRES_DB', 'eq_db')
    port = os.getenv('POSTGRES_PORT', '5432')
    host = os.getenv('POSTGRES_HOST', 'localhost')
    con_str = f"postgresql://{quote_plus(user)}:{quote_plus(password)}@{host}:{port}/{db}"

    # Log startup without exposing credentials
    masked_con_str = f"postgresql://{user}:****@{host}:{port}/{db}"
    logging.info(f"Starting PySpark ETL pipeline. Step: {STEP}, DB: {masked_con_str}")
    
    try:
        if STEP in ["bronze", "all"]:
            process_bronze(SOURCE_PATH, BRONZE_PATH)
            
        if STEP in ["silver", "all"]:
            process_silver(BRONZE_PATH, con_str, SILVER_TABLE)
            
        if STEP in ["gold", "all"]:
            process_gold(con_str, SILVER_TABLE, GOLD_TABLE)
            
        logging.info(f"ETL pipeline step '{STEP}' completed successfully.")
    except Exception as e:
        logging.error(f"ETL pipeline execution failed at step '{STEP}': {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

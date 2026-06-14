import logging
from pyspark.sql import SparkSession

def _is_databricks():
    # Detects if the code is running inside a Databricks environment
    # by attempting to import the Databricks-specific DBUtils module.
    try:
        from pyspark.dbutils import DBUtils
        return True
    except ImportError:
        return SparkSession.getActiveSession() is not None

def get_spark_session(app_name="EarthquakeDataPipeline"):
    # Returns an active SparkSession. If running in a local environment,
    # initializes a new SparkSession configured to load the PostgreSQL JDBC driver.
    # Check if running inside Databricks
    if _is_databricks():
        logging.info("Running in Databricks environment. Reusing active SparkSession.")
        return SparkSession.builder.getOrCreate()
    else:
        logging.info("Running in local environment. Building new SparkSession with PostgreSQL JDBC driver dependency.")
        return SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.session.timeZone", "UTC") \
            .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
            .getOrCreate()

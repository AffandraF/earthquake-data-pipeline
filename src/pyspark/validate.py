# TODO: Not tested yet
import os
import sys
import logging
from pyspark.sql import functions as F
from src.pyspark.spark_session import get_spark_session
from src.pyspark.postgres_loader import parse_db_con_str

# Import legacy Pandas stages
from src.pandas.bronze import process_bronze as pandas_bronze
from src.pandas.silver import process_silver as pandas_silver
from src.pandas.gold import process_gold as pandas_gold

# Import PySpark stages
from src.pyspark.bronze import process_bronze as spark_bronze
from src.pyspark.silver import process_silver as spark_silver
from src.pyspark.gold import process_gold as spark_gold

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)


def compare_dataframes(df1, df2, df1_name, df2_name, join_keys):
    # Compares two Spark DataFrames for schema, row count, null counts,
    # and row-level content equivalence.
    print(f"\n================ Comparing {df1_name} vs {df2_name} ================")

    # 1. Compare Row Count
    count1 = df1.count()
    count2 = df2.count()
    print(f"Row Count: {df1_name} = {count1} | {df2_name} = {count2}")
    if count1 != count2:
        print(f"WARNING: Row counts do not match! Difference: {abs(count1 - count2)}")
    else:
        print("SUCCESS: Row counts match.")

    # 2. Compare Schema
    schema1 = {f.name: f.dataType.simpleString() for f in df1.schema}
    schema2 = {f.name: f.dataType.simpleString() for f in df2.schema}

    schema_diff = False
    for col in set(schema1.keys()).union(schema2.keys()):
        t1 = schema1.get(col, "MISSING")
        t2 = schema2.get(col, "MISSING")
        if t1 != t2:
            print(
                f"WARNING: Schema mismatch for column '{col}': {df1_name} = {t1} | {df2_name} = {t2}"
            )
            schema_diff = True

    if not schema_diff:
        print("SUCCESS: Column names and types match.")

    # 3. Compare Null Counts
    null_counts1 = (
        df1.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df1.columns])
        .collect()[0]
        .asDict()
    )
    null_counts2 = (
        df2.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df2.columns])
        .collect()[0]
        .asDict()
    )

    null_diff = False
    for col in df1.columns:
        nc1 = null_counts1.get(col, 0)
        nc2 = null_counts2.get(col, 0)
        if nc1 != nc2:
            print(
                f"WARNING: Null count mismatch for '{col}': {df1_name} = {nc1} | {df2_name} = {nc2}"
            )
            null_diff = True
    if not null_diff:
        print("SUCCESS: Null counts match across all columns.")

    # 4. Compare Content
    # Align schemas by selecting only common columns in alphabetical order
    common_cols = sorted(list(set(df1.columns).intersection(df2.columns)))

    df1_select = df1.select(*common_cols)
    df2_select = df2.select(*common_cols)

    diff_df1_df2 = df1_select.exceptAll(df2_select)
    diff_df2_df1 = df2_select.exceptAll(df1_select)

    diff_count1 = diff_df1_df2.count()
    diff_count2 = diff_df2_df1.count()

    if diff_count1 == 0 and diff_count2 == 0:
        print("SUCCESS: Dataset content is identical.")
    else:
        print(f"WARNING: Content mismatch detected!")
        print(f"Rows in {df1_name} not in {df2_name}: {diff_count1}")
        if diff_count1 > 0:
            diff_df1_df2.show(5, truncate=False)
        print(f"Rows in {df2_name} not in {df1_name}: {diff_count2}")
        if diff_count2 > 0:
            diff_df2_df1.show(5, truncate=False)


def run_validation():
    # Setup paths and tables
    base_data_path = "./data"
    source_csv = os.path.join(base_data_path, "earthquake_data.csv")

    # Pandas paths and tables
    pandas_bronze_path = os.path.join(base_data_path, "bronze_pandas.parquet")
    pandas_silver_table = "silver.silver_data_pandas"
    pandas_gold_table = "gold.gold_data_pandas"

    # PySpark paths and tables
    spark_bronze_path = os.path.join(base_data_path, "bronze_pyspark.parquet")
    spark_silver_table = "silver.silver_data_pyspark"
    spark_gold_table = "gold.gold_data_pyspark"

    # Database Connection
    user = os.getenv("POSTGRES_USER", "eq_user")
    password = os.getenv("POSTGRES_PASSWORD", "eq_pass")
    db = os.getenv("POSTGRES_DB", "eq_db")
    port = os.getenv("POSTGRES_PORT", "5432")
    host = os.getenv("POSTGRES_HOST", "postgres")
    con_str = f"postgresql://{user}:{password}@{host}:{port}/{db}"

    print("\n" + "=" * 50)
    print("RUNNING PANDAS PIPELINE")
    print("=" * 50)
    pandas_bronze(source_csv, pandas_bronze_path)
    pandas_silver(pandas_bronze_path, con_str, pandas_silver_table)
    pandas_gold(con_str, pandas_silver_table, pandas_gold_table)

    print("\n" + "=" * 50)
    print("RUNNING PYSPARK PIPELINE")
    print("=" * 50)
    spark_bronze(source_csv, spark_bronze_path)
    spark_silver(spark_bronze_path, con_str, spark_silver_table)
    spark_gold(con_str, spark_silver_table, spark_gold_table)

    # Compare results
    spark = get_spark_session()
    jdbc_url, db_user, db_password = parse_db_con_str(con_str)

    def load_table(table):
        return (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", table)
            .option("user", db_user)
            .option("password", db_password)
            .option("driver", "org.postgresql.Driver")
            .load()
        )

    # Load Tables
    silver_pd = load_table(pandas_silver_table)
    silver_sp = load_table(spark_silver_table)

    gold_pd = load_table(pandas_gold_table)
    gold_sp = load_table(spark_gold_table)

    stats_pd = load_table("gold.country_stats_pandas")
    stats_sp = load_table("gold.country_stats_pyspark")

    # Run Comparisons
    compare_dataframes(
        silver_pd,
        silver_sp,
        "silver_pandas",
        "silver_pyspark",
        ["magnitude", "date_time", "latitude", "longitude"],
    )
    compare_dataframes(
        gold_pd,
        gold_sp,
        "gold_pandas",
        "gold_pyspark",
        ["magnitude", "date_time", "latitude", "longitude"],
    )
    compare_dataframes(stats_pd, stats_sp, "stats_pandas", "stats_pyspark", ["country"])


if __name__ == "__main__":
    run_validation()

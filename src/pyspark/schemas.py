from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)

# ─── Column Definitions ────────────────────────────────────────────────────────
# Each tuple: (column_name, spark_type, postgres_type, nullable)

SILVER_COLUMNS = [
    ("title",     StringType(),  "text",      True),
    ("magnitude", DoubleType(),  "float",     True),
    ("date_time", StringType(),  "timestamp", True),  # cast to timestamp before DB write
    ("cdi",       DoubleType(),  "float",     True),
    ("mmi",       DoubleType(),  "float",     True),
    ("alert",     StringType(),  "text",      True),
    ("tsunami",   IntegerType(), "int",       True),
    ("sig",       IntegerType(), "int",       True),
    ("net",       StringType(),  "text",      True),
    ("nst",       IntegerType(), "int",       True),
    ("dmin",      DoubleType(),  "float",     True),
    ("gap",       DoubleType(),  "float",     True),
    ("mag_type",  StringType(),  "text",      True),
    ("depth",     DoubleType(),  "float",     True),
    ("latitude",  DoubleType(),  "float",     True),
    ("longitude", DoubleType(),  "float",     True),
    ("location",  StringType(),  "text",      True),
    ("continent", StringType(),  "text",      True),
    ("country",   StringType(),  "text",      True),
]

GOLD_EXTRA_COLUMNS = [
    ("mag_class", StringType(),  "text",      True),
]

COUNTRY_STATS_COLUMNS = [
    ("country",        StringType(),  "text",  False),
    ("eq_count",       IntegerType(), "int",   True),
    ("avg_magnitude",  DoubleType(),  "float", True),
    ("max_magnitude",  DoubleType(),  "float", True),
    ("min_magnitude",  DoubleType(),  "float", True),
    ("tsunami_count",  IntegerType(), "int",   True),
]

# Composite primary key columns for Silver and Gold data tables
DATA_KEY_COLS = ["magnitude", "date_time", "latitude", "longitude"]

# ─── Derived Helpers ────────────────────────────────────────────────────────────

def get_silver_col_names():
    """Returns ordered list of Silver column names."""
    return [col[0] for col in SILVER_COLUMNS]

def get_gold_col_names():
    """Returns ordered list of Gold column names (Silver + mag_class)."""
    return get_silver_col_names() + [col[0] for col in GOLD_EXTRA_COLUMNS]

def get_country_stats_col_names():
    """Returns ordered list of country_stats column names."""
    return [col[0] for col in COUNTRY_STATS_COLUMNS]

def _spark_type_to_cast(spark_type):
    """Maps PySpark type to cast string."""
    type_map = {
        StringType():  "string",
        DoubleType():  "double",
        IntegerType(): "integer",
    }
    return type_map.get(spark_type, "string")

def get_silver_cast_map():
    """Returns dict of {col_name: cast_type} for Silver columns."""
    return {col[0]: _spark_type_to_cast(col[1]) for col in SILVER_COLUMNS}

def get_gold_cast_map():
    """Returns dict of {col_name: cast_type} for Gold columns."""
    silver = get_silver_cast_map()
    gold_extra = {col[0]: _spark_type_to_cast(col[1]) for col in GOLD_EXTRA_COLUMNS}
    return {**silver, **gold_extra}

def get_country_stats_cast_map():
    """Returns dict of {col_name: cast_type} for country_stats columns."""
    return {col[0]: _spark_type_to_cast(col[1]) for col in COUNTRY_STATS_COLUMNS}

def _generate_pg_ddl(schema_name, table_name, columns, pk_cols):
    """Generates PostgreSQL CREATE TABLE DDL from column definitions."""
    col_defs = []
    for col_name, _, pg_type, _ in columns:
        col_defs.append(f"            {col_name} {pg_type}")
    
    pk_clause = f"            PRIMARY KEY ({', '.join(pk_cols)})"
    col_defs.append(pk_clause)
    
    cols_str = ",\n".join(col_defs)
    return f"""
        CREATE SCHEMA IF NOT EXISTS {schema_name};
        CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
{cols_str}
        );
        """

def generate_table_ddl(table_name):
    """Generates DDL for a given fully-qualified table name."""
    if table_name == 'silver.silver_data':
        return _generate_pg_ddl('silver', 'silver_data', SILVER_COLUMNS, DATA_KEY_COLS)
    elif table_name == 'gold.gold_data':
        return _generate_pg_ddl('gold', 'gold_data', SILVER_COLUMNS + GOLD_EXTRA_COLUMNS, DATA_KEY_COLS)
    elif table_name == 'gold.country_stats':
        return _generate_pg_ddl('gold', 'country_stats', COUNTRY_STATS_COLUMNS, ['country'])
    else:
        raise ValueError(f"Unknown table name: {table_name}")

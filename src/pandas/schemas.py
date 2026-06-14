# Schemas for the pandas version of the earthquake data pipeline.
# Defines column names, pandas types, and DDL mappings.

SILVER_COLUMNS = [
    ("title", "string", "text"),
    ("magnitude", "float64", "float"),
    ("date_time", "datetime64[ns]", "timestamp"),
    ("cdi", "float64", "float"),
    ("mmi", "float64", "float"),
    ("alert", "string", "text"),
    ("tsunami", "Int64", "int"),
    ("sig", "Int64", "int"),
    ("net", "string", "text"),
    ("nst", "Int64", "int"),
    ("dmin", "float64", "float"),
    ("gap", "float64", "float"),
    ("mag_type", "string", "text"),
    ("depth", "float64", "float"),
    ("latitude", "float64", "float"),
    ("longitude", "float64", "float"),
    ("location", "string", "text"),
    ("continent", "string", "text"),
    ("country", "string", "text"),
]

GOLD_EXTRA_COLUMNS = [
    ("mag_class", "string", "text"),
]

COUNTRY_STATS_COLUMNS = [
    ("country", "string", "text"),
    ("eq_count", "Int64", "int"),
    ("avg_magnitude", "float64", "float"),
    ("max_magnitude", "float64", "float"),
    ("min_magnitude", "float64", "float"),
    ("tsunami_count", "Int64", "int"),
]

DATA_KEY_COLS = ["magnitude", "date_time", "latitude", "longitude"]

def get_silver_types():
    return {col[0]: col[1] for col in SILVER_COLUMNS}

def get_gold_types():
    types = get_silver_types()
    for col_name, pd_type, _ in GOLD_EXTRA_COLUMNS:
        types[col_name] = pd_type
    return types

def get_country_stats_types():
    return {col[0]: col[1] for col in COUNTRY_STATS_COLUMNS}

def get_silver_col_names():
    return [col[0] for col in SILVER_COLUMNS]

def get_gold_col_names():
    return get_silver_col_names() + [col[0] for col in GOLD_EXTRA_COLUMNS]

def get_country_stats_col_names():
    return [col[0] for col in COUNTRY_STATS_COLUMNS]

def _generate_pg_ddl(schema_name, table_name, columns, pk_cols):
    col_defs = []
    for col_name, _, pg_type in columns:
        col_defs.append(f"            {col_name} {pg_type}")
    pk_clause = f"            PRIMARY KEY ({', '.join(pk_cols)})"
    col_defs.append(pk_clause)
    cols_str = ",\n".join(col_defs)
    return f"CREATE SCHEMA IF NOT EXISTS {schema_name};\nCREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (\n{cols_str}\n);"

def generate_table_ddl(table_name):
    if table_name == "silver.silver_data":
        return _generate_pg_ddl("silver", "silver_data", SILVER_COLUMNS, DATA_KEY_COLS)
    elif table_name == "gold.gold_data":
        return _generate_pg_ddl("gold", "gold_data", SILVER_COLUMNS + GOLD_EXTRA_COLUMNS, DATA_KEY_COLS)
    elif table_name == "gold.country_stats":
        return _generate_pg_ddl("gold", "country_stats", COUNTRY_STATS_COLUMNS, ["country"])
    else:
        raise ValueError(f"Unknown table name: {table_name}")

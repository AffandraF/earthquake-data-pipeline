import pandas as pd
import logging
from sqlalchemy import create_engine
from src.pandas.postgres_loader import save_postgres
from src.pandas.schemas import (
    get_gold_col_names, get_gold_types,
    get_country_stats_col_names, get_country_stats_types,
    DATA_KEY_COLS
)

def read_silver(con_str, table_name):
    engine = create_engine(con_str)
    query = f"SELECT * FROM {table_name};"
    
    conn = engine.raw_connection()
    try:
        df = pd.read_sql(query, con=conn)
        logging.info(f"Data berhasil dibaca dari tabel {table_name}")
    finally:
        conn.close()
        engine.dispose()
        
    return df

def process_gold(con_str, silver_table, gold_table):
    logging.info(f"Starting Gold ETL process. Input table: {silver_table}, Output table: {gold_table}")
    
    df = read_silver(con_str, silver_table)
    initial_count = len(df)
    logging.info(f"Loaded {initial_count} records from Silver layer.")

    # Menambah kolom kategori gempa berdasarkan magnitude
    df['mag_class'] = df['magnitude'].apply(
        lambda x: 'Minor' if x < 3.0 else
        'Light' if 3.0 <= x < 5.0 else
        'Moderate' if 5.0 <= x < 7.0 else
        'Strong'
    )

    # Analisis statistik per negara
    df_stats = df.groupby('country').agg(
        eq_count = ('magnitude', 'count'),
        avg_magnitude = ('magnitude', 'mean'),
        max_magnitude = ('magnitude', 'max'),
        min_magnitude = ('magnitude', 'min'),
        tsunami_count = ('tsunami', 'sum')
    ).reset_index()

    # Bulatkan nilai rata-rata magnitude
    df_stats['avg_magnitude'] = df_stats['avg_magnitude'].round(2)

    # Select and cast columns using centralized schema definitions
    gold_cols = get_gold_col_names()
    gold_types = get_gold_types()
    
    df_gold = df[gold_cols].copy()
    for col, dtype in gold_types.items():
        if col == 'date_time':
            df_gold[col] = pd.to_datetime(df_gold[col])
        else:
            df_gold[col] = df_gold[col].astype(dtype)

    # Cast country_stats columns using centralized schema definitions
    stats_cols = get_country_stats_col_names()
    stats_types = get_country_stats_types()
    
    df_stats = df_stats[stats_cols].copy()
    for col, dtype in stats_types.items():
        df_stats[col] = df_stats[col].astype(dtype)

    # Upsert data to gold tables
    logging.info(f"Upserting data into Gold table: {gold_table}")
    save_postgres(df_gold, con_str, gold_table, key_cols=DATA_KEY_COLS)
    
    logging.info("Upserting statistics into country stats table: gold.country_stats")
    save_postgres(df_stats, con_str, "gold.country_stats", key_cols=["country"])
    
    logging.info("Gold layer processing completed successfully.")

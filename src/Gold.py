import pandas as pd
import logging
import psycopg2
from sqlalchemy import create_engine, text

def read_silver(con_str, table_name):
    engine = create_engine(con_str)
    query = f"SELECT * FROM {table_name};"
    
    conn = engine.raw_connection()
    try:
        df = pd.read_sql(query, con=conn)
        logging.info(f"Data berhasil dibaca dari tabel {table_name}")
    finally:
        conn.close()
        
    return df

def save_gold(con_str, table_name, df, df_stats):
    # Koneksi ke database gold
    engine = create_engine(con_str)

    create_sql = """
    CREATE SCHEMA IF NOT EXISTS gold;
    
    CREATE TABLE IF NOT EXISTS gold.gold_data (
        id text primary key,
        title text,
        magnitude float,
        date_time timestamp,
        cdi float,
        mmi float,
        alert text,
        tsunami int,
        sig int,
        net text,
        nst int,
        dmin float,
        gap float,
        mag_type text,
        depth float,
        latitude float,
        longitude float,
        location text,
        continent text,
        country text,
        mag_class text
    );

    CREATE TABLE IF NOT EXISTS gold.country_stats (
        country text primary key,
        eq_count int,
        avg_magnitude float,
        max_magnitude float,
        min_magnitude float,
        tsunami_count int
    );
    """

    insert_sql = f"""
    INSERT INTO {table_name} (
        id, title, magnitude, date_time, cdi, mmi, alert, tsunami,
        sig, net, nst, dmin, gap, mag_type, depth, latitude,
        longitude, location, continent, country, mag_class
    ) VALUES (
        :id, :title, :magnitude, :date_time, :cdi, :mmi, :alert, :tsunami,
        :sig, :net, :nst, :dmin, :gap, :mag_type, :depth, :latitude,
        :longitude, :location, :continent, :country, :mag_class
    )
    ON CONFLICT (id) DO UPDATE SET
        title = EXCLUDED.title,
        magnitude = EXCLUDED.magnitude,
        date_time = EXCLUDED.date_time,
        cdi = EXCLUDED.cdi,
        mmi = EXCLUDED.mmi,
        alert = EXCLUDED.alert,
        tsunami = EXCLUDED.tsunami,
        sig = EXCLUDED.sig,
        net = EXCLUDED.net,
        nst = EXCLUDED.nst,
        dmin = EXCLUDED.dmin,
        gap = EXCLUDED.gap,
        mag_type = EXCLUDED.mag_type,
        depth = EXCLUDED.depth,
        latitude = EXCLUDED.latitude,
        longitude = EXCLUDED.longitude,
        location = EXCLUDED.location,
        continent = EXCLUDED.continent,
        country = EXCLUDED.country,
        mag_class = EXCLUDED.mag_class;
    """
    insert_stats= """
    INSERT INTO gold.country_stats (
        country, eq_count, avg_magnitude, max_magnitude,
        min_magnitude, tsunami_count
    ) VALUES (
        :country, :eq_count, :avg_magnitude, :max_magnitude,
        :min_magnitude, :tsunami_count
    )
    ON CONFLICT (country) DO UPDATE SET
        eq_count = EXCLUDED.eq_count,
        avg_magnitude = EXCLUDED.avg_magnitude,
        max_magnitude = EXCLUDED.max_magnitude,
        min_magnitude = EXCLUDED.min_magnitude,
        tsunami_count = EXCLUDED.tsunami_count;
    """

    with engine.begin() as conn:
        conn.execute(text(create_sql))

        records = df.to_dict(orient='records')
        conn.execute(text(insert_sql), records)

        stats_records = df_stats.to_dict(orient='records')
        conn.execute(text(insert_stats), stats_records)

def process_gold(con_str, silver_table, gold_table):
    df = read_silver(con_str, silver_table)

    # Menambah kolom kategori gempa berdasarkan magnitude
    df['mag_class'] = df['magnitude'].apply(
        lambda x: 'Minor' if x < 3.0 else
        'light' if 3.0 <= x < 5.0 else
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

    # Simpan ke gold database
    save_gold(con_str, gold_table, df, df_stats)
    logging.info(f"Data berhasil disimpan ke {gold_table}")
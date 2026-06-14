import pandas as pd
import os
import logging
import reverse_geocoder as rg
import pycountry_convert as pc
from src.pandas.postgres_loader import save_postgres
from src.pandas.schemas import get_silver_col_names, get_silver_types, DATA_KEY_COLS

def enrich_location(lat, lon):
    try: 
        results = rg.search([(lat, lon)], mode=1)
        country_code = results[0]['cc']

        # Kasus kode timor leste
        if country_code == 'TL':
            return pd.Series(['Oceania', 'Timor Leste'])

        continent_name = pc.country_alpha2_to_continent_code(country_code)
        country_name = pc.country_alpha2_to_country_name(country_code)
        return pd.Series([continent_name, country_name])
    
    except Exception as e:
        logging.warning(f"Tidak dapat memperkaya lokasi untuk koordinat ({lat}, {lon}): {e}")
        return pd.Series([None, None])

def process_silver(bronze_path, con_str, silver_table):
    # Baca dari bronze
    if not os.path.exists(bronze_path):
        raise FileNotFoundError(f"Bronze path: {bronze_path} tidak ada")
    
    # Read parquet from bronze directory
    df = pd.read_parquet(bronze_path)
    initial_count = len(df)
    logging.info(f"Loaded {initial_count} records from Bronze layer.")

    # Rename kolom magType menjadi mag_type
    df = df.rename(columns={'magType': 'mag_type'})

    # Standardisasi penulisan semua kolom
    df.columns = [col.lower().replace(' ', '_') for col in df.columns]

    # Hapus duplikat
    df = df.drop_duplicates(subset=['magnitude', 'date_time', 'latitude', 'longitude'])
    dedup_count = len(df)
    logging.info(f"Deduplicated records from {initial_count} to {dedup_count} (dropped {initial_count - dedup_count}).")

    # Hapus baris dengan nilai null pada kolom penting
    df = df.dropna(subset=['magnitude', 'date_time', 'latitude', 'longitude'])
    non_null_count = len(df)
    logging.info(f"Filtered out null keys: {dedup_count - non_null_count} records dropped.")

    # Validasi magnitude (magnitude harus > 0)
    df = df[df['magnitude'] > 0]
    valid_magnitude_count = len(df)
    logging.info(f"Filtered invalid magnitudes (<= 0): {non_null_count - valid_magnitude_count} records dropped. Remaining: {valid_magnitude_count}")

    if valid_magnitude_count == 0:
        raise ValueError("All records filtered out — aborting Silver processing")

    # Standardisasi format date_time dari dd-MM-yyyy HH:mm ke yyyy-MM-dd HH:mm
    df['date_time'] = pd.to_datetime(df['date_time'], format='%d-%m-%Y %H:%M', errors='coerce').dt.strftime('%Y-%m-%d %H:%M')

    # Isi nilai null pada kolom alert dengna 'not available'
    df['alert'] = df['alert'].fillna('not available')

    # Enrich lokasi berdasarkan latitude dan longitude jika kosong (Optimized)
    is_missing = df['continent'].isna() | df['country'].isna()
    df_needs_enrich = df[is_missing].copy()
    df_already_enriched = df[~is_missing].copy()
    
    needs_enrich_count = len(df_needs_enrich)
    logging.info(f"Records needing location enrichment: {needs_enrich_count}")

    if needs_enrich_count > 0:
        enriched = df_needs_enrich.apply(
            lambda row: enrich_location(row['latitude'], row['longitude']),
            axis=1
        )
        df_needs_enrich[['continent', 'country']] = enriched
        df = pd.concat([df_already_enriched, df_needs_enrich], ignore_index=True)
    else:
        df = df_already_enriched

    # Standardisasi penamaan continent
    continent_mapping = {
        'AF': 'Africa',
        'AS': 'Asia',
        'EU': 'Europe',
        'NA': 'North America',
        'OC': 'Oceania',
        'SA': 'South America',
        'AN': 'Antarctica'
    }
    df['continent'] = df['continent'].map(continent_mapping).fillna(df['continent'])

    # Select and cast columns using centralized schema definitions
    silver_cols = get_silver_col_names()
    silver_types = get_silver_types()
    
    # Ensure all columns exist in dataframe
    for col in silver_cols:
        if col not in df.columns:
            df[col] = None
            
    df = df[silver_cols].copy()
    
    # Apply type casting
    for col, dtype in silver_types.items():
        if col == 'date_time':
            df[col] = pd.to_datetime(df[col])
        else:
            df[col] = df[col].astype(dtype)

    logging.info(f"Writing Silver data into PostgreSQL target table: {silver_table}")
    save_postgres(df, con_str, silver_table, key_cols=DATA_KEY_COLS)
    logging.info("Silver layer processing completed successfully.")
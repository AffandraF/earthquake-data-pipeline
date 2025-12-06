import pandas as pd
import os
import logging
import psycopg2
from sqlalchemy import create_engine, text
import hashlib
import reverse_geocoder as rg
import pycountry_convert as pc

def make_hash(row):
    base = f"{row.magnitude}|{row.date_time}|{row.latitude}|{row.longitude}"
    return hashlib.md5(base.encode()).hexdigest()

def enrich_location(lat, lon):
    try: 
        results = rg.search([(lat, lon)], mode=1)
        country_code = results[0]['cc']

        # Kasus kode timor leste
        if country_code == 'TL':
            continent_name = 'Oceania'
            country_name = 'Timor Leste'
            return pd.Series([continent_name, country_name])

        continent_name = pc.country_alpha2_to_continent_code(country_code)
        country_name = pc.country_alpha2_to_country_name(country_code)

        return pd.Series([continent_name, country_name])
    
    except Exception as e:
        logging.warning(f"Tidak dapat memperkaya lokasi untuk koordinat ({lat}, {lon}): {e}")
        return pd.Series([None, None])

def save_postgres(df, con_str, table_name):
    engine = create_engine(con_str)

    create_sql = f"""
    CREATE SCHEMA IF NOT EXISTS silver;

    CREATE TABLE IF NOT EXISTS {table_name} (
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
        country text
    );    
    """

    insert_sql = f"""
    INSERT INTO {table_name} (
        id, title, magnitude, date_time, cdi, mmi, alert, tsunami,
        sig, net, nst, dmin, gap, mag_type, depth, latitude,
        longitude, location, continent, country
    ) VALUES (
        :id, :title, :magnitude, :date_time, :cdi, :mmi, :alert, :tsunami,
        :sig, :net, :nst, :dmin, :gap, :mag_type, :depth, :latitude,
        :longitude, :location, :continent, :country
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
        country = EXCLUDED.country;
    """

    with engine.begin() as conn:
        conn.execute(text(create_sql))
        
        records = df.to_dict(orient='records')
        conn.execute(text(insert_sql), records)

def process_silver(bronze_path, con_str, silver_table):
    # Baca dari bronze
    if not os.path.exists(bronze_path):
        raise FileNotFoundError(f"Bronze path: {bronze_path} tidak ada")
    
    df = pd.read_parquet(bronze_path)

    # Rename kolom magType menjadi mag_type
    df = df.rename(columns={'magType':'mag_type'})

    # Standardisasi penulisan semua kolom
    df.columns = [col.lower().replace(' ', '_') for col in df.columns]

    # Hapus duplikat
    df = df.drop_duplicates(subset=['magnitude', 'date_time', 'latitude', 'longitude'])

    # Hapus baris dengan nilai null pada kolom penting
    df = df.dropna(subset=['magnitude', 'date_time', 'latitude', 'longitude'])

    # Validasi magnitude
    df = df[df['magnitude'] > 0]

    # Standardisasi format date_time dari dd-MM-yyyy HH:mm ke yyyy-MM-dd HH:mm
    df['date_time'] = pd.to_datetime(df['date_time'], format='%d-%m-%Y %H:%M').dt.strftime('%Y-%m-%d %H:%M')

    # Isi nilai null pada kolom alert dengna 'not available'
    df['alert'] = df['alert'].fillna('not available')

    # Tambahkan kolom id sebagai primary key
    df['id'] = df.apply(make_hash, axis=1)

    # Enrich lokasi berdasarkan latitude dan longitude jika kosong
    df[['continent', 'country']] = df.apply(
        lambda row: enrich_location(row['latitude'], row['longitude'])
        if pd.isna(row['continent']) or pd.isna(row['country'])
        else pd.Series([row['continent'], row['country']]),
        axis=1
    )

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

    logging.info(f"Data setelah proses silver memiliki {len(df)} baris dan {len(df.columns)} kolom")

    # Simpan ke silver
    save_postgres(df, con_str, silver_table)

    # Simpan ke silver lokal sebagai parquet (opsional)
    # os.makedirs(os.path.dirname(silver_path), exist_ok=True)    
    # df.to_parquet(silver_path, index=False)
    
    logging.info(f"Data berhasil disimpan ke tabel {'silver.silver_data'}")
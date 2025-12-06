import pandas as pd
import os
import logging

def process_silver(bronze_path, silver_path):
    # Baca dari bronze
    if not os.path.exists(bronze_path):
        raise FileNotFoundError(f"Bronze path: {bronze_path} tidak ada")
    
    df = pd.read_parquet(bronze_path)

    # Hapus duplikat
    df = df.drop_duplicates(subset=['magnitude', 'date_time', 'latitude', 'longitude'])

    # Hapus baris dengan nilai null pada kolom penting
    df = df.dropna(subset=['magnitude', 'date_time', 'latitude', 'longitude'])

    # Validasi magnitude
    df = df[df['magnitude'] > 0]

    # Standardisasi format date_time dari dd-MM-yyyy HH:mm ke yyyy-MM-dd HH:mm
    df['date_time'] = pd.to_datetime(df['date_time'], format='%d-%m-%Y %H:%M').dt.strftime('%Y-%m-%d %H:%M')

    # Rename kolom magType menjadi mag_type
    df = df.rename(columns={'magType':'mag_type'})

    # Standardisasi penulisan semua kolom
    df.columns = [col.lower().replace(' ', '_') for col in df.columns]

    # Simpan ke silver dengan format parquet
    os.makedirs(os.path.dirname(silver_path), exist_ok=True)
    df.to_parquet(silver_path, index=False)
    logging.info(f"Data berhasil disimpan ke {silver_path}")
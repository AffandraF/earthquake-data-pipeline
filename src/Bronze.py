import pandas as pd
import os
import logging

def process_bronze(source_path, bronze_path):
    # Ingest data mentah dari csv ke parquet
    if not os.path.exists(source_path):
        raise FileNotFoundError(f"Source path: {source_path} tidak ada")
    
    df = pd.read_csv(source_path)

    # Tambah metadata waktu ingest
    df['ingest_timestamp'] = pd.Timestamp.now()

    # Simpan ke bronze dengan format parquet
    os.makedirs(os.path.dirname(bronze_path), exist_ok=True)
    df.to_parquet(bronze_path, index=False)
    logging.info(f"Data berhasil disimpan ke {bronze_path}")
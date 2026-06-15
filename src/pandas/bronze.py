import pandas as pd
import os
import logging

# Strict CSV schema specification matching the source data
CSV_DTYPES = {
    "title": "string",
    "magnitude": "float64",
    "date_time": "string",
    "cdi": "float64",
    "mmi": "float64",
    "alert": "string",
    "tsunami": "Int64",
    "sig": "Int64",
    "net": "string",
    "nst": "Int64",
    "dmin": "float64",
    "gap": "float64",
    "magType": "string",
    "depth": "float64",
    "latitude": "float64",
    "longitude": "float64",
    "location": "string",
    "continent": "string",
    "country": "string",
}

def process_bronze(source_path, bronze_path):
    # Ingest data mentah dari csv ke parquet
    if not os.path.exists(source_path):
        raise FileNotFoundError(f"Source path: {source_path} tidak ada")
    
    # Read raw CSV using defined dtype schema
    df = pd.read_csv(source_path, dtype=CSV_DTYPES)
    row_count = len(df)
    logging.info(f"Loaded {row_count} records from source CSV.")

    # Tambah metadata waktu ingest dan tanggal ingest
    current_time = pd.Timestamp.now()
    df['ingest_timestamp'] = current_time
    ingest_date = current_time.strftime('%Y-%m-%d')
    df['ingest_date'] = ingest_date

    # Simpan ke bronze dengan format parquet dan partisi ingest_date
    if os.path.exists(bronze_path) and os.path.isfile(bronze_path):
        logging.info(f"Removing existing file at {bronze_path} to create directory structure.")
        os.remove(bronze_path)
        
    os.makedirs(bronze_path, exist_ok=True)
    
    # Hapus folder partisi tanggal berjalan jika sudah ada agar data tidak menumpuk
    partition_path = os.path.join(bronze_path, f"ingest_date={ingest_date}")
    if os.path.exists(partition_path) and os.path.isdir(partition_path):
        logging.info(f"Clearing existing partition directory at {partition_path} for idempotency.")
        import shutil
        shutil.rmtree(partition_path)
        
    df.to_parquet(
        bronze_path,
        partition_cols=['ingest_date'],
        index=False,
        engine='pyarrow'
    )
    logging.info(f"Data berhasil disimpan ke {bronze_path} (partitioned by ingest_date). Ingested: {row_count} records.")
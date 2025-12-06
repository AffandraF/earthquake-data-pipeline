import pandas as pd
import os
import logging

def process_gold(silver_path, gold_path):
    # Baca dari silver
    if not os.path.exists(silver_path):
        raise FileNotFoundError(f'Silver path: {silver_path} tidak ada')
    
    df = pd.read_parquet(silver_path)

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

    # Simpan ke gold dengan format parquet
    os.makedirs(os.path.dirname(gold_path), exist_ok=True)
    df_stats.to_parquet(gold_path, index=False)
    logging.info(f"Data berhasil disimpan ke {gold_path}")
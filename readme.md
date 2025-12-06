# Earthquake Data Engineering Pipeline

![Python](https://img.shields.io/badge/Python-3.11-blue)
![Airflow](https://img.shields.io/badge/Orchestrator-Apache%20Airflow-red)
![Postgres](https://img.shields.io/badge/Data%20Warehouse-PostgreSQL-blue)
![Docker](https://img.shields.io/badge/Container-Docker-2496ED)

Projek Data Engineering *end-to-end* yang memproses data historis gempa bumi global. Pipeline ini mengubah data mentah menjadi wawasan analitik menggunakan **Apache Airflow**, **Pandas**, dan **PostgreSQL**, yang berjalan sepenuhnya di lingkungan **Docker**.

## Arsitektur Data (Medallion)

Sistem ini menerapkan **Medallion Architecture**:

* **Bronze Layer (Raw):** Menyimpan data mentah dari [Kaggle Earthquake Dataset](https://www.kaggle.com/datasets/warcoder/earthquake-dataset) dalam format Parquet.
* **Silver Layer (Cleansed & Enriched):**
    * Standardisasi nama kolom dan format waktu.
    * **Deduplikasi:** Menggunakan *MD5 Hashing* untuk membuat ID unik.
    * **Enrichment:** Melengkapi data lokasi (Negara/Benua) yang hilang menggunakan koordinat.
* **Gold Layer (Aggregated):**
    * Klasifikasi gempa berdasarkan skala (Minor, Light, Moderate, Strong).
    * Agregasi statistik per negara (Frekuensi, Rata-rata kekuatan, Risiko Tsunami).

## Tech Stack

* **Bahasa:** Python 3.11
* **Orkestrasi:** Apache Airflow
* **Database:** PostgreSQL
* **Infrastruktur:** Docker
* **Library:** `pandas`, `reverse_geocoder`, `pycountry_convert`, `sqlalchemy`

## Logika Transformasi Utama

### 1. Mencegah Duplikasi (Idempotency) & Unique ID
Untuk mencegah duplikasi data saat pipeline dijalankan ulang (*rerun*), *Primary Key* sintetis dibuat menggunakan hashing:

```python
# MD5 Hash dari kombinasi atribut kunci
base = f"{row.magnitude}|{row.date_time}|{row.latitude}|{row.longitude}"
return hashlib.md5(base.encode()).hexdigest()
````

### 2. Melengkapi Data Lokasi

Mengisi data negara yang kosong dengan mengonversi koordinat Latitude/Longitude:

  * **Logic:** `(Lat, Lon) -> Kode Negara (ISO) -> Nama Negara & Benua`
  * **Edge Case:** Penanganan khusus untuk kode negara tertentu (misal: 'TL' untuk Timor Leste).

### 3. Klasifikasi Risiko Gempa

Data dikelompokkan untuk analisis risiko menggunakan logika berikut:

  * **Minor:** \< 3.0
  * **Light:** 3.0 - 4.9
  * **Moderate:** 5.0 - 6.9
  * **Strong:** \>= 7.0

## Skema Hasil (PostgreSQL)

Tabel akhir di layer Gold (`country_stats`) siap untuk visualisasi:

| Kolom | Tipe Data | Deskripsi |
| :--- | :--- | :--- |
| `country` | VARCHAR | Nama Negara |
| `eq_count` | INT | Total kejadian gempa |
| `avg_magnitude` | FLOAT | Rata-rata kekuatan gempa |
| `max_magnitude` | FLOAT | Gempa terkuat yang tercatat |
| `min_magnitude` | FLOAT | Gempa terlemah yang tercatat |
| `tsunami_count` | INT | Jumlah kejadian berpotensi tsunami |

## Cara Menjalankan

Pastikan **Docker** dan **Docker Compose** sudah terinstall.

1.  **Clone repositori ini**

    ```bash
    git clone https://github.com/AffandraF/earthquake-data-pipeline.git
    cd earthquake-pipeline
    ```

2.  **Jalankan services**

    ```bash
    dockercompose up -d --build
    ```

3.  **Monitoring Pipeline**

      * Buka Airflow UI di `http://localhost:8080`.
      * Trigger DAG bernama `earthquake_data_pipeline`.

4.  **Cek Hasil Data**
    Jalankan query sql ini di Postgres:

    ```sql
    SELECT * FROM gold.gold_data;
    SELECT * FROM gold.country_stats;
    ```
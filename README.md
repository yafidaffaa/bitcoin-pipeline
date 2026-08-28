# Bitcoin Price Data Pipeline

Pipeline data otomatis untuk mengambil, memproses, dan menganalisis data harga Bitcoin menggunakan Apache Airflow, dbt, dan PostgreSQL.

## Arsitektur

```
CoinGecko API → Airflow DAG → PostgreSQL (raw) → dbt → PostgreSQL (staging & mart)
```

## Tech Stack

- **Apache Airflow** — orkestrasi dan penjadwalan pipeline
- **dbt (data build tool)** — transformasi dan validasi data
- **PostgreSQL** — penyimpanan data
- **Python** — pengambilan data dari API
- **Docker** — containerisasi environment

## Struktur Project

```
bitcoin-pipeline/
├── dags/
│   └── bitcoin_pipeline_dag.py
├── dbt/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── sources.yml
│   │   │   ├── schema.yml
│   │   │   └── stg_bitcoin_prices.sql
│   │   └── mart/
│   │       └── mart_bitcoin_monthly.sql
│   ├── macros/
│   │   └── generate_schema_name.sql
│   └── dbt_project.yml
├── scripts/
│   └── fetch_bitcoin.py
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── .env.example
```

## Struktur Database

```
bitcoin_db
├── raw
│   └── bitcoin_prices          # Data mentah dari CoinGecko API
├── staging
│   └── stg_bitcoin_prices      # Data bersih dengan kalkulasi tambahan
└── mart
    └── mart_bitcoin_monthly    # Agregasi bulanan siap analisis
```

## Pipeline Flow

Pipeline berjalan otomatis setiap hari jam 08:00 WIB:

1. **fetch_bitcoin_data** — ambil data harga Bitcoin dari CoinGecko API
2. **dbt_run_staging** — bersihkan dan transformasi data
3. **dbt_run_mart** — agregasi data per bulan

## Setup & Installation

### Prerequisites

- Docker Desktop
- Python 3.12+
- PostgreSQL 17

### Langkah Instalasi

1. Clone repository

```bash
git clone https://github.com/username/bitcoin-pipeline.git
cd bitcoin-pipeline
```

2. Buat file `.env` dari template

```bash
cp .env.example .env
```

3. Jalankan container

```bash
docker-compose up -d
```

4. Setup database PostgreSQL

```sql
CREATE DATABASE bitcoin_db;
\c bitcoin_db
CREATE SCHEMA raw;
CREATE SCHEMA staging;
CREATE SCHEMA mart;
```

5. Buat tabel

```sql
CREATE TABLE raw.bitcoin_prices (
    id          SERIAL PRIMARY KEY,
    date        DATE NOT NULL UNIQUE,
    open        NUMERIC(15, 2),
    high        NUMERIC(15, 2),
    low         NUMERIC(15, 2),
    close       NUMERIC(15, 2),
    volume      NUMERIC(25, 2),
    market_cap  NUMERIC(25, 2),
    created_at  TIMESTAMP DEFAULT NOW()
);
```

6. Load data historis

```bash
python scripts/fetch_bitcoin.py
```

7. Jalankan dbt

```bash
cd dbt
dbt run
dbt test
```

8. Buka Airflow dashboard di `http://localhost:8080` dengan login `admin` / `admin`

## dbt Tests

| Test | Kolom | Deskripsi |
|------|-------|-----------|
| not_null | price_date | Tanggal tidak boleh kosong |
| unique | price_date | Tidak boleh ada tanggal duplikat |
| not_null | close_price | Harga tidak boleh kosong |
| not_null | volume_usd | Volume tidak boleh kosong |
| not_null | market_cap_usd | Market cap tidak boleh kosong |
| not_null | price_category | Kategori tidak boleh kosong |
| accepted_values | price_category | Hanya boleh: high, medium, low |

Jalankan tests:

```bash
cd dbt && dbt test
```

## Menjalankan Pipeline

### Otomatis

Pipeline berjalan otomatis setiap hari jam 08:00 selama Docker berjalan.

### Manual

```bash
python scripts/fetch_bitcoin.py
cd dbt && dbt run
```

## Menghidupkan Kembali Project

```bash
# Start
docker-compose start

# Stop (data tetap tersimpan)
docker-compose stop
```

## Author

**Yafi Daffa Andriansyah**  
Fresh Graduate — Teknologi Informasi  
Penelitian: "Investigating the Limitation of Technical-Based Multivariate Time Series Modeling in Predicting Bitcoin Prices"

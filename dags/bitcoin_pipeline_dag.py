from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import requests
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

# ================================
# KONFIGURASI
# ================================
DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST_DOCKER"),
    "port": int(os.getenv("POSTGRES_PORT")),
    "database": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD")
}

DBT_PROJECT_DIR = "/opt/airflow/dbt"
DBT_BIN = "/home/airflow/.local/bin/dbt"

# ================================
# FUNGSI TASKS
# ================================
def fetch_and_save_bitcoin():
    print("Mengambil data Bitcoin dari CoinGecko...")

    url = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart"
    params = {
        "vs_currency": "usd",
        "days": 1,
        "interval": "daily"
    }

    response = requests.get(url, params=params)

    if response.status_code != 200:
        raise Exception(f"API Error: {response.status_code}")

    data = response.json()
    prices     = data['prices']
    volumes    = data['total_volumes']
    market_cap = data['market_caps']

    conn   = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    inserted = 0
    for i in range(len(prices)):
        from datetime import timezone
        timestamp = prices[i][0] / 1000
        date = datetime.fromtimestamp(timestamp, tz=timezone.utc).date()

        cursor.execute("""
            INSERT INTO raw.bitcoin_prices
                (date, close, volume, market_cap)
            VALUES
                (%s, %s, %s, %s)
            ON CONFLICT (date) DO NOTHING
        """, (
            date,
            round(prices[i][1], 2),
            round(volumes[i][1], 2),
            round(market_cap[i][1], 2)
        ))

        if cursor.rowcount > 0:
            inserted += 1

    conn.commit()
    cursor.close()
    conn.close()

    print(f"Selesai! {inserted} data baru disimpan")

# ================================
# DEFINISI DAG
# ================================
default_args = {
    'owner': 'yafi',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False
}

with DAG(
    dag_id='bitcoin_daily_pipeline',
    description='Pipeline harian data harga Bitcoin',
    start_date=datetime(2026, 8, 1),
    schedule_interval='0 8 * * *',
    catchup=False,
    default_args=default_args,
    tags=['bitcoin', 'crypto', 'data-engineering']
) as dag:

    task_fetch = PythonOperator(
        task_id='fetch_bitcoin_data',
        python_callable=fetch_and_save_bitcoin
    )

    task_dbt_staging = BashOperator(
        task_id='dbt_run_staging',
        bash_command=f'cd {DBT_PROJECT_DIR} && {DBT_BIN} run --select staging --profiles-dir {DBT_PROJECT_DIR} --target docker'
    )

    task_dbt_mart = BashOperator(
        task_id='dbt_run_mart',
        bash_command=f'cd {DBT_PROJECT_DIR} && {DBT_BIN} run --select mart --profiles-dir {DBT_PROJECT_DIR} --target docker'
    )

    # Urutan eksekus
    task_fetch >> task_dbt_staging >> task_dbt_mart
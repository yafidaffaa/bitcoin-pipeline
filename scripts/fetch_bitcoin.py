import requests
import psycopg2
from datetime import datetime, timezone
import time
import os
from dotenv import load_dotenv

load_dotenv()

# ================================
# KONFIGURASI DATABASE
# ================================
DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": int(os.getenv("POSTGRES_PORT")),
    "database": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD")
}

# ================================
# FUNGSI AMBIL DATA DARI API
# ================================
def fetch_bitcoin_data(days=365):
    """
    Ambil data harga Bitcoin dari CoinGecko API
    days: berapa hari ke belakang yang mau diambil
    """
    print(f"Mengambil data Bitcoin {days} hari terakhir...")
    
    url = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart"
    params = {
        "vs_currency": "usd",
        "days": days,
        "interval": "daily"
    }
    
    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        data = response.json()
        print(f"Berhasil ambil data dari API")
        return data
    else:
        print(f"Error: {response.status_code} - {response.text}")
        return None

# ================================
# FUNGSI PROSES DATA
# ================================
def process_data(raw_data):
    """
    Ubah format data dari API menjadi list of dictionary
    yang siap dimasukkan ke database
    """
    prices     = raw_data['prices']
    volumes    = raw_data['total_volumes']
    market_cap = raw_data['market_caps']
    
    processed = []
    
    for i in range(len(prices)):
        # Timestamp dari API dalam milidetik, ubah ke tanggal
        timestamp = prices[i][0] / 1000
        date = datetime.fromtimestamp(timestamp, tz=timezone.utc).date()
        
        processed.append({
            "date":       date,
            "close":      round(prices[i][1], 2),
            "volume":     round(volumes[i][1], 2),
            "market_cap": round(market_cap[i][1], 2)
        })
    
    print(f"Berhasil proses {len(processed)} data points")
    return processed

# ================================
# FUNGSI SIMPAN KE DATABASE
# ================================
def save_to_database(data):
    """
    Simpan data ke tabel raw.bitcoin_prices di PostgreSQL
    Kalau data untuk tanggal itu sudah ada, skip (tidak dobel)
    """
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    inserted = 0
    skipped  = 0
    
    for row in data:
        try:
            cursor.execute("""
                INSERT INTO raw.bitcoin_prices 
                    (date, close, volume, market_cap)
                VALUES 
                    (%(date)s, %(close)s, %(volume)s, %(market_cap)s)
                ON CONFLICT (date) DO NOTHING
            """, row)
            
            if cursor.rowcount > 0:
                inserted += 1
            else:
                skipped += 1
                
        except Exception as e:
            print(f"Error insert baris {row['date']}: {e}")
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"Selesai! {inserted} data baru disimpan, {skipped} data sudah ada (skip)")

# ================================
# FUNGSI UTAMA
# ================================
def main():
    print("=== Bitcoin Data Pipeline ===")
    print(f"Waktu: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Step 1: Ambil data dari API
    raw_data = fetch_bitcoin_data(days=365)
    if not raw_data:
        print("Gagal ambil data, pipeline berhenti")
        return
    
    # Jeda 1 detik agar tidak kena rate limit API
    time.sleep(1)
    
    # Step 2: Proses data
    processed_data = process_data(raw_data)
    
    # Step 3: Simpan ke database
    save_to_database(processed_data)
    
    print()
    print("=== Pipeline selesai! ===")

if __name__ == "__main__":
    main()
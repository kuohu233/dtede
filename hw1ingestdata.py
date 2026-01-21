import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import requests
import os

PG_USER = 'postgres'
PG_PASS = 'postgres'
PG_HOST = 'localhost'
PG_PORT = 5432
PG_DB = 'hw'

GREEN_DATA_FILE = f'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet'
ZONES_DATA_FILE = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv'

def ingest_green_taxi(engine):
    target_table = 'green_taxi_data'
    local_filename = 'green_tripdata_2025-11.parquet'

    if not os.path.exists(local_filename):
        print(f"正在下载文件: {GREEN_DATA_FILE}...")
        r = requests.get(GREEN_DATA_FILE)
        if r.status_code == 200:
            with open(local_filename, 'wb') as f:
                f.write(r.content)
            print("下载完成。")
        else:
            print(f"下载失败，状态码: {r.status_code}")
            return

    file_size = os.path.getsize(local_filename)
    if file_size == 0:
        print("错误：下载的文件大小为 0 字节，请检查链接是否有效。")
        return

    print(f"正在读取本地文件 {local_filename}...")
    df = pd.read_parquet(local_filename)

    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    chunk_size = 100000
    n_chunks = (len(df) // chunk_size) + 1

    print(f"开始分批写入 {target_table}...")
    for i in tqdm(range(n_chunks)):
        start = i * chunk_size
        end = (i + 1) * chunk_size
        df_chunk = df.iloc[start:end]
        
        if df_chunk.empty:
            continue
            
        if i == 0:
            df_chunk.head(0).to_sql(name=target_table, con=engine, if_exists='replace', index=False)
        
        df_chunk.to_sql(name=target_table, con=engine, if_exists='append', index=False)

def ingest_zones(engine):
    """处理 Zones CSV 数据"""
    target_table = 'taxi_zones'
    print(f"正在读取 {ZONES_DATA_FILE}...")
    df_zones = pd.read_csv(ZONES_DATA_FILE)
    
    df_zones.to_sql(name=target_table, con=engine, if_exists='replace', index=False)
    print(f"Zones 数据写入 {target_table} 完成。")

def run():
    engine = create_engine(f'postgresql://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}')

    ingest_green_taxi(engine)
    ingest_zones(engine)

if __name__ == '__main__':
    run()

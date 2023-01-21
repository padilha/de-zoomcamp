import pandas as pd
import argparse
import os
from sqlalchemy import create_engine
from time import time

def parquet_to_csv(parquet_file, csv_file):
    df = pd.read_parquet(parquet_file, engine = 'pyarrow')
    df.to_csv(csv_file, index=False)

def ingest(csv_file, table_name, engine, chunksize=100000):
    df_iter = pd.read_csv(csv_file, iterator=True, chunksize=chunksize)
    run = True
    while run:
        try:
            t_start = time()
            df = next(df_iter)
            df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
            df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
            df.to_sql(name=table_name, con=engine, if_exists='append')
            t_end = time()
            print(f'inserted another chunk, took {t_end-t_start:.3f} seconds')
        except Exception:
            run = False

def main(params):
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    table_name = params.table_name
    url = params.url
    parquet_file = 'output.parquet'
    csv_file = 'output.csv'
    os.system(f'wget {url} -O {parquet_file}')
    parquet_to_csv(parquet_file, csv_file)
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()
    ingest(csv_file, table_name, engine)
    

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')
    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--url', required=True, help='url of the csv file')
    args = parser.parse_args()
    main(args)
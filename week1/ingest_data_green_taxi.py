import pandas as pd
import argparse
import os
from sqlalchemy import create_engine
from time import time

def ingest(csv_file, table_name, engine, chunksize=100000):
    df_iter = pd.read_csv(csv_file, iterator=True, chunksize=chunksize, compression='gzip')
    run = True
    while run:
        try:
            t_start = time()
            df = next(df_iter)
            df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
            df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])
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
    csv_file = 'green_tripdata_2019-01.csv.gz'
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
    args = parser.parse_args()
    main(args)
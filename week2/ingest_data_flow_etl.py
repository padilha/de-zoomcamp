import pandas as pd
import os
import argparse
from sqlalchemy import create_engine
from time import time
from prefect import flow, task

@task(log_prints=True)
def extract(url: str, parque_file: str = 'output.parquet', csv_file: str = 'output.csv'):
    parquet_file = 'output.parquet'
    os.system(f'wget {url} -O {parquet_file}')
    df = pd.read_parquet(parquet_file, engine = 'pyarrow')
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    return df

@task(log_prints=True)
def transform(df: pd.DataFrame):
    print(f"pre: missing passenger count: {(df['passenger_count'] == 0).sum()}")
    df = df[df['passenger_count'] != 0]
    print(f"post: missing passenger count: {(df['passenger_count'] == 0).sum()}")
    return df

@task(log_prints=True, retries=3)
def load(user, password, host, port, db, table_name, df):
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()
    df.to_sql(name=table_name, con=engine, if_exists='append')

@flow(name='Ingest Data')
def main_flow(args):
    user = args.user
    password = args.password
    host = args.host
    port = args.port
    db = args.db
    table_name = args.table_name
    url = args.url
    raw_data = extract(url)
    data = transform(raw_data)
    load(user, password, host, port, db, table_name, data)   

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')
    parser.add_argument('--user', required=False, help='user name for postgres', default='root')
    parser.add_argument('--password', required=False, help='password for postgres', default='root')
    parser.add_argument('--host', required=False, help='host for postgres', default='localhost')
    parser.add_argument('--port', required=False, help='port for postgres', default='5432')
    parser.add_argument('--db', required=False, help='database name for postgres', default='ny_taxi')
    parser.add_argument('--table_name', required=False, help='name of the table where we will write the results to', default='yellow_taxi_trips')
    DEFAULT_URL = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet'
    parser.add_argument('--url', required=False, help='url of the csv file', default=DEFAULT_URL)
    args = parser.parse_args()
    main_flow(args)
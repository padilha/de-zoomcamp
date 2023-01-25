import pandas as pd
import os
import argparse
from sqlalchemy import create_engine
from datetime import timedelta
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_sqlalchemy import SqlAlchemyConnector

@task(log_prints=True, tags=['extract'], cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
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
def load(table_name, df):
    connection_block = SqlAlchemyConnector.load("postgres-connector")
    with connection_block.get_connection(begin=False) as engine:
        df.to_sql(name=table_name, con=engine, if_exists='append')

@flow(name='Ingest Data')
def main_flow(args):
    table_name = args.table_name
    url = args.url
    raw_data = extract(url)
    data = transform(raw_data)
    load(table_name, data)   

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')
    parser.add_argument('--table_name', required=False, help='name of the table where we will write the results to', default='yellow_taxi_trips')
    DEFAULT_URL = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet'
    parser.add_argument('--url', required=False, help='url of the csv file', default=DEFAULT_URL)
    args = parser.parse_args()
    main_flow(args)
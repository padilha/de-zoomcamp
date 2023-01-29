import pandas as pd
from pathlib import Path
from datetime import timedelta
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
# from prefect.tasks import task_input_hash
# from random import randint

# For some reason, the commented @task below is responsible for an exception when deploying the code using
# the docker block as the infrastructure. It has something to do with cache_key_fn and cache_expiration.
# I discovered this solution by reading a thread in the course's Slack.
# See:
# https://datatalks-club.slack.com/archives/C01FABYF2RG/p1674823816614039
# https://github.com/PrefectHQ/prefect/issues/6086
# @task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    # simulating failure to test retries
    # if randint(0, 1) == 1:
    #     raise Exception()
    df = pd.read_csv(dataset_url)
    return df

@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix some dtype issues"""
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    print(df.head(5))
    print(f'columns: {df.dtypes}')
    print(f'rows: {len(df)}')
    return df

@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out as parquet file"""
    data_dir = f'data/{color}'
    Path(data_dir).mkdir(parents=True, exist_ok=True)
    path = Path(f'{data_dir}/{dataset_file}.parquet')
    df.to_parquet(path, compression='gzip')
    return path

@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcp_cloud_storage_bucket_block = GcsBucket.load("zoomcamp-gcs")
    gcp_cloud_storage_bucket_block.upload_from_path(from_path=path, to_path=path)

@flow()
def etl_web_to_gcs(year: int, month: int, color: str) -> None:
    """The main ETL function"""
    dataset_file = f'{color}_tripdata_{year}-{month:02}'
    dataset_url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz'
    df = fetch(dataset_url)
    df = clean(df)
    path = write_local(df, color, dataset_file)
    write_gcs(path)

@flow()
def etl_parent_flow(months: list[int] = [1, 2], year: int = 2021, color: str = 'yellow') -> None:
    for month in months:
        etl_web_to_gcs(year, month, color)

if __name__ == '__main__':
    color = 'yellow'
    months = [1, 2, 3]
    year = 2021
    etl_parent_flow(months, year, color)
import os
import pandas as pd
from sqlalchemy import create_engine
url = 'https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv'
os.system(f'wget {url}')
engine = create_engine(f'postgresql://root:root@localhost:5432/ny_taxi')
engine.connect()
df_zones = pd.read_csv('taxi+_zone_lookup.csv')
df_zones.to_sql(name='zones', con=engine, if_exists='replace')
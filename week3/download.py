# script to download the fhv tripdata
import os
for month in range(1, 13):
    url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-{month:02d}.csv.gz'
    os.system('wget ' + url)
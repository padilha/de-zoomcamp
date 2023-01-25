## Question 1.
```--iidfile string``` has the text _Write the image ID to the file_

## Question 2.
Command output:

    Package    Version
    ---------- -------
    pip        22.0.4
    setuptools 58.1.0
    wheel      0.38.4

python:3.9 has a total of 3 packages installed.

# Prepare Postgres

Before solving the next questions, we need to ingest the green taxi trips dataset using the following commands. For some reason, GitHub does not allow us to wget the green_tripdata_2019-01.csv.gz described in the homework. So, I downloaded it manually, saved in the week1 directory and run the code ingest_data_green_taxi.py (see below). It is not a good practice to copy a code and just change a few lines to be able to run it for a new file. I only did that as a quickfix for this homework.
```
docker run -it \
    -e POSTGRES_USER="root" \
    -e POSTGRES_PASSWORD="root" \
    -e POSTGRES_DB="ny_taxi" \
    -v /home/padilha/projects/de-zoomcamp/ny_taxi_postgres_data:/var/lib/postgresql/data \
    -p 5432:5432 \
    --network=pg-network \
    --name=pg-database \
    postgres:13

python ingest_data_green_taxi.py
    --user=root
    --password=root
    --host=localhost
    --port=5432
    --db=ny_taxi
    --table_name=green_taxi_trips

python ingest_taxi_zone_lookup.py
```

## Question 3. Count records

Query:
```sql
SELECT
    CAST(lpep_pickup_datetime AS DATE),
    CAST(lpep_dropoff_datetime AS DATE),
    COUNT(1)
FROM
    green_taxi_trips
WHERE
    CAST(lpep_pickup_datetime AS DATE) = TO_DATE('2019-01-15', 'YYYY-MM-DD') AND
    CAST(lpep_dropoff_datetime AS DATE) = TO_DATE('2019-01-15', 'YYYY-MM-DD')
GROUP BY 1, 2;
```

Output:

    +----------------------+-----------------------+-------+
    | lpep_pickup_datetime | lpep_dropoff_datetime | count |
    |----------------------+-----------------------+-------|
    | 2019-01-15           | 2019-01-15            | 20530 |
    +----------------------+-----------------------+-------+
    SELECT 1
    Time: 0.079s

## Question 4. Largest trip for each day

Query:
```sql
SELECT
    CAST(lpep_pickup_datetime AS DATE),
    MAX(trip_distance)
FROM
    green_taxi_trips
WHERE
    CAST(lpep_pickup_datetime AS DATE) = TO_DATE('2019-01-18', 'YYYY-MM-DD') OR
    CAST(lpep_pickup_datetime AS DATE) = TO_DATE('2019-01-28', 'YYYY-MM-DD') OR
    CAST(lpep_pickup_datetime AS DATE) = TO_DATE('2019-01-15', 'YYYY-MM-DD') OR
    CAST(lpep_pickup_datetime AS DATE) = TO_DATE('2019-01-10', 'YYYY-MM-DD')
GROUP BY 1;
```

Output:

    +----------------------+--------+
    | lpep_pickup_datetime | max    |
    |----------------------+--------|
    | 2019-01-18           | 80.96  |
    | 2019-01-15           | 117.99 |
    | 2019-01-10           | 64.2   |
    | 2019-01-28           | 64.27  |
    +----------------------+--------+
    SELECT 4
    Time: 0.175s

## Question 5. The number of passengers

Since the question does not specify if we must consider lpep_pickup_datetime or lpep_dropoff_datetime, I consider lpep_pickup_datetime as in Question 4.

Query:
```sql
SELECT
    CAST(lpep_pickup_datetime AS DATE),
    passenger_count,
    COUNT(1)
FROM
    green_taxi_trips
WHERE
    CAST(lpep_pickup_datetime AS DATE) = TO_DATE('2019-01-01', 'YYYY-MM-DD')
GROUP BY 1, 2;
```

Output:

    +----------------------+-----------------+-------+
    | lpep_pickup_datetime | passenger_count | count |
    |----------------------+-----------------+-------|
    | 2019-01-01           | 0               | 21    |
    | 2019-01-01           | 1               | 12415 |
    | 2019-01-01           | 2               | 1282  |
    | 2019-01-01           | 3               | 254   |
    | 2019-01-01           | 4               | 129   |
    | 2019-01-01           | 5               | 616   |
    | 2019-01-01           | 6               | 273   |
    +----------------------+-----------------+-------+

In 2019-01-01 1282 trips had 2 passengers and 254 trips had 3 passengers.

## Question 6. Largest tip

Query:
```sql
SELECT
	MAX(tip_amount),
	zpu."Zone" AS "pickup_loc",
    zdo."Zone" AS "dropoff_loc"
FROM
	green_taxi_trips t
	JOIN zones zpu ON t."PULocationID" = zpu."LocationID"
	JOIN zones zdo ON t."DOLocationID" = zdo."LocationID"
WHERE
    zpu."Zone" = 'Astoria'
GROUP BY 2, 3
ORDER BY 1 DESC LIMIT 10;
```

Output:

    +-------+------------+-------------------------------+
    | max   | pickup_loc | dropoff_loc                   |
    |-------+------------+-------------------------------|
    | 88.0  | Astoria    | Long Island City/Queens Plaza |
    | 30.0  | Astoria    | Central Park                  |
    | 25.0  | Astoria    | <null>                        |
    | 25.0  | Astoria    | Jamaica                       |
    | 18.16 | Astoria    | Astoria                       |
    | 16.95 | Astoria    | Coney Island                  |
    | 15.0  | Astoria    | South Ozone Park              |
    | 14.96 | Astoria    | Marine Park/Mill Basin        |
    | 14.42 | Astoria    | Old Astoria                   |
    | 13.58 | Astoria    | Arrochar/Fort Wadsworth       |
    +-------+------------+-------------------------------+
    SELECT 10
    Time: 0.037s

For the passengers picked up in the Astoria Zone, Long Island City/Queens Plaza was the drop off zone that had the largest tip.

## Learning in public
[LinkedIn](https://www.linkedin.com/posts/victor-padilha_dataengineering-dataanalytics-dezoomcamp-activity-7023252179117350912-Ch-d?utm_source=share&utm_medium=member_desktop)
## Week 3 Overview

* [DE Zoomcamp 3.1.1 - Data Warehouse and BigQuery](#de-zoomcamp-311---data-warehouse-and-bigquery)

## [DE Zoomcamp 3.1.1 - Data Warehouse and BigQuery](https://www.youtube.com/watch?v=jrHljAoD6nM)

**On-Line Transaction Processing (OLTP) systems** are typically used in backend services, where sequences of SQL statements are grouped together in the form of transactions, which are rolled back if any of their statements fails. These systems deal with fast and small updates, store data in normalized databases that reduce data redundancy and increase productivity of end users.

**On-Line Analytical Processing (OLTP) systems** are composed by denormalized databases, which simplify analytics queries, and are mainly used for data mining. **Data Warehouses** are the main example in this category. They generally contain data from many sources (e.g., different OLTP systems) and implement [star](https://en.wikipedia.org/wiki/Star_schema) or [snowflake](https://en.wikipedia.org/wiki/Snowflake_schema) schemas that are optimized for analytical tasks.

**BigQuery** is a Data Warehouse solution from Google. Its main advantages are: no servers to manage or software to install; high scalability and availability; and builtin features like machine learning, geospatial analysis and business inteligence directly from the SQL interface.

BigQuery provides a lot of open source data. For example, we can search for the citibike_stations public data in BigQuery.

![](./img/citibike_stations1.png)

Then, click on the table and open a new query tab.

![](./img/citibike_stations2.png)

The new tab will have the following content.
```sql
SELECT  FROM `bigquery-public-data.new_york_citibike.citibike_stations` LIMIT 1000
```

For example, we can query the `station_id` and `name` fields from the citibike_stations table.
```sql
SELECT station_id, name FROM `bigquery-public-data.new_york_citibike.citibike_stations` LIMIT 1000;
```

Now we start in the practical part of BigQuery. First, we will create an external table. According to [BigQuery's documentation](https://cloud.google.com/bigquery/docs/external-data-sources):
* _External tables are similar to standard BigQuery tables, in that these tables store their metadata and schema in BigQuery storage. However, their data resides in an external source._
* _External tables are contained inside a dataset, and you manage them in the same way that you manage a standard BigQuery table._

Here, we create an external table for our yellow taxi trips data. In my case, I included the 12 parquet files of 2021. `dtc-de-375514` is the id of my project, `trips_data_all` is the name of my dataset and `external_yellow_tripdata` is the name of the external table that we are creating.
```sql
CREATE OR REPLACE EXTERNAL TABLE `dtc-de-375514.trips_data_all.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dtc_data_lake_dtc-de-375514/data/yellow/yellow_tripdata_2021-*.parquet']
);

SELECT * FROM `dtc-de-375514.trips_data_all.external_yellow_tripdata` limit 10;
```

### Partitioning in BigQuery
 
When we create a dataset, we generally have one or more columns that are used as some type of filter. In this case, we can partition a table based on such columns to improve BigQuery's performance. In this lesson, the instructor shows us an example of a dataset containing StackOverflow questions (left), and how the dataset would look like if it was partitioned by the `Creation_date` field (right).

Partitioning is a powerful feature of BigQuery. Suppose we want to query the questions created on a specific date. Partition improves processing, because BigQuery will not read or process any data from other dates. This improves efficiency and reduces querying costs.

![](./img/partition.png)

To illustrate the difference in performance, we first create a non partitioned data table from our dataset.
```sql
CREATE OR REPLACE TABLE `dtc-de-375514.trips_data_all.yellow_tripdata_non_partitioned` AS
SELECT * FROM `dtc-de-375514.trips_data_all.external_yellow_tripdata`;
```

For some reason, I got the following errors when running the command above.
    
    Error while reading table: dtc-de-375514.trips_data_all.external_yellow_tripdata, error message: Parquet column 'payment_type' has type DOUBLE which does not match the target cpp_type INT64. File: gs://dtc_data_lake_dtc-de-375514/data/yellow/yellow_tripdata_2021-02.parquet

    Error while reading table: dtc-de-375514.trips_data_all.external_yellow_tripdata, error message: Parquet column 'VendorID' has type DOUBLE which does not match the target cpp_type INT64. File: gs://dtc_data_lake_dtc-de-375514/data/yellow/yellow_tripdata_2021-02.parquet

Since in this example we are only interested in seeing the performance difference between a non partitioned table and a partitioned table, a quickfix for the SQL statement is:
```sql
CREATE OR REPLACE TABLE `dtc-de-375514.trips_data_all.yellow_tripdata_non_partitioned` AS
SELECT * REPLACE(
  CAST(0 AS NUMERIC) AS VendorID,
  CAST(0 AS NUMERIC) AS payment_type
) FROM `dtc-de-375514.trips_data_all.external_yellow_tripdata`;
```

Next, we create a partitioned table.
```sql
CREATE OR REPLACE TABLE `dtc-de-375514.trips_data_all.yellow_tripdata_partitioned`
PARTITION BY
  DATE(tpep_pickup_datetime) AS
SELECT * REPLACE(
  CAST(0 AS NUMERIC) AS VendorID,
  CAST(0 AS NUMERIC) AS payment_type
) FROM `dtc-de-375514.trips_data_all.external_yellow_tripdata`
```

Now, let's compare the difference in performance when querying non partitioned and partitioned data.
```sql
SELECT DISTINCT(PULocationID)
FROM `dtc-de-375514.trips_data_all.yellow_tripdata_non_partitioned`
WHERE DATE(tpep_pickup_datetime) BETWEEN '2021-01-01' AND '2021-06-30';
```

![](./img/result_non_partitioned.png)

```sql
SELECT DISTINCT(PULocationID)
FROM `dtc-de-375514.trips_data_all.yellow_tripdata_partitioned`
WHERE DATE(tpep_pickup_datetime) BETWEEN '2021-06-01' AND '2021-06-30';
```

![](./img/result_partitioned.png)

We can see the large difference in processing and billing (in this example, more than 10x improvement when using partitioned data).

Let's look into the partitions.
```sql
SELECT table_name, partition_id, total_rows
FROM trips_data_all.INFORMATION_SCHEMA.PARTITIONS
WHERE table_name = 'yellow_tripdata_partitioned'
ORDER BY total_rows DESC;
```

![](./img/information_schema_partitions.png)

### Clustering in BigQuery

We can cluster tables based on some field. In the StackOverflow example presented by the instructor, after partitioning questions by date, we may want to cluster them by tag in each partition. Clustering also helps us to reduce our costs and improve query performance. The field that we choose for clustering depends on how the data will be queried.

![](./img/clustering.png)

Creating a clustered data for our dataset.

```sql
CREATE OR REPLACE TABLE `dtc-de-375514.trips_data_all.yellow_tripdata_partitioned_clustered`
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY PULocationID AS
SELECT * REPLACE(
  CAST(0 AS NUMERIC) AS VendorID,
  CAST(0 AS NUMERIC) AS payment_type
) FROM `dtc-de-375514.trips_data_all.external_yellow_tripdata`;
```

Now, let's compare the difference in performance when querying unclustered and clustered data.
```sql
SELECT count(*) as trips
FROM `dtc-de-375514.trips_data_all.yellow_tripdata_partitioned`
WHERE DATE(tpep_pickup_datetime) BETWEEN '2021-01-01' and '2021-10-31'
AND PULocationID = 132;
```

![](./img/results_unclustered.png)

```sql
SELECT count(*) as trips
FROM `dtc-de-375514.trips_data_all.yellow_tripdata_partitioned_clustered`
WHERE DATE(tpep_pickup_datetime) BETWEEN '2021-01-01' and '2021-10-31'
AND PULocationID = 132;
```

![](./img/results_clustered.png)

We achieved ~8% of improvement in this example. As the dataset grows, this difference becomes more evident.
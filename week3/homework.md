## Week 3 Homework
<b><u>Important Note:</b></u> <p>You can load the data however you would like, but keep the files in .GZ Format. 
If you are using orchestration such as Airflow or Prefect do not load the data into Big Query using the orchestrator.</br> 
Stop with loading the files into a bucket. </br></br>
<u>NOTE:</u> You can use the CSV option for the GZ files when creating an External Table</br>

<b>SETUP:</b></br>
Create an external table using the fhv 2019 data. </br>
Create a table in BQ using the fhv 2019 data (do not partition or cluster this table). </br>
Data can be found here: https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv </p>

## Question 1:
What is the count for fhv vehicle records for year 2019?
- 65,623,481
- 43,244,696
- 22,978,333
- 13,942,414

### Solution

```sql
SELECT COUNT(1) FROM `dtc-de-375514.week3_homework.external_fhv_tripdata`;
```

**Answer:** 43,244,696

## Question 2:
Write a query to count the distinct number of affiliated_base_number for the entire dataset on both the tables.</br> 
What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

- 25.2 MB for the External Table and 100.87MB for the BQ Table
- 225.82 MB for the External Table and 47.60MB for the BQ Table
- 0 MB for the External Table and 0MB for the BQ Table
- 0 MB for the External Table and 317.94MB for the BQ Table

### Solution

```sql
SELECT COUNT(DISTINCT affiliated_base_number) FROM `dtc-de-375514.week3_homework.external_fhv_tripdata`;
```
Output: This query will process 0 B when run.

```sql
SELECT COUNT(DISTINCT affiliated_base_number) FROM `dtc-de-375514.week3_homework.fhv_tripdata`;
```
Output: This query will process 317.94 MB when run.

**Answer:** 0 MB for the External Table and 317.94MB for the BQ Table

## Question 3:
How many records have both a blank (null) PUlocationID and DOlocationID in the entire dataset?
- 717,748
- 1,215,687
- 5
- 20,332

### Solution

```sql
SELECT COUNT(1) from `dtc-de-375514.week3_homework.fhv_tripdata` WHERE PUlocationID IS NULL AND DOlocationID IS NULL;
```

**Answer:** 717,748

## Question 4:
What is the best strategy to optimize the table if query always filter by pickup_datetime and order by affiliated_base_number?
- Cluster on pickup_datetime Cluster on affiliated_base_number
- Partition by pickup_datetime Cluster on affiliated_base_number
- Partition by pickup_datetime Partition by affiliated_base_number
- Partition by affiliated_base_number Cluster on pickup_datetime

**Answer:** Partition by pickup_datetime Cluster on affiliated_base_number

## Question 5:
Implement the optimized solution you chose for question 4. Write a query to retrieve the distinct affiliated_base_number between pickup_datetime 2019/03/01 and 2019/03/31 (inclusive).</br> 
Use the BQ table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values? Choose the answer which most closely matches.
- 12.82 MB for non-partitioned table and 647.87 MB for the partitioned table
- 647.87 MB for non-partitioned table and 23.06 MB for the partitioned table
- 582.63 MB for non-partitioned table and 0 MB for the partitioned table
- 646.25 MB for non-partitioned table and 646.25 MB for the partitioned table

### Solution

Querying the non-partitioned table.
```sql
SELECT COUNT(DISTINCT affiliated_base_number)
FROM `dtc-de-375514.week3_homework.fhv_tripdata`
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' and '2019-03-31';
```

Creating and querying the partitioned and clustered table.
```sql
CREATE OR REPLACE TABLE `dtc-de-375514.week3_homework.fhv_tripdata_partitioned_clustered`
PARTITION BY DATE(pickup_datetime)
CLUSTER BY Affiliated_base_number AS
SELECT * FROM `dtc-de-375514.week3_homework.fhv_tripdata`;

SELECT COUNT(DISTINCT affiliated_base_number)
FROM `dtc-de-375514.week3_homework.fhv_tripdata_partitioned_clustered`
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' and '2019-03-31';
```

**Answer:** 647.87 MB for non-partitioned table and 23.06 MB for the partitioned table

## Question 6: 
Where is the data stored in the External Table you created?

- Big Query
- GCP Bucket
- Container Registry
- Big Table

**Answer:** GCP Bucket

## Question 7:
It is best practice in Big Query to always cluster your data:
- True
- False

**Answer:** False. For instance, if the cardinality of the number of values in a column or group of columns is small.

## (Not required) Question 8:
A better format to store these files may be parquet. Create a data pipeline to download the gzip files and convert them into parquet. Upload the files to your GCP Bucket and create an External and BQ Table. 


Note: Column types for all files used in an External Table must have the same datatype. While an External Table may be created and shown in the side panel in Big Query, this will need to be validated by running a count query on the External Table to check if any errors occur. 
 
## Submitting the solutions

* Form for submitting: https://forms.gle/rLdvQW2igsAT73HTA
* You can submit your homework multiple times. In this case, only the last submission will be used. 

Deadline: 13 February (Monday), 22:00 CET


## Solution

We will publish the solution here

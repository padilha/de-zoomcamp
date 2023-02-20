## Week 5 Overview

* [DE Zoomcamp 5.1.1 - Introduction to Batch processing](#de-zoomcamp-511---introduction-to-batch-processing)
* [DE Zoomcamp 5.1.2 - Introduction to Spark](#de-zoomcamp-512---introduction-to-spark)
* [DE Zoomcamp 5.3.1 - First Look at Spark/PySpark](#de-zoomcamp-531---first-look-at-sparkpyspark)
* [DE Zoomcamp 5.3.2 - Spark DataFrames](#de-zoomcamp-532---spark-dataframes)

## [DE Zoomcamp 5.1.1 - Introduction to Batch processing](https://www.youtube.com/watch?v=dcHe5Fl3MF8&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb)

Batch jobs are routines that are run in regular intervals. The most common types of batch jobs are either daily or hourly jobs. Batch jobs process data after the reference time period is over (e.g., after a day ends, a batch job processes all data that was gathered in that day). Batch jobs are easy to manage, retry (which also helps for fault tolerance) and scale. The main disadvantage is that we won't have the most recent data available, since we need to wait for an interval to end and our batch workflows run before being able to do anything with such data.

## [DE Zoomcamp 5.1.2 - Introduction to Spark](https://www.youtube.com/watch?v=FhaqbEOuQ8U&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb)

Apache Spark is a data processing engine (i.e., the processing happens _in_ Spark) that is normally used for batch processing (although it can also be used for streaming).

Spark typically pulls data from a data lake, performs some processing steps and stores the data back in the data lake. Spark can be used for tasks where we would use SQL. However, the engine is recommended for dealing directly with files and in situations where we need more flexibility than SQL offers, such as: if we want to split our code into different modules, write unit tests or even some functionality that may not be possible to write using SQL (e.g., machine learning-related routines, such as training and/or using a model).

In the lesson, Alexey Grigorev gives us the following advice: _"If you can express something with SQL, you should go with SQL. But for cases where you cannot you should go with Spark"_.

## [DE Zoomcamp 5.3.1 - First Look at Spark/PySpark](https://www.youtube.com/watch?v=r_Sf6fCB40c&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb)

To be able to use PySpark, I created a conda environment and installed it using the following commands:
```
conda install openjdk
conda install pyspark
```

**Step 1:** `SparkSession` is the entry point into all functionality in Spark. We usually create a `SparkSession` as in the following example:
```python
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()
```

We can analyze the code above in smaller parts:
* `builder` is a class attribute that is and instance of `pyspark.sql.session.SparkSession.Builder` that is used to construct `SparkSession` instances.
* `master("local[*]")` sets the Spark master URL to connect to. In this example, `local` refers to the local machine and `[*]` tells Spark to use all available cores (e.g., if we wanted to use only 2 cores, we would write `local[2]`).
* `appName('test')` sets the application name that is shown in Spark UI.
* `getOrCreate()` returns an existing `SparkSession`, if available, or creates a new one.

**Step 2:** read the fhvhv_tripdata_2021-01.csv.gz (available [here](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhvhv)).
```python
df = spark.read \
    .option("header", "true") \
    .option("inferSchema",True) \
    .csv('fhvhv_tripdata_2021-01.csv.gz')
df.show(5)
```

**Step 3:** check the inferred schema through `df.schema`. For the fhvhv tripdata, we get the following output:
```
StructType(
    List(
        StructField(hvfhs_license_num,StringType,true),
        StructField(dispatching_base_num,StringType,true),
        StructField(pickup_datetime,StringType,true),
        StructField(dropoff_datetime,StringType,true),
        StructField(PULocationID,IntegerType,true),
        StructField(DOLocationID,IntegerType,true),
        StructField(SR_Flag,IntegerType,true)
    )
)
```

It is possible to observe that `pickup_datetime` and `dropoff_datetime` were both interpreted as strings. Furthermore, if we haven't used `inferSchema`, Spark would interpret `PULocationID` and `DOLocationID` as LongTypes (which use 8 bytes instead of the 4 bytes used by IntegerType). When these things happen, we may want to achieve better performance and intepret all fields correctly. Then, we can specify the expected schema:
```python
from pyspark.sql import types
schema = types.StructType(
    [
        types.StructField('hvfhs_license_num', types.StringType(), True),
        types.StructField('dispatching_base_num', types.StringType(), True),
        types.StructField('pickup_datetime', types.TimestampType(), True),
        types.StructField('dropoff_datetime', types.TimestampType(), True),
        types.StructField('PULocationID', types.IntegerType(), True),
        types.StructField('DOLocationID', types.IntegerType(), True),
        types.StructField('SR_Flag', types.IntegerType(), True)
    ]
)

df = spark.read \
    .option("header", "true") \
    .option("inferSchema",True) \
    .csv('fhvhv_tripdata_2021-01.csv.gz')

df.schema
```

**Step 4:** save DataFrame as parquet. As explained by the instructor, it is not good to have a smaller number of files than CPUs (because only a subset of CPUs will be used and the remaining will be idle). For such, we first use the repartition method and then save the data as parquet. Suppose we have 8 cores, then we can repartition our dataset into 24 parts.
```
df = df.repartition(24)
df.write.parquet('fhvhv/2021/01/')
```

Then, if we list the output directory, we should get an output similar to this one:

    part-00000-d7d0e901-04fe-46b9-95a6-aea2a107e8e1-c000.snappy.parquet
    part-00001-d7d0e901-04fe-46b9-95a6-aea2a107e8e1-c000.snappy.parquet
    part-00002-d7d0e901-04fe-46b9-95a6-aea2a107e8e1-c000.snappy.parquet
    part-00003-d7d0e901-04fe-46b9-95a6-aea2a107e8e1-c000.snappy.parquet
    part-00004-d7d0e901-04fe-46b9-95a6-aea2a107e8e1-c000.snappy.parquet
    part-00005-d7d0e901-04fe-46b9-95a6-aea2a107e8e1-c000.snappy.parquet
    part-00006-d7d0e901-04fe-46b9-95a6-aea2a107e8e1-c000.snappy.parquet
    part-00007-d7d0e901-04fe-46b9-95a6-aea2a107e8e1-c000.snappy.parquet
    part-00008-d7d0e901-04fe-46b9-95a6-aea2a107e8e1-c000.snappy.parquet
    part-00009-d7d0e901-04fe-46b9-95a6-aea2a107e8e1-c000.snappy.parquet
    part-00010-d7d0e901-04fe-46b9-95a6-aea2a107e8e1-c000.snappy.parquet
    part-00011-d7d0e901-04fe-46b9-95a6-aea2a107e8e1-c000.snappy.parquet
    part-00012-d7d0e901-04fe-46b9-95a6-aea2a107e8e1-c000.snappy.parquet
    part-00013-d7d0e901-04fe-46b9-95a6-aea2a107e8e1-c000.snappy.parquet
    part-00014-d7d0e901-04fe-46b9-95a6-aea2a107e8e1-c000.snappy.parquet
    part-00015-d7d0e901-04fe-46b9-95a6-aea2a107e8e1-c000.snappy.parquet
    part-00016-d7d0e901-04fe-46b9-95a6-aea2a107e8e1-c000.snappy.parquet
    part-00017-d7d0e901-04fe-46b9-95a6-aea2a107e8e1-c000.snappy.parquet
    part-00018-d7d0e901-04fe-46b9-95a6-aea2a107e8e1-c000.snappy.parquet
    part-00019-d7d0e901-04fe-46b9-95a6-aea2a107e8e1-c000.snappy.parquet
    part-00020-d7d0e901-04fe-46b9-95a6-aea2a107e8e1-c000.snappy.parquet
    part-00021-d7d0e901-04fe-46b9-95a6-aea2a107e8e1-c000.snappy.parquet
    part-00022-d7d0e901-04fe-46b9-95a6-aea2a107e8e1-c000.snappy.parquet
    part-00023-d7d0e901-04fe-46b9-95a6-aea2a107e8e1-c000.snappy.parquet
    _SUCCESS

## [DE Zoomcamp 5.3.2 - Spark DataFrames](https://www.youtube.com/watch?v=ti3aC1m3rE8&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb)

We start by reading the parquet data that we saved in the end of the last lesson.
```python
df = spark.read.parquet('fhvhv/2021/01/')
```

We can print the schema, which is also contained in the files and is interpreted by Spark, using `df.printSchema()` and get the output below.

    root
    |-- hvfhs_license_num: string (nullable = true)
    |-- dispatching_base_num: string (nullable = true)
    |-- pickup_datetime: timestamp (nullable = true)
    |-- dropoff_datetime: timestamp (nullable = true)
    |-- PULocationID: integer (nullable = true)
    |-- DOLocationID: integer (nullable = true)
    |-- SR_Flag: integer (nullable = true)

Next, we can use the `select()` and `filter()` methods to execute simple queries on our DataFrame.
```python
df.select('pickup_datetime', 'dropoff_datetime', 'PULocationID', 'DOLocationID') \
    .filter(df.hvfhs_license_num == 'HV0003')
```
In the code above we selected 4 columns where `hvfhs_license_num` is equal to 'HV0003'.

### Actions vs. Transformations

In Spark, we have a distinction between Actions (code that is executed immediately) and Transformations (code that is lazy, i.e., not executed immediately).

Examples of Transformations are: selecting columns, data filtering, joins and groupby operations. In these cases, Spark creates a sequence of transformations that is executed only when we call some method like `show()`, which is an example of an Action.

Examples of Actions are: `show()`, `take()`, `head()`, `write()`, etc.

### Spark SQL Functions

Spark has many predefined SQL-like functions.
```python
from pyspark.sql import functions as F
df \
    .withColumn('pickup_date', F.to_date(df.pickup_datetime)) \
    .withColumn('dropoff_date', F.to_date(df.dropoff_datetime)) \
    .select('pickup_date', 'dropoff_date', 'PULocationID', 'DOLocationID') \
    .show()
```

In addition, we can also define custom functions to run on our DataFrames.
```python
def crazy_stuff(base_num):
    num = int(base_num[1:])
    if num % 7 == 0:
        return f's/{num:03x}'
    elif num % 3 == 0:
        return f'a/{num:03x}'
    return f'e/{num:03x}'

crazy_stuff_udf = F.udf(crazy_stuff, returnType=types.StringType())

df \
    .withColumn('pickup_date', F.to_date(df.pickup_datetime)) \
    .withColumn('dropoff_date', F.to_date(df.dropoff_datetime)) \
    .withColumn('base_id', crazy_stuff_udf(df.dispatching_base_num)) \
    .select('base_id', 'pickup_date', 'dropoff_date', 'PULocationID', 'DOLocationID') \
    .show()
```
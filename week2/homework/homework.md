## Week 2 Homework

The goal of this homework is to familiarise users with workflow orchestration and observation. 


## Question 1. Load January 2020 data

Using the `etl_web_to_gcs.py` flow that loads taxi data into GCS as a guide, create a flow that loads the green taxi CSV dataset for January 2020 into GCS and run it. Look at the logs to find out how many rows the dataset has.

How many rows does that dataset have?

* 447,770
* 766,792
* 299,234
* 822,132

### Solution

After making minor changes for the script to work for green taxi data, we have this output:

    18:59:08.489 | INFO    | prefect.engine - Created flow run 'fantastic-starling' for flow 'etl-web-to-gcs'
    18:59:08.647 | INFO    | Flow run 'fantastic-starling' - Created task run 'fetch-b4598a4a-0' for task 'fetch'
    18:59:08.648 | INFO    | Flow run 'fantastic-starling' - Executing 'fetch-b4598a4a-0' immediately...
    /home/padilha/projects/de-zoomcamp/week2/homework/etl_web_to_gcs.py:14: DtypeWarning: Columns (3) have mixed types. Specify dtype option on import or set low_memory=False.
    df = pd.read_csv(dataset_url)
    18:59:09.582 | INFO    | Task run 'fetch-b4598a4a-0' - Finished in state Completed()
    18:59:09.602 | INFO    | Flow run 'fantastic-starling' - Created task run 'clean-b9fd7e03-0' for task 'clean'
    18:59:09.602 | INFO    | Flow run 'fantastic-starling' - Executing 'clean-b9fd7e03-0' immediately...
    18:59:09.721 | INFO    | Task run 'clean-b9fd7e03-0' -    VendorID lpep_pickup_datetime lpep_dropoff_datetime  ... payment_type  trip_type  congestion_surcharge
    0       2.0  2019-12-18 15:52:30   2019-12-18 15:54:39  ...          1.0        1.0                   0.0
    1       2.0  2020-01-01 00:45:58   2020-01-01 00:56:39  ...          1.0        2.0                   0.0

    [2 rows x 20 columns]
    18:59:09.722 | INFO    | Task run 'clean-b9fd7e03-0' - columns: VendorID                        float64
    lpep_pickup_datetime     datetime64[ns]
    lpep_dropoff_datetime    datetime64[ns]
    store_and_fwd_flag               object
    RatecodeID                      float64
    PULocationID                      int64
    DOLocationID                      int64
    passenger_count                 float64
    trip_distance                   float64
    fare_amount                     float64
    extra                           float64
    mta_tax                         float64
    tip_amount                      float64
    tolls_amount                    float64
    ehail_fee                       float64
    improvement_surcharge           float64
    total_amount                    float64
    payment_type                    float64
    trip_type                       float64
    congestion_surcharge            float64
    dtype: object
    18:59:09.723 | INFO    | Task run 'clean-b9fd7e03-0' - rows: 447770
    18:59:09.743 | INFO    | Task run 'clean-b9fd7e03-0' - Finished in state Completed()
    18:59:09.760 | INFO    | Flow run 'fantastic-starling' - Created task run 'write_local-f322d1be-0' for task 'write_local'
    18:59:09.761 | INFO    | Flow run 'fantastic-starling' - Executing 'write_local-f322d1be-0' immediately...
    18:59:10.475 | INFO    | Task run 'write_local-f322d1be-0' - Finished in state Completed()
    18:59:10.496 | INFO    | Flow run 'fantastic-starling' - Created task run 'write_gcs-1145c921-0' for task 'write_gcs'
    18:59:10.496 | INFO    | Flow run 'fantastic-starling' - Executing 'write_gcs-1145c921-0' immediately...
    18:59:10.606 | INFO    | Task run 'write_gcs-1145c921-0' - Getting bucket 'dtc_data_lake_dtc-de-375514'.
    18:59:11.779 | INFO    | Task run 'write_gcs-1145c921-0' - Uploading from PosixPath('data/green/green_tripdata_2020-01.parquet') to the bucket 'dtc_data_lake_dtc-de-375514' path 'data/green/green_tripdata_2020-01.parquet'.
    18:59:15.829 | INFO    | Task run 'write_gcs-1145c921-0' - Finished in state Completed()
    18:59:15.863 | INFO    | Flow run 'fantastic-starling' - Finished in state Completed('All states completed.')

We have the output: `18:59:09.723 | INFO    | Task run 'clean-b9fd7e03-0' - rows: 447770`.

**Answer:** 447,770


## Question 2. Scheduling with Cron

Cron is a common scheduling specification for workflows. 

Using the flow in `etl_web_to_gcs.py`, create a deployment to run on the first of every month at 5am UTC. What’s the cron schedule for that?

- `0 5 1 * *`
- `0 0 5 1 *`
- `5 * 1 0 *`
- `* * 5 1 0`

### Solution

Let's build and deploy our workflow.
```
prefect deployment build ./etl_web_to_gcs.py:etl_web_to_gcs -n "Week2HomeworkQ2"
prefect deployment apply etl_web_to_gcs-deployment.yaml
```

Then, in Orion's interface we can go to Deployments and edit the schedule of Week2HomeworkQ2. The first option (`0 5 1 * *`) is the correct cron expression.

![](../img/homework_Q2.png)

**Answer:** `0 5 1 * *`


## Question 3. Loading data to BigQuery 

Using `etl_gcs_to_bq.py` as a starting point, modify the script for extracting data from GCS and loading it into BigQuery. This new script should not fill or remove rows with missing values. (The script is really just doing the E and L parts of ETL).

The main flow should print the total number of rows processed by the script. Set the flow decorator to log the print statement.

Parametrize the entrypoint flow to accept a list of months, a year, and a taxi color. 

Make any other necessary changes to the code for it to function as required.

Create a deployment for this flow to run in a local subprocess with local flow code storage (the defaults).

Make sure you have the parquet data files for Yellow taxi data for Feb. 2019 and March 2019 loaded in GCS. Run your deployment to append this data to your BiqQuery table. How many rows did your flow code process?

- 14,851,920
- 12,282,990
- 27,235,753
- 11,338,483

### Solution

**Step 1:** the original csv.gz files for Yellow taxi data for Feb. 2019 and March 2019 can be downloaded using the following commands:
```
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2019-02.csv.gz
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2019-03.csv.gz
```

Since it's just 2 files, I converted them to parquet using pandas and manually uploaded them to my GCS bucket.

**Step 2:** let's build and deploy our workflow.
```
prefect deployment build ./etl_gcs_to_bq.py:etl_gcs_to_bq -n "Week2HomeworkQ3"
prefect deployment apply etl_gcs_to_bq-deployment.yaml
```

**Step 3:** trigger a quick run using the interface and launch an agent using the terminal.
```
prefect agent start -q 'default'
```

Then, we have the following output:

    21:46:40.202 | INFO    | prefect.agent - Submitting flow run '71aad507-121d-4a8c-9939-b0a3c92d52d6'
    21:46:40.264 | INFO    | prefect.infrastructure.process - Opening process 'bizarre-squirrel'...
    21:46:40.283 | INFO    | prefect.agent - Completed submission of flow run '71aad507-121d-4a8c-9939-b0a3c92d52d6'
    /home/padilha/miniconda3/envs/de-zoomcamp-week2/lib/python3.9/runpy.py:127: RuntimeWarning: 'prefect.engine' found in sys.modules after import of package 'prefect', but prior to execution of 'prefect.engine'; this may result in unpredictable behaviour
    warn(RuntimeWarning(msg))
    21:46:41.441 | INFO    | Flow run 'bizarre-squirrel' - Downloading flow code from storage at '/home/padilha/projects/de-zoomcamp/week2/homework'
    21:46:41.560 | INFO    | Flow run 'bizarre-squirrel' - Created task run 'extract_from_gcs-272ec809-0' for task 'extract_from_gcs'
    21:46:41.561 | INFO    | Flow run 'bizarre-squirrel' - Executing 'extract_from_gcs-272ec809-0' immediately...
    21:46:42.752 | INFO    | Task run 'extract_from_gcs-272ec809-0' - Downloading blob named data/yellow/yellow_tripdata_2019-02.parquet from the dtc_data_lake_dtc-de-375514 bucket to ../data/data/yellow/yellow_tripdata_2019-02.parquet
    21:46:52.451 | INFO    | Task run 'extract_from_gcs-272ec809-0' - Finished in state Completed()
    21:46:53.915 | INFO    | Flow run 'bizarre-squirrel' - yellow 2019-2 rows: 7019375
    21:46:53.934 | INFO    | Flow run 'bizarre-squirrel' - Created task run 'write_bq-f3b17cf5-0' for task 'write_bq'
    21:46:53.935 | INFO    | Flow run 'bizarre-squirrel' - Executing 'write_bq-f3b17cf5-0' immediately...
    21:47:37.201 | INFO    | Task run 'write_bq-f3b17cf5-0' - Finished in state Completed()
    21:47:37.235 | INFO    | Flow run 'bizarre-squirrel' - Created task run 'extract_from_gcs-272ec809-1' for task 'extract_from_gcs'
    21:47:37.236 | INFO    | Flow run 'bizarre-squirrel' - Executing 'extract_from_gcs-272ec809-1' immediately...
    21:47:38.456 | INFO    | Task run 'extract_from_gcs-272ec809-1' - Downloading blob named data/yellow/yellow_tripdata_2019-03.parquet from the dtc_data_lake_dtc-de-375514 bucket to ../data/data/yellow/yellow_tripdata_2019-03.parquet
    21:47:53.287 | INFO    | Task run 'extract_from_gcs-272ec809-1' - Finished in state Completed()
    21:47:54.996 | INFO    | Flow run 'bizarre-squirrel' - yellow 2019-3 rows: 7832545
    21:47:55.020 | INFO    | Flow run 'bizarre-squirrel' - Created task run 'write_bq-f3b17cf5-1' for task 'write_bq'
    21:47:55.020 | INFO    | Flow run 'bizarre-squirrel' - Executing 'write_bq-f3b17cf5-1' immediately...
    21:48:36.496 | INFO    | Task run 'write_bq-f3b17cf5-1' - Finished in state Completed()
    21:48:36.498 | INFO    | Flow run 'bizarre-squirrel' - Processed rows (total): 14851920
    21:48:36.546 | INFO    | Flow run 'bizarre-squirrel' - Finished in state Completed('All states completed.')
    21:48:36.894 | INFO    | prefect.infrastructure.process - Process 'bizarre-squirrel' exited cleanly.

We can see the final result in `21:48:36.498 | INFO    | Flow run 'bizarre-squirrel' - Processed rows (total): 14851920`.

**Answer:** 14,851,920


## Question 4. Github Storage Block

Using the `web_to_gcs` script from the videos as a guide, you want to store your flow code in a GitHub repository for collaboration with your team. Prefect can look in the GitHub repo to find your flow code and read it. Create a GitHub storage block from the UI or in Python code and use that in your Deployment instead of storing your flow code locally or baking your flow code into a Docker image. 

Note that you will have to push your code to GitHub, Prefect will not push it for you.

Run your deployment in a local subprocess (the default if you don’t specify an infrastructure). Use the Green taxi data for the month of November 2020.

How many rows were processed by the script?

- 88,019
- 192,297
- 88,605
- 190,225

### Solution

From the root directory of this repo run:
```
prefect deployment build ./week2/homework/etl_web_to_gcs.py:etl_web_to_gcs -n "GitHub Storage Flow" -sb github/week2q4 -o ./week2/homework/etl_web_to_gcs_github-deployment.yaml --apply
prefect deployment run "etl-web-to-gcs/GitHub Storage Flow"
prefect agent start -q 'default'
```

And we get the output below:

    22:32:33.555 | INFO    | prefect.agent - Submitting flow run '85bca7d3-4a7f-4bc8-bee9-a762edf238e9'
    22:32:33.651 | INFO    | prefect.infrastructure.process - Opening process 'colorful-bee'...
    22:32:33.672 | INFO    | prefect.agent - Completed submission of flow run '85bca7d3-4a7f-4bc8-bee9-a762edf238e9'
    /home/padilha/miniconda3/envs/de-zoomcamp-week2/lib/python3.9/runpy.py:127: RuntimeWarning: 'prefect.engine' found in sys.modules after import of package 'prefect', but prior to execution of 'prefect.engine'; this may result in unpredictable behaviour
    warn(RuntimeWarning(msg))
    22:32:34.749 | INFO    | Flow run 'colorful-bee' - Downloading flow code from storage at ''
    22:32:36.018 | INFO    | Flow run 'colorful-bee' - Created task run 'fetch-ba00c645-0' for task 'fetch'
    22:32:36.019 | INFO    | Flow run 'colorful-bee' - Executing 'fetch-ba00c645-0' immediately...
    week2/homework/etl_web_to_gcs.py:14: DtypeWarning: Columns (3) have mixed types. Specify dtype option on import or set low_memory=False.
    df = pd.read_csv(dataset_url)
    22:32:38.152 | INFO    | Task run 'fetch-ba00c645-0' - Finished in state Completed()
    22:32:38.200 | INFO    | Flow run 'colorful-bee' - Created task run 'clean-2c6af9f6-0' for task 'clean'
    22:32:38.201 | INFO    | Flow run 'colorful-bee' - Executing 'clean-2c6af9f6-0' immediately...
    22:32:38.305 | INFO    | Task run 'clean-2c6af9f6-0' -    VendorID lpep_pickup_datetime  ... trip_type congestion_surcharge
    0       2.0  2020-11-01 00:08:23  ...       1.0                 2.75
    1       2.0  2020-11-01 00:23:32  ...       1.0                 0.00

    [2 rows x 20 columns]
    22:32:38.306 | INFO    | Task run 'clean-2c6af9f6-0' - columns: VendorID                        float64
    lpep_pickup_datetime     datetime64[ns]
    lpep_dropoff_datetime    datetime64[ns]
    store_and_fwd_flag               object
    RatecodeID                      float64
    PULocationID                      int64
    DOLocationID                      int64
    passenger_count                 float64
    trip_distance                   float64
    fare_amount                     float64
    extra                           float64
    mta_tax                         float64
    tip_amount                      float64
    tolls_amount                    float64
    ehail_fee                       float64
    improvement_surcharge           float64
    total_amount                    float64
    payment_type                    float64
    trip_type                       float64
    congestion_surcharge            float64
    dtype: object
    22:32:38.307 | INFO    | Task run 'clean-2c6af9f6-0' - rows: 88605
    22:32:38.321 | INFO    | Task run 'clean-2c6af9f6-0' - Finished in state Completed()
    22:32:38.337 | INFO    | Flow run 'colorful-bee' - Created task run 'write_local-09e9d2b8-0' for task 'write_local'
    22:32:38.338 | INFO    | Flow run 'colorful-bee' - Executing 'write_local-09e9d2b8-0' immediately...
    22:32:38.571 | INFO    | Task run 'write_local-09e9d2b8-0' - Finished in state Completed()
    22:32:38.600 | INFO    | Flow run 'colorful-bee' - Created task run 'write_gcs-67f8f48e-0' for task 'write_gcs'
    22:32:38.602 | INFO    | Flow run 'colorful-bee' - Executing 'write_gcs-67f8f48e-0' immediately...
    22:32:38.708 | INFO    | Task run 'write_gcs-67f8f48e-0' - Getting bucket 'dtc_data_lake_dtc-de-375514'.
    22:32:39.897 | INFO    | Task run 'write_gcs-67f8f48e-0' - Uploading from PosixPath('/home/padilha/projects/de-zoomcamp/data/green/green_tripdata_2020-11.parquet') to the bucket 'dtc_data_lake_dtc-de-375514' path '/home/padilha/projects/de-zoomcamp/data/green/green_tripdata_2020-11.parquet'.
    22:32:41.976 | INFO    | Task run 'write_gcs-67f8f48e-0' - Finished in state Completed()
    22:32:42.018 | INFO    | Flow run 'colorful-bee' - Finished in state Completed('All states completed.')
    22:32:42.246 | INFO    | prefect.infrastructure.process - Process 'colorful-bee' exited cleanly.

The result is in line `22:32:38.307 | INFO    | Task run 'clean-2c6af9f6-0' - rows: 88605`.

**Answer:** 88,605


## Question 5. Email or Slack notifications

Q5. It’s often helpful to be notified when something with your dataflow doesn’t work as planned. Choose one of the options below for creating email or slack notifications.

The hosted Prefect Cloud lets you avoid running your own server and has Automations that allow you to get notifications when certain events occur or don’t occur. 

Create a free forever Prefect Cloud account at app.prefect.cloud and connect your workspace to it following the steps in the UI when you sign up. 

Set up an Automation that will send yourself an email when a flow run completes. Run the deployment used in Q4 for the Green taxi data for April 2019. Check your email to see the notification.

Alternatively, use a Prefect Cloud Automation or a self-hosted Orion server Notification to get notifications in a Slack workspace via an incoming webhook. 

Join my temporary Slack workspace with [this link](https://join.slack.com/t/temp-notify/shared_invite/zt-1odklt4wh-hH~b89HN8MjMrPGEaOlxIw). 400 people can use this link and it expires in 90 days. 

In the Prefect Cloud UI create an [Automation](https://docs.prefect.io/ui/automations) or in the Prefect Orion UI create a [Notification](https://docs.prefect.io/ui/notifications/) to send a Slack message when a flow run enters a Completed state. Here is the Webhook URL to use: https://hooks.slack.com/services/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

Test the functionality.

Alternatively, you can grab the webhook URL from your own Slack workspace and Slack App that you create. 


How many rows were processed by the script?

- `125,268`
- `377,922`
- `728,390`
- `514,392`

### Solution

**Step 1:** sign in to app.prefect.cloud and create an API key.

**Step 2:** sign in from terminal.
```
prefect cloud login -k your-api-key-here
```

**Step 3:** build and deploy the workflow.
```
prefect deployment build ./etl_web_to_gcs.py:etl_web_to_gcs -n "Prefect Cloud Flow" --apply
```

**Step 4:** create an e-mail block.

![](../img/homework_Q5_1.png)

Then, in app.prefect.cloud select "Automations > Add Automation +".

![](../img/homework_Q5_2.png)

![](../img/homework_Q5_3.png)

**Step 5:** run the deployed workflow.
```
prefect deployment run "etl-web-to-gcs/Prefect Cloud Flow"
prefect agent start --work-queue "default"
```

Output:

    Agent started! Looking for work from queue(s): default...
    09:59:27.087 | INFO    | prefect.agent - Submitting flow run '91ab8db6-c1de-46b1-9f03-7f8767bac682'
    09:59:28.008 | INFO    | prefect.infrastructure.process - Opening process 'bulky-moose'...
    09:59:28.216 | INFO    | prefect.agent - Completed submission of flow run '91ab8db6-c1de-46b1-9f03-7f8767bac682'
    /home/padilha/miniconda3/envs/de-zoomcamp-week2/lib/python3.9/runpy.py:127: RuntimeWarning: 'prefect.engine' found in sys.modules after import of package 'prefect', but prior to execution of 'prefect.engine'; this may result in unpredictable behaviour
    warn(RuntimeWarning(msg))
    09:59:31.962 | INFO    | Flow run 'bulky-moose' - Downloading flow code from storage at '/home/padilha/projects/de-zoomcamp/week2/homework'
    09:59:33.946 | INFO    | Flow run 'bulky-moose' - Created task run 'fetch-ba00c645-0' for task 'fetch'
    09:59:33.947 | INFO    | Flow run 'bulky-moose' - Executing 'fetch-ba00c645-0' immediately...
    09:59:39.783 | INFO    | Task run 'fetch-ba00c645-0' - Finished in state Completed()
    09:59:39.978 | INFO    | Flow run 'bulky-moose' - Created task run 'clean-2c6af9f6-0' for task 'clean'
    09:59:39.979 | INFO    | Flow run 'bulky-moose' - Executing 'clean-2c6af9f6-0' immediately...
    09:59:40.919 | INFO    | Task run 'clean-2c6af9f6-0' -    VendorID lpep_pickup_datetime  ... trip_type congestion_surcharge
    0         2  2019-04-01 00:18:40  ...         1                 2.75
    1         2  2019-04-01 00:18:24  ...         1                 0.00

    [2 rows x 20 columns]
    09:59:40.921 | INFO    | Task run 'clean-2c6af9f6-0' - columns: VendorID                          int64
    lpep_pickup_datetime     datetime64[ns]
    lpep_dropoff_datetime    datetime64[ns]
    store_and_fwd_flag               object
    RatecodeID                        int64
    PULocationID                      int64
    DOLocationID                      int64
    passenger_count                   int64
    trip_distance                   float64
    fare_amount                     float64
    extra                           float64
    mta_tax                         float64
    tip_amount                      float64
    tolls_amount                    float64
    ehail_fee                       float64
    improvement_surcharge           float64
    total_amount                    float64
    payment_type                      int64
    trip_type                         int64
    congestion_surcharge            float64
    dtype: object
    09:59:40.923 | INFO    | Task run 'clean-2c6af9f6-0' - rows: 514392
    09:59:41.098 | INFO    | Task run 'clean-2c6af9f6-0' - Finished in state Completed()
    09:59:41.099 | INFO    | Flow run 'bulky-moose' - Processed rows: 514392
    09:59:41.329 | INFO    | Flow run 'bulky-moose' - Created task run 'write_local-09e9d2b8-0' for task 'write_local'
    09:59:41.331 | INFO    | Flow run 'bulky-moose' - Executing 'write_local-09e9d2b8-0' immediately...
    09:59:44.698 | INFO    | Task run 'write_local-09e9d2b8-0' - Finished in state Completed()
    09:59:44.987 | INFO    | Flow run 'bulky-moose' - Created task run 'write_gcs-67f8f48e-0' for task 'write_gcs'
    09:59:44.988 | INFO    | Flow run 'bulky-moose' - Executing 'write_gcs-67f8f48e-0' immediately...
    09:59:45.909 | INFO    | Flow run 'bulky-moose' - Traceback (most recent call last):

We have the result in `09:59:40.923 | INFO    | Task run 'clean-2c6af9f6-0' - rows: 514392`

The workflow raises an exception because it cannot find a GCS block named zoomcamp-gcs. When I checked my e-mail, I got Prefect's notification:

![](../img/homework_Q5_4.png)

**Answer:** 514,392


## Question 6. Secrets

Prefect Secret blocks provide secure, encrypted storage in the database and obfuscation in the UI. Create a secret block in the UI that stores a fake 10-digit password to connect to a third-party service. Once you’ve created your block in the UI, how many characters are shown as asterisks (*) on the next page of the UI?

- 5
- 6
- 8
- 10

### Solution

I created a Database Credentials block in Orion. When I visualize the block, there are 8 asterisks.

![](../img/homework_Q6.png)

**Answer:** 8


## Submitting the solutions

* Form for submitting: https://forms.gle/PY8mBEGXJ1RvmTM97
* You can submit your homework multiple times. In this case, only the last submission will be used. 

Deadline: 6 February (Monday), 22:00 CET


## Solution

We will publish the solution here

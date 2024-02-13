-- Query public available table
SELECT station_id, name FROM
    bigquery-public-data.new_york_citibike.citibike_stations
LIMIT 100;


-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE
  `datatalks-de-351308.trips_data_all.external_green_tripdata`
  OPTIONS(
    format ="PARQUET",
    uris = ['gs://dtc_data_lake_datatalks-de-351308/green_trips_2022.parquet']
    );

-- Check green trip data
SELECT * FROM datatalks-de-351308.trips_data_all.external_green_tripdata limit 10;

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE datatalks-de-351308.trips_data_all.green_tripdata_non_partitoned AS
SELECT * FROM datatalks-de-351308.trips_data_all.external_green_tripdata;


-- Create a partitioned table from external table
CREATE OR REPLACE TABLE datatalks-de-351308.trips_data_all.green_tripdata_partitoned
PARTITION BY
  DATE(cleaned_pickup_datetime) 
   AS 
SELECT *, TIMESTAMP_MICROS(CAST(lpep_pickup_datetime / 1000 AS INT64)) AS cleaned_pickup_datetime, TIMESTAMP_MICROS(CAST(lpep_dropoff_datetime / 1000 AS INT64)) AS cleaned_dropoff_datetime 
FROM  datatalks-de-351308.trips_data_all.external_green_tripdata;

-- Impact of partition
-- Scanning 1.6GB of data
SELECT DISTINCT(VendorID)
FROM datatalks-de-351308.trips_data_all.green_tripdata_non_partitoned
WHERE DATE(lpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';

-- Scanning ~106 MB of DATA
SELECT DISTINCT(VendorID)
FROM datatalks-de-351308.trips_data_all.green_tripdata_partitoned
WHERE DATE(lpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';

-- Let's look into the partitons
SELECT table_name, partition_id, total_rows
FROM `trips_data_all.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'green_tripdata_partitoned'
ORDER BY total_rows DESC;

-- Creating a partition and cluster table
CREATE OR REPLACE TABLE datatalks-de-351308.trips_data_all.green_tripdata_partitoned_clustered
PARTITION BY
DATE(cleaned_pickup_datetime) 
CLUSTER BY VendorID AS 
SELECT *, TIMESTAMP_MICROS(CAST(lpep_pickup_datetime / 1000 AS INT64)) AS cleaned_pickup_datetime, TIMESTAMP_MICROS(CAST(lpep_dropoff_datetime / 1000 AS INT64)) AS cleaned_dropoff_datetime 
FROM  datatalks-de-351308.trips_data_all.external_green_tripdata;

-- Query scans 1.1 GB
SELECT count(*) as trips
FROM datatalks-de-351308.trips_data_all.green_tripdata_partitoned
WHERE DATE(lpep_pickup_datetime) BETWEEN '2019-06-01' AND '2020-12-31'
  AND VendorID=1;

-- Query scans 864.5 MB
SELECT count(*) as trips
FROM datatalks-de-351308.trips_data_all.green_tripdata_partitoned_clustered
WHERE DATE(lpep_pickup_datetime) BETWEEN '2019-06-01' AND '2020-12-31'
  AND VendorID=1;


-- Impact of partition
-- Distinct PULocationIDs on external table
SELECT DISTINCT(PULocationID)
FROM datatalks-de-351308.trips_data_all.external_green_tripdata;

-- Distinct PULocationIDs on bigquery table
SELECT DISTINCT(PULocationID)
FROM datatalks-de-351308.trips_data_all.green_trips_2022;


select  DISTINCT(PULocationID)
from datatalks-de-351308.trips_data_all.green_trips_2022
WHERE DATE(TIMESTAMP_MICROS(CAST(lpep_pickup_datetime / 1000 AS INT64))) BETWEEN '2022-06-01' AND '2022-06-29';

select  DISTINCT(PULocationID)
from datatalks-de-351308.trips_data_all.green_tripdata_non_partitoned
WHERE DATE(TIMESTAMP_MICROS(CAST(lpep_pickup_datetime / 1000 AS INT64))) BETWEEN '2022-06-01' AND '2022-06-29';

select  DISTINCT(PULocationID)
from datatalks-de-351308.trips_data_all.green_tripdata_partitoned
WHERE DATE(TIMESTAMP_MICROS(CAST(lpep_pickup_datetime / 1000 AS INT64))) BETWEEN '2022-06-01' AND '2022-06-29';

-- Question 1
-- Answer: Yorkville East, Steinway

CREATE MATERIALIZED VIEW zone_stats AS
  SELECT 
    pu_zone.Zone as pickup_zone, 
    do_zone.Zone as dropoff_zone, 
    AVG(trip_data.tpep_dropoff_datetime - trip_data.tpep_pickup_datetime) as avg_time,
    MIN(trip_data.tpep_dropoff_datetime - trip_data.tpep_pickup_datetime) as min_time,
    MAX(trip_data.tpep_dropoff_datetime - trip_data.tpep_pickup_datetime) as max_time,
	COUNT(trip_data.tpep_dropoff_datetime - trip_data.tpep_pickup_datetime) as no_of_trips
  FROM trip_data
  JOIN taxi_zone as pickup_zone
    ON trip_data.PULocationID = pickup_zone.location_id                 
  JOIN taxi_zone as dropoff_zone
    ON trip_data.DOLocationID = dropoff_zone.location_id
  WHERE trip_data.PULocationID != trip_data.DOLocationID
  GROUP BY 
    pickup_zone.Zone, dropoff_zone.Zone
  ORDER BY avg_time DESC;

SELECT * FROM zone_stats;



-- Question 2 
-- Ans: 1
CREATE MATERIALIZED VIEW zone_stats_new AS
  SELECT 
    pu_zone.Zone as pickup_zone, 
    do_zone.Zone as dropoff_zone, 
    AVG(trip_data.tpep_dropoff_datetime - trip_data.tpep_pickup_datetime) as avg_time,
    MIN(trip_data.tpep_dropoff_datetime - trip_data.tpep_pickup_datetime) as min_time,
    MAX(trip_data.tpep_dropoff_datetime - trip_data.tpep_pickup_datetime) as max_time,
	COUNT(trip_data.tpep_dropoff_datetime - trip_data.tpep_pickup_datetime) as no_of_trips
  FROM trip_data
  JOIN taxi_zone pu_zone
    ON trip_data.PULocationID = pu_zone.location_id                 
  JOIN taxi_zone do_zone
    ON trip_data.DOLocationID = do_zone.location_id
  WHERE trip_data.PULocationID != trip_data.DOLocationID
  GROUP BY 
    pu_zone.Zone, do_zone.Zone
  ORDER BY avg_time DESC;

SELECT * FROM zone_stats_new;


-- Question 3
-- Ans: LaGuardia Airport, Lincoln Square East, JFK Airport

CREATE MATERIALIZED VIEW latest_pickups_17 AS
WITH latest_pickup_time AS (SELECT MAX(tpep_pickup_datetime) AS latest_pickup_time FROM trip_data)

SELECT pu_zone.Zone as pickup_zone, COUNT(1) as count
FROM trip_data
JOIN taxi_zone pu_zone ON trip_data.PULocationID = pu_zone.location_id
WHERE tpep_pickup_datetime > (SELECT latest_pickup_time - INTERVAL '17' HOUR FROM latest_pickup_time)
GROUP BY pu_zone.Zone
ORDER BY count DESC, 1 DESC;

SELECT * FROM latest_pickups_17;
----- Question 3. Count Records

SELECT count(*) --index, "VendorID", lpep_pickup_datetime, lpep_dropoff_datetime, store_and_fwd_flag, "RatecodeID", "PULocationID", "DOLocationID", passenger_count, trip_distance, fare_amount, extra, mta_tax, tip_amount, tolls_amount, ehail_fee, improvement_surcharge, total_amount, payment_type, trip_type, congestion_surcharge
FROM public.green_trip_data
WHERE date(lpep_pickup_datetime) = '2019-09-18' and date(lpep_dropoff_datetime) = '2019-09-18'


-- Question 4: Largest Trip for each day

SELECT DATE(lpep_pickup_datetime), MAX(trip_distance)
FROM public.green_trip_data
group by DATE(lpep_pickup_datetime)
ORDER BY MAX(trip_distance) DESC


---- Question 5: Three biggest pick up Boroughs

SELECT "Borough", sum(total_amount)
FROM public.green_trip_data AS g_trip
LEFT JOIN public.zones ON public.zones."LocationID" = g_trip."PULocationID"
where date(lpep_pickup_datetime) = '2019-09-18'
group by "Borough"
having sum(total_amount) > 50000
order by sum(total_amount) desc


--- Question 6: Largest Tip

SELECT dropoff_zone."Zone", max(tip_amount)
FROM public.green_trip_data AS g_trip
LEFT JOIN public.zones as pickup_zone ON pickup_zone."LocationID" = g_trip."PULocationID"
LEFT JOIN public.zones as dropoff_zone ON dropoff_zone."LocationID" = g_trip."DOLocationID"
where pickup_zone."Zone" = 'Astoria' and extract(MONTH FROM lpep_pickup_datetime) = 9 and extract(YEAR FROM lpep_pickup_datetime) = 2019 
group by dropoff_zone."Zone"
order by max(tip_amount) desc

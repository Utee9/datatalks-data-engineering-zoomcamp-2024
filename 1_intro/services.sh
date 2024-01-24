services:
	postgres:
		image: postgres:13
		environment:
			POSTGRES_USER: airflow
			POSTGRES_PASSWORD: airflow
			POSTGRES_DB: airflow
		volumes:
			- postgres-db-volume:/var/lib/postgresql/data
		healthcheck:
			test: ["CMD", "pg_isready", "-U", "airflow"]
			interval: 5s
			retries: 5
		restart: always

docker run -it \\
	-e POSTGRES_USER="root" \\
	-e POSTGRES_PASSWORD="root" \\
	-e POSTGRES_DB="ny_taxi" \\
	-v c:/Users/Utomi Ogbe/Desktop/DataTalks DE/1_intro/ny_taxi_postgres_data:var/lib/postgresql/data \\
	-p 5432:5432
	postgres:13


docker run -it  -e POSTGRES_USER = "root" -e POSTGRES_PASSWORD = "root" \
			-e POSTGRES_DB = "ny_taxi" \
			-v c:/Users/Utomi Ogbe/Desktop/DataTalks DE/1_intro
/ny_taxi_postgres_data:/var/lib/postgresql/data \
			-p 5432:5432 \
postgres:13 

docker run -it \
			-e POSTGRES_USER = "root" \
			-e POSTGRES_PASSWORD = "root" \
			-e POSTGRES_DB = "ny_taxi" \
			-v (pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
			-p 5432:5432 \
postgres:13


docker run -it \
	-e POSTGRES_USER="root" \
	-e POSTGRES_PASSWORD="root" \
	-e POSTGRES_DB="ny_taxi" \
	-v c:/Users/"Utomi Ogbe"/Desktop/datatalks/1_intro/ny_taxi_postgres_data:/var/lib/postgresql/data \
	-p 5432:5432 postgres:13


docker run -it \
	-e POSTGRES_USER="root" \
	-e POSTGRES_PASSWORD="root" \
	-e POSTGRES_DB="ny_taxi" \
	-v $(pwd)/ny_taxi_postgres_data:var/lib/postgresql/data \
	-p 5432:5432 postgres:13


docker run -it \
	-e POSTGRES_USER="root" \
	-e POSTGRES_PASSWORD="root" \
	-e POSTGRES_DB="ny_taxi" \
	-v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
	-p 5432:5432 postgres:13

winpty pgcli -h localhost -p 5432 -u root -d ny_taxi

https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page
https://www1.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf


docker pull dpage/pgadmin4

docker run -it \

docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -p 8080:80 \
dpage/pgadmin4

docker network create pg-network

winpty docker run -it \
	-e POSTGRES_USER="root" \
	-e POSTGRES_PASSWORD="root" \
	-e POSTGRES_DB="ny_taxi" \
	-v c:/Users/"Utomi Ogbe"/Desktop/datatalks/1_intro/ny_taxi_postgres_data:/var/lib/postgresql/data \
	-p 5432:5432 \
	--network=pg-network \
	--name pg-database postgres:13

docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -p 8080:80 \
  --network=pg-network \
  --name pgadmin dpage/pgadmin4

URL="https://drive.google.com/open?id=1MlkA_Oh88b2HW44_7f5kp7qPxQDBoUhj"
URL="<https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.csv>"
URL="C:\Users\Utomi Ogbe\Desktop\datatalks\1_intro\yellow_taxi_data.csv"

URL="http://192.168.0.119:8000/yellow_taxi_data.csv"

  python ingest_data.py \
  --user=root \
  --password=root \
  --host=localhost \
  --port=5432 \
  --db=ny_taxi \
  --table_name=yellow_taxi_trips --url="${URL}"

  docker build -t taxi_ingest:v001 .

URL="http://192.168.0.119:8000/yellow_taxi_data.csv"
  docker run -it \
  --network=pg-network \
  taxi_ingest:v001 \
  --user=root \
  --password=root \
  --host=pg-database \
  --port=5432 \
  --db=ny_taxi \
  --table_name=yellow_taxi_trips --url="${URL}"


  user, password, host, port, database name, table name
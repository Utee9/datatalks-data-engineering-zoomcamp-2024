import pyarrow as pa
import pyarrow.parquet as pq
import os

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/src/datatalks-de-351308-fcafb3d971e9.json"

bucket_name = 'dtc_data_lake_datatalks-de-351308'
project_id = 'datatalks-de-351308'

table_name = "nyc_greentrips_2022"

root_path = f'{bucket_name}/{table_name}'

@data_exporter
def export_data(data, *args, **kwargs):
    data['lpep_pickup_datetime'] = data['lpep_pickup_datetime'].dt.date
    # data['lpep_dropoff_datetime'] = data['lpep_dropoff_datetime'].dt.date

    table = pa.Table.from_pandas(data)

    gcs = pa.fs.GcsFileSystem()

    pq.write_to_dataset(
        table,
        root_path=root_path,
        partition_cols = ['lpep_pickup_datetime'],
        filesystem=gcs
    )



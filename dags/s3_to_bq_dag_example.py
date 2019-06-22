from datetime import timedelta, datetime
import re

from airflow import DAG
from airflow.contrib.operators.s3_to_gcs_transfer_operator import S3ToGoogleCloudStorageTransferOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

"""
Airflow version: 1.10.13
"""

# -- CONFIG BY USER
s3_bucket = 'my_s3_bucket'
include_prefix = 'my_data/parquet/day_1/my_event/event_date={{ ds }}/part-' # data partitioned by date
destination_table = 'my-gcp-project.mydataset.my_event${{ ds_nodash }}' # bq partitioned by date
source_format = 'PARQUET'
start_date = datetime.strptime('2019-06-01','%Y-%m-%d')
end_date = datetime.strptime('2019-06-20','%Y-%m-%d') # specify if backfilling until certain date
email_alert = 'myemail@email.com'
schedule_interval = '@daily'

project_id = 'my-gcp-project'
gcs_bucket = 's3-to-bq-' + s3_bucket # temporary bucket to store file
dag_id = re.sub('[^0-9a-zA-Z]+', '_', include_prefix) # use s3 prefix as dag name so it is unique

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': start_date,
    'end_date': end_date,
    'email': email_alert,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(dag_id, schedule_interval=schedule_interval,
        default_args=default_args) as dag:

    s3_to_gcs = S3ToGoogleCloudStorageTransferOperator(
        task_id='s3_to_gcs',
        s3_bucket=s3_bucket,
        project_id=project_id,
        gcs_bucket=gcs_bucket,
        description= '_'.join([gcs_bucket, include_prefix]),
        object_conditions={ 'include_prefixes': [ include_prefix ] },
        replace=True
    )

    gcs_to_bq = GoogleCloudStorageToBigQueryOperator(
        task_id='gcs_to_bq',
        bucket=gcs_bucket,
        source_objects=[ include_prefix + '*' ],
        destination_project_dataset_table=destination_table,
        source_format=source_format,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True
    )

    s3_to_gcs >> gcs_to_bq

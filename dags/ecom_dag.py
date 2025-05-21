# Airflow imports
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.base import PokeReturnValue
from airflow.hooks.base import BaseHook
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata

# Other imports
from datetime import datetime, timedelta

# Tasks imports
from include.ecom.tasks import _get_logical_date, _store_data_in_minio, _get_formatted_csv, BUCKET_NAME

@dag(
    start_date=datetime(2025,4,1),
    schedule=None,
    catchup=False,
    description='This is a test for various feature.',
    dagrun_timeout=timedelta(minutes=5),
)

def ecom_dag():
    # Get logical date to check for .csv files later.
    get_logical_date = PythonOperator(
        task_id='get_logical_date',
        python_callable=_get_logical_date
    )

    # Check if the file at the logical date exsits in the specified location.
    @task.sensor(poke_interval=30, timeout=300, mode='poke')
    def check_file_exists(ti=None) -> PokeReturnValue:
        import os.path
        logical_date = ti.xcom_pull(
            key='logical_date',
            task_ids='get_logical_date'
        )
        file_path = f'/usr/local/airflow/include/sensor/{logical_date}.csv'
        does_file_exist = os.path.exists(file_path)
        if does_file_exist == True:
            ti.xcom_push(key='file_path', value=file_path)
        return PokeReturnValue(is_done=does_file_exist)

    # Store data on MinIO.
    store_data_in_minio = PythonOperator(
        task_id='store_data_in_minio',
        python_callable=_store_data_in_minio
    )

    format_data = SparkSubmitOperator(
        task_id='format_data',
        conn_id='my_spark_conn',
        jars="aws-java-sdk-bundle-1.12.262.jar,hadoop-aws-3.3.4.jar",
        application='include/scripts/ecom_transform.py',
        verbose=False,
        name='FormatEcom',
        executor_memory='3G',
        executor_cores=2,
        driver_memory='2G',
        conf={
            "inferSchema": "true",
            "spark.hadoop.fs.s3a.access.key": BaseHook.get_connection('minio').login,
            "spark.hadoop.fs.s3a.secret.key": BaseHook.get_connection('minio').password,
            "spark.hadoop.fs.s3a.endpoint":"http://host.docker.internal:9000",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "true",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.attempts.maximum": "1",
            "spark.hadoop.fs.s3a.connection.establish.timeout": "500000",
            "spark.hadoop.fs.s3a.connection.timeout": "1000000",
        },
        env_vars=
        {
            'SPARK_APPLICATION_ARGS': '{{ ti.xcom_pull(task_ids="store_data_in_minio") }}',
        }
    )

    get_formatted_csv = PythonOperator(
        task_id='get_formatted_csv',
        python_callable=_get_formatted_csv,
        op_kwargs={
            'path': '{{ ti.xcom_pull(task_ids="store_data_in_minio") }}'
        }
    )
    load_to_dw = aql.load_file(
        task_id='load_to_dw',
        input_file=File(
            path=f"s3://{BUCKET_NAME}/{{{{ ti.xcom_pull(task_ids='get_formatted_csv') }}}}",
            conn_id='minio'
        ),
        output_table=Table(
            name='ecom',
            conn_id='postgres',
            metadata=Metadata(
                schema='public'
            )
        ),
        load_options={
            "aws_access_key_id": BaseHook.get_connection('minio').login,
            "aws_secret_access_key": BaseHook.get_connection('minio').password,
            "endpoint_url": BaseHook.get_connection('minio').host,
        },
        if_exists='replace'
    )
    
    get_logical_date >> check_file_exists() >> store_data_in_minio >> format_data >> get_formatted_csv >> load_to_dw

ecom_dag()

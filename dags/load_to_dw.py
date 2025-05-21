from airflow.decorators import dag, task, task_group
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.base import PokeReturnValue
from airflow.hooks.base import BaseHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
import sqlalchemy

from datetime import datetime, timedelta

from include.ecom.tasks import _get_logical_date, _store_data_in_minio, _get_formatted_csv, BUCKET_NAME

KAFKA_TOPIC = 'test_topic_v2'

@dag(
    start_date=datetime(2019,12,1),
    end_date=datetime(2019,12,31),
    schedule='@daily',
    catchup=False,
    description='Upload files to datawarehouse.',
    dagrun_timeout=timedelta(minutes=5),
)

def load_to_postgres():
    # Get logical date to check for .csv files later.
    get_logical_date = PythonOperator(
        task_id='get_logical_date',
        python_callable=_get_logical_date
    )

    #consume_records = ConsumeFromTopicOperator(
    #    task_id="consume_records",
    #    kafka_config_id="kafka_default",
    #    topics=[KAFKA_TOPIC],
    #    apply_function=_cons_func,
    #    poll_timeout=20,
    #    max_messages=1000,
    #)

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
        verbose=True,
        name='FormatEcom',
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
            "spark.executor.memory": "4g",
            "spark.driver.memory": "4g"
        },
        env_vars=
        {
            'SPARK_APPLICATION_ARGS': '{{ ti.xcom_pull(task_ids="store_data_in_minio") }}',
        }
    )

    # Create table
    get_formatted_csv = PythonOperator(
        task_id='get_formatted_csv',
        python_callable=_get_formatted_csv,
        op_kwargs={
            'path': '{{ ti.xcom_pull(task_ids="store_data_in_minio") }}'
        }
    )

    create_main_table = SQLExecuteQueryOperator(
        task_id='create_main_table',
        conn_id='postgres',
        sql="""
        CREATE TABLE IF NOT EXISTS ecom (
        event_time TEXT,
        date TEXT NOT NULL,
        time TEXT NOT NULL,
        event_type TEXT NOT NULL,
        product_id BIGINT NOT NULL,
        category_id BIGINT NOT NULL,
        category_code TEXT,
        sub_category_1 TEXT,
        sub_category_2 TEXT,
        sub_category_3 TEXT,
        sub_category_4 TEXT,
        brand TEXT,
        price FLOAT,
        price_predict FLOAT,
        user_id BIGINT,
        user_session TEXT,
        PRIMARY KEY (event_time, user_session, product_id)
        );
        """
    )

    import_to_temp_table = aql.load_file(
        task_id='import_to_temp_table',
        input_file=File(
            path=f"s3://{BUCKET_NAME}/{{{{ ti.xcom_pull(task_ids='get_formatted_csv') }}}}",
            conn_id='minio'
        ),
        output_table=Table(
            name='ecomtemp',
            conn_id='postgres',
            metadata=Metadata(
                schema='public'
            ),
            columns=[
                sqlalchemy.Column("event_time", sqlalchemy.String(60), primary_key=True),
                sqlalchemy.Column("date", sqlalchemy.String(60), nullable=False),
                sqlalchemy.Column("time", sqlalchemy.String(60), nullable=False),
                sqlalchemy.Column("event_type", sqlalchemy.String(60), primary_key=True),
                sqlalchemy.Column("product_id", sqlalchemy.BigInteger, primary_key=True),
                sqlalchemy.Column("category_id", sqlalchemy.BigInteger, nullable=False),
                sqlalchemy.Column("category_code", sqlalchemy.String(60)),
                sqlalchemy.Column("sub_category_1", sqlalchemy.String(60)),
                sqlalchemy.Column("sub_category_2", sqlalchemy.String(60)),
                sqlalchemy.Column("sub_category_3", sqlalchemy.String(60)),
                sqlalchemy.Column("sub_category_4", sqlalchemy.String(60)),
                sqlalchemy.Column("brand", sqlalchemy.String(60) ),
                sqlalchemy.Column("price", sqlalchemy.Float),
                sqlalchemy.Column("price_predict", sqlalchemy.Float),
                sqlalchemy.Column("user_id", sqlalchemy.BigInteger, nullable=False),
                sqlalchemy.Column("user_session", sqlalchemy.Text, primary_key=True)
            ]
        ),
        load_options={
            "aws_access_key_id": BaseHook.get_connection('minio').login,
            "aws_secret_access_key": BaseHook.get_connection('minio').password,
            "endpoint_url": BaseHook.get_connection('minio').host,
        },
        if_exists='replace'
    )

    merge_table = aql.merge(
        task_id = 'merge_table',
        target_table = Table(
            name='ecom',
            conn_id='postgres',
            metadata=Metadata(
                schema='public'
            )
        ),
        source_table = Table(
            name='ecomtemp',
            conn_id='postgres',
            metadata=Metadata(
                schema='public'
            )
        ),
        columns = ['event_time','date','time','event_type','product_id','category_id','category_code','sub_category_1','sub_category_2','sub_category_3','sub_category_4',
                'brand','price','price_predict','user_id','user_session'],
        target_conflict_columns = ['event_time', 'user_session', 'product_id'],
        if_conflicts="ignore"
    )

    drop_temp_table = SQLExecuteQueryOperator(
        task_id='drop_temp_table',
        conn_id='postgres',
        sql="""
        DROP TABLE IF EXISTS ecomtemp;
        """,
        trigger_rule='all_done'
    )

        

    
    get_logical_date >> check_file_exists()  >> store_data_in_minio >> format_data >> get_formatted_csv >> create_main_table >> import_to_temp_table >> merge_table >> drop_temp_table
load_to_postgres()

#  get_logical_date >>  >> consume_records
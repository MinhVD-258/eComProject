from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowNotFoundException
from io import BytesIO
from minio import Minio
import pendulum
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import json

BUCKET_NAME = 'ecommerce'

def _get_postgres_hook():
    # Get PostgresHook to interface with Postgres
    postgres_client = PostgresHook(
        postgres_conn_id='postgres', # set up in Airflow UI
        schema='postgres'
        )
    return postgres_client

def _cons_func(message, ti=None):
    # Load message key & value
    message_content=json.loads(message.value())
    prod_event_time=message_content['event_time']
    prod_event_type=message_content['event_type']
    prod_product_id=message_content['product_id']
    prod_category_id=message_content['category_id']
    prod_category_code=message_content['category_code']
    prod_brand=message_content['brand']
    prod_price=message_content['price']
    prod_user_id=message_content['user_id']
    prod_user_session=message_content['user_session']

    postgres_sql_insert = _get_postgres_hook()
    
    # Insert rows into Postgres database
    postgres_sql_insert.insert_rows(
        table='kafka_test',
        rows=[(prod_event_time,prod_event_type,prod_product_id,prod_category_id,prod_category_code,prod_brand,prod_price,prod_user_id,prod_user_session)],
        target_fields=['event_time','event_type','product_id','category_id','category_code','brand','price','user_id','user_session'],
        replace=True,
        replace_index=['event_time','product_id','user_session']
        )
    
    # Insert into File in Minio (seems very impractical)
    client = _get_minio_client
    
    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)

    logical_date = ti.xcom_pull(
        key='logical_date',
        task_ids='get_logical_date'
    )

    while True:
        try:
            client.stat_object(
                bucket_name = BUCKET_NAME,
                object_name = f"{logical_date}/raw.parquet"
            )

            break
        except:
            raw = pd.DataFrame(columns=['event_time','event_type','product_id','category_id','category_code','brand','price','user_id','user_session'])
            raw = raw.to_parquet()
            client.put_object(
                bucket_name=BUCKET_NAME,
                object_name=f'{logical_date}/raw.parquet',
                data=BytesIO(raw),
                length=len(raw)
            )
    
    response = client.get_object(
        bucket_name=BUCKET_NAME,
        object_name=f'{logical_date}/raw.parquet'
    )

    data = BytesIO(response.read())
    df


def _get_logical_date(ti=None,**kwargs):
    logical_date = kwargs['logical_date']
    logical_date = logical_date.to_date_string()
    ti.xcom_push(key='logical_date', value=logical_date)

def _get_minio_client():
    minio = BaseHook.get_connection('minio')
    client=Minio(
        endpoint=minio.extra_dejson['endpoint_url'].split('//')[1],
        access_key=minio.login,
        secret_key=minio.password,
        secure=False
    )
    return client

def _store_data_in_minio(ti=None):
    import os.path
    client=_get_minio_client()

    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)
    #file_path = ti.xcom_pull(
    #    key='file_path',
    #    task_ids='check_file_exists'
    #)
    logical_date = ti.xcom_pull(
        key='logical_date',
        task_ids='get_logical_date'
    )
    #with open(file_path, 'r') as file:
    #    eComData = file.read().encode('utf-8')
    postgres_sql = _get_postgres_hook()
    eComData = postgres_sql.get_pandas_df('SELECT * FROM kafka_test_v2').to_parquet()

    objw = client.put_object(
        bucket_name=BUCKET_NAME,
        object_name=f'{logical_date}/raw.parquet',
        data=BytesIO(eComData),
        length=len(eComData)
    )
    return f'{objw.bucket_name}/{logical_date}'

def _get_formatted_csv(path):
    client = _get_minio_client()
    prefix_name = f"{path.split('/')[1]}/formatted/"
    objects = client.list_objects(BUCKET_NAME, prefix=prefix_name, recursive=True)
    for obj in objects:
        if obj.object_name.endswith('.csv'):
            return obj.object_name
    return AirflowNotFoundException('The csv file does not exist')
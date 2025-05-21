from airflow.decorators import dag,task
from pendulum import datetime
import json
import pandas as pd
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

KAFKA_TOPIC = 'test_topic_v2'

def prod_func():
    # Read from .csv file to pandas DataFrame
    df = pd.read_csv(r'include/data/2019-12-01.csv',delimiter=',')
    df = df.astype(str)

    # Process rows into key-value pairs to send to Kafka. Key is row number, value is column name & value for row.
    col_list = list(df.columns)
    for r in range(df.shape[0]):
        dumps_dict = {}
        for c in range(len(col_list)):
            dumps_dict.update({col_list[c]: df.iloc[r,c]})
        yield (
            json.dumps(int(r)),
            json.dumps(dumps_dict)
        )

def cons_func(message):
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

    # Get PostgresHook to interface with Postgres
    postgres_sql_insert = PostgresHook(
        postgres_conn_id='postgres', # set up in Airflow UI
        schema='postgres'
        )
    
    # Insert rows into Postgres database
    postgres_sql_insert.insert_rows(
        table='kafka_test_v2',
        rows=[(prod_event_time,prod_event_type,prod_product_id,prod_category_id,prod_category_code,prod_brand,prod_price,prod_user_id,prod_user_session)],
        target_fields=['event_time','event_type','product_id','category_id','category_code','brand','price','user_id','user_session'],
        replace=True,
        replace_index=['event_time','product_id','user_session']
        )

@dag(
    start_date=datetime(2019,12,1),
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,
)

def kafka_producer():
    create_test_table = SQLExecuteQueryOperator(
        task_id='create_test_table',
        conn_id='postgres',
        sql="""
            CREATE TABLE IF NOT EXISTS kafka_test_v2 (
            event_time TEXT,
            event_type TEXT, 
            product_id TEXT,
            category_id TEXT,
            category_code TEXT,
            brand TEXT, 
            price FLOAT,
            user_id TEXT,
            user_session TEXT,
            PRIMARY KEY (event_time, product_id, user_session)
            );
            """
    )

    produce_records = ProduceToTopicOperator(
        task_id='produce_records',
        kafka_config_id='kafka_default',
        topic=KAFKA_TOPIC,
        producer_function=prod_func,
        poll_timeout=10
    )

    create_test_table >> produce_records

kafka_producer()
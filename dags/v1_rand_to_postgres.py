from airflow.decorators import dag,task
from pendulum import datetime
import json
import random
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

KAFKA_TOPIC = 'test_topic'
TEST_ROW_COUNT = 10

def prod_func():
    for i in range(5):
        person_name=random.choice(['Muk','Pat','Joe','Bob'])
        person_status=random.choice(['Alive','Away','Inactive','Dead'])
        yield (
            json.dumps(i),
            json.dumps(
                {
                    'name': person_name,
                    'status': person_status
                }
            )
        )

def cons_func(message):
    key=str(json.loads(message.key()))
    message_content=json.loads(message.value())
    prod_name=message_content['name']
    prod_status=message_content['status']
    postgres_sql_insert = PostgresHook(
        postgres_conn_id='postgres',
        schema='postgres'
        )
    print(
        f"Message #{key}: {prod_name}: {prod_status}!"
    )
    postgres_sql_insert.insert_rows(
        table='kafka_test_v1',
        rows=[(key,prod_name,prod_status)],
        target_fields=['id','name','status'],
        replace=True,
        replace_index='id'
        )

@dag(
    start_date=datetime(2023, 4, 1),
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,
)
def kafka_test_rand_to_postgres():

    @task
    def get_row_count(row_count=None):
        return row_count

    create_test_table = SQLExecuteQueryOperator(
        task_id='create_test_table',
        conn_id='postgres',
        sql="""
            CREATE TABLE IF NOT EXISTS kafka_test_v1 (
            id BIGINT,
            name TEXT,
            status TEXT,
            PRIMARY KEY (id)
            );
            """
    )

    produce_records = ProduceToTopicOperator(
        task_id='produce_records',
        kafka_config_id='kafka_default',
        topic=KAFKA_TOPIC,
        producer_function=prod_func,
        #producer_function_args=["{{ ti.xcom_pull(task_ids='get_row_count')}}"],
        poll_timeout=10
    )

    consume_records = ConsumeFromTopicOperator(
        task_id="consume_records",
        kafka_config_id="kafka_default",
        topics=[KAFKA_TOPIC],
        apply_function=cons_func,
        poll_timeout=20,
        max_messages=1000,
    )

    get_row_count(TEST_ROW_COUNT) >> create_test_table >> produce_records >> consume_records
kafka_test_rand_to_postgres()
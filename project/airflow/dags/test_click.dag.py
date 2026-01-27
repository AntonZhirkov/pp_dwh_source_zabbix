from airflow import DAG
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from datetime import datetime, timedelta

with DAG(
    dag_id='example_clickhouse_dag',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@once',
) as dag:

    # 1. Задача на выполнение SQL-запроса
    create_table = ClickHouseOperator(
        task_id='create_table',
        clickhouse_conn_id='dwh-zabbix-clickhouse',  # Используем наше подключение
        sql='''
            CREATE TABLE IF NOT EXISTS test_table (
                event_date Date,
                event_name String
            ) ENGINE = MergeTree()
            ORDER BY event_date
        '''
    )
    
    create_table
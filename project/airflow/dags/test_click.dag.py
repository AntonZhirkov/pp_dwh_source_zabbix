from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from datetime import datetime, timezone, timedelta

with DAG(
    dag_id='example_clickhouse_dag',
    start_date=datetime(2026, 1, 29, 21, tzinfo=timezone(offset=timedelta(hours=3))),
    schedule_interval='@once',
) as dag:

    # 1. Задача на выполнение SQL-запроса
    # create_table = ClickHouseOperator(
    #     task_id='create_table',
    #     clickhouse_conn_id='dwh-zabbix-clickhouse',  # Используем наше подключение
    #     sql='''
    #         CREATE TABLE IF NOT EXISTS test_table (
    #             event_date Date,
    #             event_name String
    #         ) ENGINE = MergeTree()
    #         ORDER BY event_date
    #     '''
    # )
    test = BashOperator(
        task_id="test",
        bash_command="echo {{ dag.timezone }}"
    )
    
    
    #create_table
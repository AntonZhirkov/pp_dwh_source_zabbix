import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook

import pandas as pd

log = logging.getLogger(__name__)


default_args = {
    "owner": "ZhirkovAnton",
    "start_date": datetime(2026, 1, 26),
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}


def get_data(**kwargs):
    hook = BaseHook.get_hook(conn_id="zabbix-postgres")
    engine = hook.get_sqlalchemy_engine()
    execution_date = kwargs["execution_date"]
    try:
        df = pd.read_sql_table(
            table_name="outbox",
            schema="public",
            con=engine
        )
    except Exception as error:
        log.error(f"""Не удалось выгрузить данные 
                из источника за {execution_date}.
        """)
        raise Exception(error)
    df.to_parquet(f"/tmp/outbox_{execution_date}.parquet")
    log.info(f"""
            Данные сохранены в промежуточный файл
            /tmp/outbox_{execution_date}.parquet"""
        )
    
def load_data(**kwargs) -> None:
    execution_date = kwargs["execution_date"]
    df = pd.read_parquet(f"/tmp/outbox_{execution_date}.parquet")
    load_data = df.values.tolist()
    hook = ClickHouseHook(clickhouse_conn_id='dwh-zabbix-clickhouse')
    try:
        hook.execute("INSERT INTO stg.host_metric VALUES", load_data)
    except Exception as error:
        log.error(f"""
            Не удалось загрузить данны в stg слой
            из файла /tmp/outbox_{execution_date}.parquet
        """
        )
        raise Exception(error)
    log.info(f"""
            Данные за {execution_date} успешно загружены в stg слой
        """
        )


with DAG(
    dag_id="ETL_oubox_to_stg",
    description="Выгружаем данные из источника раз в 1 час",
    default_args=default_args,
    schedule="@hourly",
    catchup=False,
    is_paused_upon_creation=True
) as dag:
    
    get_data_from_src_oubox = PythonOperator(
        task_id="get_data_from_src_outbox",
        python_callable=get_data
    )
    
    load_data_to_dwh_stg = PythonOperator(
        task_id="load_data_to_dwh_stg",
        python_callable=load_data
    )
    
    remove_temp_file = BashOperator(
        task_id="remove_temp_fil",
        bash_command="""
            rm /tmp/outbox_{{ execution_date }}.parquet
            echo "Файл /tmp/outbox_{{ execution_date }}.parquet успешно удален"
        """
    )
    
    get_data_from_src_oubox >> load_data_to_dwh_stg >> remove_temp_file

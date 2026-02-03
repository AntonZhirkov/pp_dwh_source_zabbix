from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator


default_args = {
    "owner": "AntonZhirkov",
    "start_date": datetime(2026, 1, 26),
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

with DAG(
    dag_id="create_dwh_layers",
    default_args=default_args,
    description="Создание слоев stg, dds, cdm",
    template_searchpath="/opt/airflow/sql",
    schedule_interval="@once",
    catchup=False,
    is_paused_upon_creation=False
) as dag:
    
    create_stg_layer = ClickHouseOperator(
        task_id="create_stg_layer",
        clickhouse_conn_id="dwh-zabbix-clickhouse",
        sql="./stg/DDL_crate_database_stg.sql"
    )
    
    create_stg_host_metric = ClickHouseOperator(
        task_id="create_stg_host_metric",
        clickhouse_conn_id="dwh-zabbix-clickhouse",
        sql="./stg/DDL_create_stg_host_metric.sql"
    )
    
    create_dds_layer = ClickHouseOperator(
        task_id="create_dds_layer",
        clickhouse_conn_id="dwh-zabbix-clickhouse",
        sql="./dds/DDL_crate_database_dds.sql"
    )
    
    create_cdm_layer = ClickHouseOperator(
        task_id="create_cdm_layer",
        clickhouse_conn_id="dwh-zabbix-clickhouse",
        sql="./cdm/DDL_crate_database_cdm.sql"
    )
    
    stop_dag = BashOperator(
        task_id="stop_dag",
        bash_command="airflow dags pause {{ dag.dag_id }}"
    )
    
    [
        create_stg_layer >> create_stg_host_metric, 
        create_dds_layer, 
        create_cdm_layer
    ] >> stop_dag # type: ignore
    
    

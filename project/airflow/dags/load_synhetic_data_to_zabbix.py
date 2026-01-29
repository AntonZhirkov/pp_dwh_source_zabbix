import logging
import random
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook

log = logging.getLogger(__name__)

default_args = {
    "owner": "AntonZhirkov",
    "start_date": datetime(2026, 1, 26, 0, 0),
    "retries": "1",
    "retry_delay": timedelta(minutes=1)
}

hook = BaseHook.get_hook(conn_id='zabbix-postgres')

def generate_icmp_ping_data(
    itemid: int, time: datetime
) -> tuple[int, int, int, int]:
    clock = int(time.timestamp())
    ns = random.randint(100_000_000, 999_999_999)
    value = 0 if random.random() > 0.95 else 1
    return (itemid, clock, value, ns)

def generate_util_data(
    itemid: int, time: datetime
) -> tuple[int, int, int, int]:
    clock = int(time.timestamp())
    ns = random.randint(100_000_000, 999_999_999)
    value = random.randint(1_000_000, 100_000_000)
    return (itemid, clock, value, ns)

def generate_cpu_util_data(
    itemid: int, time: datetime
) -> tuple[int, int, int, int]:
    clock = int(time.timestamp())
    ns = random.randint(100_000_000, 999_999_999)
    random_cpu = random.normalvariate(60, 20)
    value = random_cpu if random_cpu < 100 and random_cpu > 0 else 99.9999
    return (itemid, clock, value, ns)

def load_icmp_data(**kwargs):
    ti = kwargs["ti"]
    hosts = ti.xcom_pull(task_ids="get_hosts_id", key="hosts")
    items = hook.get_records(
        sql=f"SELECT itemid FROM public.items WHERE name = 'ICMP ping' AND hostid IN { hosts }"
    )
    items = tuple([item[0] for item in items])
    time = kwargs["data_interval_start"]
    
    for item in items:
        ping_data = generate_icmp_ping_data(item, time)
        hook.run(
            sql=f"""
                INSERT INTO public.history_uint(itemid, clock, value, ns)
                    VALUES {ping_data}
                """
        )
        
def load_sent_util(**kwargs):
    ti = kwargs["ti"]
    hosts = ti.xcom_pull(task_ids="get_hosts_id", key="hosts")

    items = hook.get_records(
        sql=f"SELECT itemid FROM public.items WHERE name LIKE '%Bits sent%' AND hostid IN {hosts}"
    )
    items = tuple([item[0] for item in items])
    time = kwargs["data_interval_start"]
    
    for item in items:
        send_util_data = generate_util_data(item, time)
        hook.run(
            sql=f"""
                INSERT INTO public.history_uint(itemid, clock, value, ns)
                    VALUES {send_util_data}
                """
        )

def load_recieve_util(**kwargs):
    ti = kwargs["ti"]
    hosts = ti.xcom_pull(task_ids="get_hosts_id", key="hosts")
    items = hook.get_records(
        sql=f"SELECT itemid FROM public.items WHERE name LIKE '%Bits received%' AND hostid IN {hosts}"
    )
    items = tuple([item[0] for item in items])
    time = kwargs["data_interval_start"]
    
    for item in items:
        recieve_util_data = generate_util_data(item, time)
        hook.run(
            sql=f"""
                INSERT INTO public.history_uint(itemid, clock, value, ns)
                    VALUES {recieve_util_data}
                """
        )
        
def load_cpu_util(**kwargs):
    ti = kwargs["ti"]
    hosts = ti.xcom_pull(task_ids="get_hosts_id", key="hosts")
    items = hook.get_records(
        sql=f"SELECT itemid FROM public.items WHERE name = 'CPU utilization' AND hostid IN {hosts}"
    )
    items = tuple([item[0] for item in items])
    time = kwargs["data_interval_start"]
    
    for item in items:
        cpu_util_data = generate_cpu_util_data(item, time)
        hook.run(
            sql=f"""
                INSERT INTO public.history(itemid, clock, value, ns)
                    VALUES {cpu_util_data}
                """
        )
    
def get_hosts(**kwargs):
    hosts = hook.get_records(
        sql="SELECT hostid FROM public.hosts WHERE host LIKE 'server%';"
    )
    hosts = tuple([host[0] for host in hosts])
    ti = kwargs["ti"]
    ti.xcom_push(key="hosts", value=hosts)

with DAG(
    dag_id="load_synthetic_data_to_zabbix",
    default_args=default_args,
    description="""
        Загрузка метрик ICMP ping, CPU utilization, Bit sent/recieved 
        в БД zabbix.
    """,
    schedule="*/2 * * * *",
    catchup=True
) as dag:
    
    get_hosts_id = PythonOperator(
        task_id="get_hosts_id",
        python_callable=get_hosts
    )

    load_icmp = PythonOperator(
        task_id="load_icmp_data",
        python_callable=load_icmp_data
    )
    
    load_recieve_util_data = PythonOperator(
        task_id="load_recieve_util_data",
        python_callable=load_recieve_util
    )
    
    load_sent_util_data = PythonOperator(
        task_id="load_sent_util_data",
        python_callable=load_sent_util
    )
    
    load_cpu_data = PythonOperator(
        task_id="load_cpu_data",
        python_callable=load_cpu_util
    )
    
    get_hosts_id >> [
        load_icmp, 
        load_recieve_util_data, 
        load_sent_util_data, 
        load_cpu_data
    ] # type: ignore
!/bin/bash
docker-compose up -d 

sleep 60

python ./create_hosts_in_zabbix/create_snmp_hosts.py

docker exec airflow-scheduler airflow connections add 'zabbix-postgres' \
    --conn-type 'postgres' \
    --conn-login 'zabbix' \
    --conn-password 'zabbix' \
    --conn-host 'zabbix-postgres-server' \
    --conn-port '5432' \
    --conn-schema 'zabbix'


docker exec airflow-scheduler airflow connections add 'dwh-zabbix-clickhouse' \
    --conn-type 'generic' \
    --conn-login 'clickhouse' \
    --conn-password 'clickhouse' \
    --conn-host 'clickhouse' \
    --conn-port '9000' \
    --conn-schema 'default'

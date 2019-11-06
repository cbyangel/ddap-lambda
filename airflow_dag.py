from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

local_tz = pendulum.timezone("UTC")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": pendulum.datetime(2017, 10, 3),
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

dag = DAG("ddap",
          catchup=True,
          schedule_interval='01 3 * * *',  # 12 pm KST
          default_args=default_args)

svd_append_dhub = BashOperator(
    bash_command="""
cd /data2/ddap
/data2/ddap/conda/envs/ddap-batch/bin/python -c '
from main import *

broad_dt = int({{ yesterday_ds_nodash }})
write_db.delete_by_day_auto_table(broad_dt)
write_db.delete_by_day_snapshot_table(broad_dt)
main(broad_dt)
'
""",
    task_id="svd_append_dhub",
    dag=dag
)

svd_append_dhub

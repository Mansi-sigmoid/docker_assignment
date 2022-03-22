from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from Create_table import check_connection
from Fetch_table import fetch_info

default_args = {
    "owner": "Mansi Gupta",
    "depends_on_past": False,
    "start_date": datetime(2022, 3, 22),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),

}

dag = DAG("Docker_assignment", default_args=default_args, schedule_interval=timedelta(1))
t1 = PythonOperator(task_id='Adding_info_to_DB_table', python_callable=check_connection, dag=dag)
t2 = PythonOperator(task_id='fetching_info_from_PostgresDB', python_callable=fetch_info, dag=dag)

t1 >> t2
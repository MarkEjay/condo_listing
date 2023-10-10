from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from toronto_rent import extract_rental
from toronto_rent import load_to_redshift
from toronto_rent import testing
from airflow.timetables.trigger import CronTriggerTimetable




default_args = {
    'owner':'airflow',
    "depends_on_past": False,
    "start_date":datetime(2023, 10, 8),
    "schedule_interval":'0 0 1 * *',
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag=DAG(
    'rental_dag',
    default_args=default_args,
    description="Rental etl",

)

run_test=PythonOperator(
    task_id='testing_v2',
    python_callable=testing,
    dag=dag
    )
run_extract=PythonOperator(
    task_id='rental_extract_v2',
    python_callable=extract_rental,
    dag=dag
    )

run_load2redshift=PythonOperator(
    task_id='rental_load2redshift_v2',
    python_callable=load_to_redshift,
    dag=dag
)


run_test

run_extract
run_load2redshift
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Import cleaning functions from the scripts folder
from scripts import clean_missing, clean_invalid, clean_datetime, cast_types

# DAG
with DAG('lakehouse_pipeline',
         start_date=datetime(2023, 1, 1),
         schedule_interval='@daily',
         catchup=False,
         tags=['lakehouse', 'data_quality']) as dag:

    clean_missing_task = PythonOperator(
        task_id='cleanMissing',
        python_callable=cleanMissing.run
    )

    clean_invalid_task = PythonOperator(
        task_id='cleanInvalid',
        python_callable=cleanInvalid.run
    )

    clean_datetime_task = PythonOperator(
        task_id='cleanDateTime',
        python_callable=cleanDateTime.run
    )

    cast_types_task = PythonOperator(
        task_id='castTypes',
        python_callable=castTypes.run
    )

    # Chain tasks in order
    clean_missing_task >> clean_invalid_task >> clean_datetime_task >> cast_types_task

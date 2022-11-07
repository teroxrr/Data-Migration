# General
from datetime import datetime, timedelta

# Airflow
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator


dag = DAG (
    dag_id="move_historic_data",
    max_active_runs=1,
    tags=["etl", "prod", "migration"],
    start_date=datetime(2022, 10, 1), 
    schedule_interval="0 12 * * *",
    default_args={
        "email": ["adtdacdb@gmail.com"],
        'email_on_failure': True,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
    }
)

entry = DummyOperator(
    task_id="entry-point",
    dag=dag,
)

end = DummyOperator(
    task_id="end-point",
    dag=dag,
)

insert = SparkSubmitOperator(
    task_id="insert_into_table",
    application= str("tasks/insert_data.py"),
    spark_binary="/bin/spark-submit",
    dag=dag,
)

entry >> insert >> end
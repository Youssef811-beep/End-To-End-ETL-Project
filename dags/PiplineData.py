from datetime import datetime
from airflow import DAG
from airflow.sdk import dag, task 
from airflow.timetables.trigger import CronTriggerTimetable
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
import pendulum

local_tz = pendulum.timezone("Asia/Kolkata")

with DAG(
    dag_id="databricks_pipeline_run",
    tags=["databricks"],
    start_date=pendulum.datetime(2026, 3, 31, tz=local_tz),
    schedule=CronTriggerTimetable("0 22 * * MON-FRI", timezone=local_tz),
    end_date=pendulum.datetime(2026, 4, 2, tz=local_tz),
    is_paused_upon_creation=False,
    catchup=True,
) as dag:

    run_databricks_pipeline = DatabricksRunNowOperator(
        task_id="run_databricks_pipeline",
        databricks_conn_id="databricks_default",
        job_id=65573403043706,
    )

    run_databricks_gold = DatabricksRunNowOperator(
        task_id="run_databricks_gold",
        databricks_conn_id="databricks_default",
        job_id=417594846350641,
    )

    run_databricks_pipeline >> run_databricks_gold

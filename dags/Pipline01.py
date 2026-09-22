from airflow import DAG
from datetime import datetime
from airflow.timetables.trigger import CronTriggerTimetable
#from airflow.providers.databricks.operator.databricks import DatabricksRunNowOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

import pendulum
local_tz=pendulum.timezone("Asia/Kolkata")

with DAG(
    dag_id="pipelinejob",
    tags=["data_bricks"],
    start_date=pendulum.datetime(2026,4,1,tz=local_tz),
    schedule=CronTriggerTimetable("0 22 * * MON-FRI",timezone=local_tz),
    end_date=pendulum.datetime(2026,4,3,tz=local_tz),
    catchup=True
    
) as dag:
    databricksPipe=DatabricksRunNowOperator(
        task_id="databricksPipe",
        databricks_conn_id="databricks_default",
        job_id=65573403043706,
    )
    databricksgoldpipe=DatabricksRunNowOperator(
        task_id="databricksgoldpipe",
        databricks_conn_id="databricks_default",
        job_id=417594846350641
    )
    databricksPipe >> databricksgoldpipe

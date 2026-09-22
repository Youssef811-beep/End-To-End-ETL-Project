from airflow.sdk import dag, task 
from pendulum import datetime

@dag(
    dag_id='first_schedule',
    start_date=datetime(year=2026, month=2, day=28, tz="Asia/Kolkata"),
        schedule="@daily",
        is_paused_upon_creation=False,
        catchup=True)
    

def first_schedule():
    
    @task.python
    
    def first_task():
        print("this is first")
    
    @task.python
    
    def second_task():
        print("this is second")
    
    @task.python
    
    def third_task():
        print("this is third")
    
    first=first_task()
    sec=second_task()
    third=third_task()
    
    first >> sec >> third
    
dag=first_schedule()
    
    

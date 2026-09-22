from airflow.sdk import dag, task 
from pendulum import datetime
from airflow.timetables.trigger import CronTriggerTimetable

@dag(
    dag_id='cron_schedule',
    start_date=datetime(year=2026, month=2, day=27, tz="Asia/Kolkata"),
    schedule=CronTriggerTimetable("0 22 * * MON-FRI",timezone="Asia/Kolkata"),
    end_date=datetime(year=2026,month=3,day=2,tz="Asia/Kolkata"),   
    is_paused_upon_creation=False,
        catchup=True)
    

def cron_schedule():
    
    @task.python
    
    def first_task():
        print("this is first")
        extract_data={'akash':[1,3,4,3],'db':[5,6,4,2]}
    
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
    
dag=cron_schedule()
    
    

from airflow.sdk import dag, task 
from pendulum import datetime, duration
#from airflow.timetables.trigger import DeltaTriggerTimetable
from airflow.timetables.trigger import DeltaTriggerTimetable


@dag(
    dag_id="delta_schedule",
   start_date=datetime(year=2026, month=2, day=27, tz="Asia/Kolkata"),
   end_date=datetime(year=2026, month=2, day=28, tz="Asia/Kolkata"),
   schedule=DeltaTriggerTimetable(duration(days=3)),
   is_paused_upon_creation=False,
   catchup=True
)
def delta_schedule():
    
    @task.python
    def first_task():
        print('my first task')
    
    @task.python
    def second_task():
        print('my second task')
        
    @task.python
    
    def third():
        print('third')
    
    first=first_task()
    sec=second_task()
    third=third()
    
    first >> sec >> third
    
dag=delta_schedule()


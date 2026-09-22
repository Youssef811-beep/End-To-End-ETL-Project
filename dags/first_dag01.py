from airflow.sdk import dag, task

@dag(
    dag_id="first_dag02",
    
)
def first_dag02():

    @task.python
    def first_task():
        print("first task completed ")

    @task.python
    def second_task():
        print("second task completed ")

    @task.python
    def third_task():
        print("third welocme to apache airflow ")

   
    @task.python
    def versioning_task():
        print("this is version")

    first = first_task()
    second = second_task()
    third = third_task()
    version = versioning_task()

    first >> second >> third >> version


dag =first_dag02()

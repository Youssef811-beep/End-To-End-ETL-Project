from airflow.sdk import dag, task
from airflow.operators.bash import BashOperator   # ✅ required import

@dag(
    dag_id="bash_operator",
    schedule=None,
    catchup=False,
)
def bash_operator():

    @task.python
    def first_task():
        print("first task completed")

    @task.python
    def second_task():
        print("second task completed")

    @task.python
    def third_task():
        print("third welcome to apache airflow")

    @task.python
    def versioning():
        print("this is new version")

    @task.bash
    def bash_school() -> str:
        return "echo https://airflow.apache.org/"

    # Classic operator
    bash_task = BashOperator(
        task_id="bash_task",
        bash_command="echo https://airflow.apache.org/",
    )

    # Create task instances
    first = first_task()
    second = second_task()
    version01 = versioning()
    third = third_task()
    bash = bash_school()

    # Set dependencies
    first >> second >> version01 >> third >> bash >> bash_task

dag = bash_operator()

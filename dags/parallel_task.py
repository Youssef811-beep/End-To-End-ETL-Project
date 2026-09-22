
    
from airflow.sdk import dag, task

@dag(
    dag_id='parallel',
    schedule=None,
    catchup=False
)
def parallel():

    @task.python
    def extract_task(**kwargs):
        print('Extracting data....')
        ti = kwargs['ti']
        extract_data_dict = {
            "api_extract_data": [1, 2, 3],
            "db_extract_data": [4, 5, 6],
            "trans_extract_data": [6, 7, 8]
        }
        ti.xcom_push(key="return_value", value=extract_data_dict)

    @task.python
    def transform_task_api(**kwargs):
        print('transforming api data')
        ti = kwargs['ti']
        extract_data_dict = ti.xcom_pull(task_ids="extract_task")
        api_extract_data = extract_data_dict['api_extract_data']
        print(f"tranforming:{api_extract_data}")
        transformed_api_data = [i * 10 for i in api_extract_data]
        ti.xcom_push(key="return_value", value=transformed_api_data)

    @task.python
    def transform_task_db(**kwargs):
        print('transforming db data')
        ti = kwargs['ti']
        extract_data_dict = ti.xcom_pull(task_ids="extract_task")
        db_extract_data = extract_data_dict['db_extract_data']
        print(f"tranforming:{db_extract_data}")
        transformed_db_data = [i * 100 for i in db_extract_data]
        ti.xcom_push(key="return_value", value=transformed_db_data)

    @task.python
    def transform_task_trans(**kwargs):
        print('transforming trans data')
        ti = kwargs['ti']
        extract_data_dict = ti.xcom_pull(task_ids="extract_task")
        trans_extract_data = extract_data_dict['trans_extract_data']
        print(f"tranforming:{trans_extract_data}")
        transformed_trans_data = [i * 1000 for i in trans_extract_data]
        ti.xcom_push(key="return_value", value=transformed_trans_data)

    @task.python
    def load_data(**kwargs):
        ti = kwargs['ti']
        api_data = ti.xcom_pull(task_ids="transform_task_api")
        db_data = ti.xcom_pull(task_ids="transform_task_db")
        trans_data = ti.xcom_pull(task_ids="transform_task_trans")
        print(f"load data : {api_data}, {db_data}, {trans_data}")

    # Task instances
    extract = extract_task()
    transform_api = transform_task_api()
    transform_db = transform_task_db()
    transform_s3 = transform_task_trans()
    load = load_data()

    extract >> [transform_api, transform_db, transform_s3] >> load


dag = parallel()

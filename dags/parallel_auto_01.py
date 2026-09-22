from airflow import dag,task

@dag(
    dag_id='auto'
)

def auto():
    @task.python
    def extract_task(**kwargs):
        ti=**kwargs['ti']
        extract_data_dict={'api_extract_data':[1,2,3,4],
                           'db_extract_data':[5,6,7,8],
                          's3_extract_data':[9,10,11,12]}
        ti.xcom_push(key='return_value',value=extract_data_dict)
    
    @task.python
    def transform_task_api(**kwargs):
        ti=kwargs['ti']
        extract_data_dict=ti.xcom_pull(task_ids='extract_task')
        api_extract_data=extract_data_dict['api_extract_data']
        print(f"api_data :'{extract_data_dict}'")
        trasform_api_data=[i*10 for i in api_extract_data]
        ti.xcom_push(key='return_value',value=trasform_api_data) 
    
    @task.python
    def transform_task_db(**kwargs):
        ti=kwargs['ti']
        extract_data_dict=ti.xcom_pull(task_ids='extract_task')
        db_extract_data=extract_data_dict['db_extract_data']
        print(f"db_data :'{extract_data_dict}'")
        trasform_db_data=[i*100 for i in db_extract_data]
        ti.xcom_push(key='return_value',value=trasform_db_data)
        
    
    @task.python
    def transform_task_s3(**kwargs):
        ti=kwargs['ti']
        extract_data_dict=ti.xcom_pull(task_ids='extract_task')
        s3_extract_data=extract_data_dict[api_extract_data]
        print(f"api_data :'{s3_extract_data}'")
        trasform_s3_data=[i*10 for i in s3_extract_data]
        ti.xcom_push(key='return_value',value=trasform_s3_data) 
    @task.python
    def load_data(**kwargs):
        ti=kwargs['ti']
        api_data=ti.xcom_pull(task_ids='transform_task_api')
        db_data=ti.xcom_pull(task_ids='transform_task_db')
        s3_data=ti.xcom_pull(task_ids='transform_task_s3')
        
        print(f'transformed_main_data :{api_data},{db_data},{s3_data}')
     
    extract=extract_task()
    api=transform_task_api()
    db=transform_task_db()
    s3=transform_task_s3()
    load=load_data()
    
    extract >> [api,s3,db] >> load
    
dag=auto()
    
    
        
        
    
        
        
        
            

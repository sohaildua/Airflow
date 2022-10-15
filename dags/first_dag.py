try:

    from airflow import DAG
    from datetime import timedelta,datetime
    from airflow.operators.bash_operator import BashOperator
    from airflow.operators.python_operator import PythonOperator
    from datacleaner import data_cleaner
  

    print("All Dag modules are ok ......")
except Exception as e:
    print("Error  {} ".format(e))


with DAG(
        dag_id="first_dag",
        schedule_interval="@daily",
        default_args={
            "owner": "airflow",
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
            "start_date": datetime(2021, 1, 1),
        },
        catchup=False) as f:

    t1 = BashOperator(task_id='check_file_exists', bash_command='shasum ~/store_files_airflow/raw_store_transactions.csv',retries =2 ,retry_delay=timedelta(seconds=15))
    t2 = PythonOperator(task_id='clean_raw_csv', python_callable=data_cleaner)


    t1 >> t2
    

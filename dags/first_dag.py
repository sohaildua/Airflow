try:

    from airflow import DAG
    from datetime import timedelta,datetime
    from airflow.operators.bash_operator import BashOperator
    from airflow.operators.python_operator import PythonOperator
    from airflow.operators.mysql_operator import MySqlOperator
    
  

    print("All Dag modules are ok ......")
except Exception as e:
    print("Error  {} ".format(e))

from datacleaner import data_cleaner
with DAG(
        dag_id="first_dag",
        schedule_interval="@daily",
        template_searchpath=['/usr/local/airflow/sql_files'],
        default_args={
            "owner": "airflow",
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
            "start_date": datetime(2021, 1, 1),
        },
        catchup=False) as f:

    t1 = BashOperator(task_id='check_file_exists', bash_command='shasum ~/store_files_airflow/raw_store_transactions.csv',retries =2 ,retry_delay=timedelta(seconds=15))
    t2 = PythonOperator(task_id='clean_raw_csv', python_callable=data_cleaner)
    t3 = MySqlOperator(task_id='create_mysql_table', mysql_conn_id="mysql_conn", sql="create_table.sql")
    t4 = MySqlOperator(task_id='insert_into_table', mysql_conn_id="mysql_conn", sql="insert_into_table.sql")
     
    t5 = MySqlOperator(task_id='select_from_table', mysql_conn_id="mysql_conn", sql="select_from_table.sql")
    t1 >> t2 >> t3 >> t4 >> t5
    

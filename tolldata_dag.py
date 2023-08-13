# Import the libraries
import os
from datetime import timedelta
from airflow import DAG 
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.email import EmailOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# Using env variable as opposed to hard coding email addresses
from_email=os.environ.get('MY_GMAIL') 
to_email=os.environ.get('ADMIN_MAIL')

# Defining DAG arguements
default_args = {
    'owner': 'Oseloka',
    'start_date': days_ago(0),
    'email': [from_email],
    'email_on_failure':  True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=10),
}

# Defining the DAG
dag = DAG(
    dag_id='ETL_tolldata',
    default_args=default_args,
    description='National Road Traffic De-Congestion Strategy Data Analysis ETL',
    schedule_interval=timedelta(days=1)
)

# Defining the tasks

# Task1: Make script executable
task1_chmod_script = BashOperator(
    task_id="make_script_executable",
    bash_command="chmod 777 '/home/oseloka/pyprojects/airflow/dags/tolldata-etl/etl_tolldata.sh'",
    dag=dag,
)

# Task2: Call shell script
task2_extract_transform = BashOperator(
    task_id="extract_transform",
    bash_command="'/home/oseloka/pyprojects/airflow/dags/tolldata-etl/etl_tolldata.sh'",
    dag=dag,
)

load_data2_sql="""
    load data local infile
    '/home/oseloka/pyprojects/airflow/tolldata-etl/highway_data.csv'
    into table tolldata.highways
    fields terminaated by ','
    enclosed by '"'
    lines terminated by '\n'
    ignore 1 lines
    (Name, Address, Latitude, Longitude, Rating, User_ratings_total, Business_status, Types)
    ;    
    """

# Task3: Append the csv file to the database table.  
task3_Load_data2 = MySqlOperator(
    task_id="load_data_into_db",
    sql=load_data2_sql,
    mysql_conn_id="mysql_f1", # got to airflow UI and setup a connection and use the conn id here.
    dag=dag
)

#  Task4: Send email notification on etl success
task4_email_success = EmailOperator(
    task_id="send_success_email",
    to=to_email,
    subject='Toll Data ETL Successful',
    html_content='<p>The task has been executed successfully.</p>',
    dag=dag
)

# Task pipeline
task1_chmod_script >> task2_extract_transform >> task3_Load_data2 >>task4_email_success
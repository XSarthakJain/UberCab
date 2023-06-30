from datetime import datetime,timedelta
from Airflow import DAG
from pythonoperator import PythonOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

default_args={
    'owner':'Sarthak Jain',
    'start_date':datetime('2023,07,1'),
    'retries':5,
    'retries_delay':timedelta(minute=5)
}

Uber_dag = DAG('UberDAG',default_args=default_args,description='Uber Pipeline',catch_up=False,schedule_interval='0 12 * * *',tags=['uber','pipeline'])
spark_config = {
    'conf': {
        "spark.yarn.maxAppAttempts":"1",
        "spark.yarn.executor.memoryOverhead":"512"
    },
    'conn_id':'spark_local',
    'application':'Uber_Pipeline.ipynb',
    'driver_memory':"1g",
    "executor_cores":1,
    "num_executors":1,
    "executor_memory":'1g'
}

pyspark_job = SparkSubmitOperator(task_id="uber_task",dag=Uber_dag,**spark_config)

pyspark_job
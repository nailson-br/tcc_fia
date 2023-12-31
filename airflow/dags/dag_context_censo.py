import os
from airflow import DAG
from datetime import datetime
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

os.environ["JAVA_HOME"] = Variable.get('JAVA_HOME')

default_args = {
    'owner': 'aulafia',
    'start_date': datetime(2023, 8, 20)
}

dag = DAG(dag_id='02_dag_context_censo',
          default_args=default_args,
          schedule_interval='0 3 * * *',
          tags=['CONTEXT'],
          )

start_dag = DummyOperator(
    task_id='start_dag',
    dag=dag
)

task1 = SparkSubmitOperator(
    task_id='context_censo',
    conn_id='spark_local',
    jars='/usr/local/airflow/jars/aws-java-sdk-dynamodb-1.11.534.jar,\
                                /usr/local/airflow/jars/aws-java-sdk-core-1.11.534.jar,\
                                /usr/local/airflow/jars/aws-java-sdk-s3-1.11.534.jar,\
                                /usr/local/airflow/jars/hadoop-aws-3.2.2.jar'.replace(' ', ''),
    application='/usr/local/airflow/dags/spark_scripts/context_censo_municipios.py',
    dag=dag
)

task2 = SparkSubmitOperator(
    task_id='context_coordenadas_municipios',
    conn_id='spark_local',
    jars='/usr/local/airflow/jars/aws-java-sdk-dynamodb-1.11.534.jar,\
                                /usr/local/airflow/jars/aws-java-sdk-core-1.11.534.jar,\
                                /usr/local/airflow/jars/aws-java-sdk-s3-1.11.534.jar,\
                                /usr/local/airflow/jars/hadoop-aws-3.2.2.jar'.replace(' ', ''),
    application='/usr/local/airflow/dags/spark_scripts/context_coordenadas_municipios.py',
    dag=dag
)

task3 = SparkSubmitOperator(
    task_id='context_coordenadas_estados',
    conn_id='spark_local',
    jars='/usr/local/airflow/jars/aws-java-sdk-dynamodb-1.11.534.jar,\
                                /usr/local/airflow/jars/aws-java-sdk-core-1.11.534.jar,\
                                /usr/local/airflow/jars/aws-java-sdk-s3-1.11.534.jar,\
                                /usr/local/airflow/jars/hadoop-aws-3.2.2.jar'.replace(' ', ''),
    application='/usr/local/airflow/dags/spark_scripts/context_coordenadas_estados.py',
    dag=dag
)

dag_finish = DummyOperator(
    task_id='dag_finish',
    dag=dag
)

start_dag >> [task1, task2, task3] >> dag_finish

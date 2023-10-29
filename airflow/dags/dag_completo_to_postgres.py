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

dag = DAG(dag_id='04_dag_dados_completos_para_postgres',
          default_args=default_args,
          schedule_interval='0 3 * * *',
          tags=['TRUST'],
          )

start_dag = DummyOperator(
    task_id='start_dag',
    dag=dag
)

task1 = SparkSubmitOperator(
    task_id='censo_saude_to_postgre',
    conn_id='spark_local',
    jars='/usr/local/airflow/jars/aws-java-sdk-dynamodb-1.11.534.jar,\
                                /usr/local/airflow/jars/aws-java-sdk-core-1.11.534.jar,\
                                /usr/local/airflow/jars/aws-java-sdk-s3-1.11.534.jar,\
                                /usr/local/airflow/jars/hadoop-aws-3.2.2.jar'.replace(' ', ''),
    application='/usr/local/airflow/dags/spark_scripts/saude_to_postgres.py',
    dag=dag
)

task2 = SparkSubmitOperator(
    task_id='censo_completo_to_postgre',
    conn_id='spark_local',
    jars='/usr/local/airflow/jars/aws-java-sdk-dynamodb-1.11.534.jar,\
                                /usr/local/airflow/jars/aws-java-sdk-core-1.11.534.jar,\
                                /usr/local/airflow/jars/aws-java-sdk-s3-1.11.534.jar,\
                                /usr/local/airflow/jars/hadoop-aws-3.2.2.jar'.replace(' ', ''),
    application='/usr/local/airflow/dags/spark_scripts/censo_completo_to_postgres.py',
    dag=dag
)

task3 = SparkSubmitOperator(
    task_id='coordenadas_municipios_to_postgre',
    conn_id='spark_local',
    jars='/usr/local/airflow/jars/aws-java-sdk-dynamodb-1.11.534.jar,\
                                /usr/local/airflow/jars/aws-java-sdk-core-1.11.534.jar,\
                                /usr/local/airflow/jars/aws-java-sdk-s3-1.11.534.jar,\
                                /usr/local/airflow/jars/hadoop-aws-3.2.2.jar'.replace(' ', ''),
    application='/usr/local/airflow/dags/spark_scripts/coordenadas_municipios_to_postgres.py',
    dag=dag
)

task4 = SparkSubmitOperator(
    task_id='coordenadas_estados_to_postgre',
    conn_id='spark_local',
    jars='/usr/local/airflow/jars/aws-java-sdk-dynamodb-1.11.534.jar,\
                                /usr/local/airflow/jars/aws-java-sdk-core-1.11.534.jar,\
                                /usr/local/airflow/jars/aws-java-sdk-s3-1.11.534.jar,\
                                /usr/local/airflow/jars/hadoop-aws-3.2.2.jar'.replace(' ', ''),
    application='/usr/local/airflow/dags/spark_scripts/coordenadas_estados_to_postgres.py',
    dag=dag
)

task5 = SparkSubmitOperator(
    task_id='censo_indicadores_to_postgre',
    conn_id='spark_local',
    jars='/usr/local/airflow/jars/aws-java-sdk-dynamodb-1.11.534.jar,\
                                /usr/local/airflow/jars/aws-java-sdk-core-1.11.534.jar,\
                                /usr/local/airflow/jars/aws-java-sdk-s3-1.11.534.jar,\
                                /usr/local/airflow/jars/hadoop-aws-3.2.2.jar'.replace(' ', ''),
    application='/usr/local/airflow/dags/spark_scripts/censo_indicadores_to_postgres.py',
    dag=dag
)

dag_tasks_join = DummyOperator(
    task_id='dag_tasks_join',
    dag=dag
)

dag_finish = DummyOperator(
    task_id='dag_finish',
    dag=dag
)

start_dag >> [task3, task4] >> dag_tasks_join >> [task1, task2, task5] >> dag_finish

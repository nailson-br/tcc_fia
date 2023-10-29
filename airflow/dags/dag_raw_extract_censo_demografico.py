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

dag = DAG(dag_id='01_dag_raw_obter_base_censo',
          default_args=default_args,
          schedule_interval='0 3 * * *',
          tags=['RAW'],
          )

start_dag = DummyOperator(
    task_id='start_dag',
    dag=dag
)

task1 = SparkSubmitOperator(
    task_id='inserir_base_censo_zipada_na_raw',
    conn_id='spark_local',
    jars='/usr/local/airflow/jars/aws-java-sdk-dynamodb-1.11.534.jar,\
                                /usr/local/airflow/jars/aws-java-sdk-core-1.11.534.jar,\
                                /usr/local/airflow/jars/aws-java-sdk-s3-1.11.534.jar,\
                                /usr/local/airflow/jars/hadoop-aws-3.2.2.jar'.replace(' ', ''),
    application='/usr/local/airflow/dags/spark_scripts/extract_censo_demografico.py',
    dag=dag
)

task2 = SparkSubmitOperator(
    task_id='extrair_censo_xlsx_na_raw',
    conn_id='spark_local',
    jars='/usr/local/airflow/jars/aws-java-sdk-dynamodb-1.11.534.jar,\
                                /usr/local/airflow/jars/aws-java-sdk-core-1.11.534.jar,\
                                /usr/local/airflow/jars/aws-java-sdk-s3-1.11.534.jar,\
                                /usr/local/airflow/jars/hadoop-aws-3.2.2.jar'.replace(' ', ''),
    application='/usr/local/airflow/dags/spark_scripts/censo_unzip.py',
    dag=dag
)

task3 = SparkSubmitOperator(
    task_id='extrair_censo_csv_na_raw',
    conn_id='spark_local',
    jars='/usr/local/airflow/jars/aws-java-sdk-dynamodb-1.11.534.jar,\
                                /usr/local/airflow/jars/aws-java-sdk-core-1.11.534.jar,\
                                /usr/local/airflow/jars/aws-java-sdk-s3-1.11.534.jar,\
                                /usr/local/airflow/jars/spark-excel_2.12-0.13.7.jar,\
                                /usr/local/airflow/jars/hadoop-aws-3.2.2.jar'.replace(' ', ''),
    application='/usr/local/airflow/dags/spark_scripts/extrair_censo_csv.py',
    dag=dag
)

task4 = SparkSubmitOperator(
    task_id='inserir_base_pnad_zipada_na_raw',
    conn_id='spark_local',
    jars='/usr/local/airflow/jars/aws-java-sdk-dynamodb-1.11.534.jar,\
                                /usr/local/airflow/jars/aws-java-sdk-core-1.11.534.jar,\
                                /usr/local/airflow/jars/aws-java-sdk-s3-1.11.534.jar,\
                                /usr/local/airflow/jars/hadoop-aws-3.2.2.jar'.replace(' ', ''),
    application='/usr/local/airflow/dags/spark_scripts/extract_pnad.py',
    dag=dag
)
task5 = SparkSubmitOperator(
    task_id='extrair_pnad_xlsx_na_raw',
    conn_id='spark_local',
    jars='/usr/local/airflow/jars/aws-java-sdk-dynamodb-1.11.534.jar,\
                                /usr/local/airflow/jars/aws-java-sdk-core-1.11.534.jar,\
                                /usr/local/airflow/jars/aws-java-sdk-s3-1.11.534.jar,\
                                /usr/local/airflow/jars/hadoop-aws-3.2.2.jar'.replace(' ', ''),
    application='/usr/local/airflow/dags/spark_scripts/pnad_unzip.py',
    dag=dag
)
task6 = SparkSubmitOperator(
    task_id='extrair_pnad_csv_na_raw',
    conn_id='spark_local',
    jars='/usr/local/airflow/jars/aws-java-sdk-dynamodb-1.11.534.jar,\
                                /usr/local/airflow/jars/aws-java-sdk-core-1.11.534.jar,\
                                /usr/local/airflow/jars/aws-java-sdk-s3-1.11.534.jar,\
                                /usr/local/airflow/jars/spark-excel_2.12-0.13.7.jar,\
                                /usr/local/airflow/jars/hadoop-aws-3.2.2.jar'.replace(' ', ''),
    application='/usr/local/airflow/dags/spark_scripts/extrair_pnad_csv.py',
    dag=dag
)

task7 = SparkSubmitOperator(
    task_id='extrair_municipios_ibge',
    conn_id='spark_local',
    jars='/usr/local/airflow/jars/aws-java-sdk-dynamodb-1.11.534.jar,\
                                /usr/local/airflow/jars/aws-java-sdk-core-1.11.534.jar,\
                                /usr/local/airflow/jars/aws-java-sdk-s3-1.11.534.jar,\
                                /usr/local/airflow/jars/spark-excel_2.12-0.13.7.jar,\
                                /usr/local/airflow/jars/hadoop-aws-3.2.2.jar'.replace(' ', ''),
    application='/usr/local/airflow/dags/spark_scripts/extrair_municipios_ibge_json.py',
    dag=dag
)

task8 = SparkSubmitOperator(
    task_id='extrair_coordenadas_municipios',
    conn_id='spark_local',
    jars='/usr/local/airflow/jars/aws-java-sdk-dynamodb-1.11.534.jar,\
                                /usr/local/airflow/jars/aws-java-sdk-core-1.11.534.jar,\
                                /usr/local/airflow/jars/aws-java-sdk-s3-1.11.534.jar,\
                                /usr/local/airflow/jars/spark-excel_2.12-0.13.7.jar,\
                                /usr/local/airflow/jars/hadoop-aws-3.2.2.jar'.replace(' ', ''),
    application='/usr/local/airflow/dags/spark_scripts/extrair_coordenadas_municipios_json.py',
    dag=dag
)

task9 = SparkSubmitOperator(
    task_id='extrair_coordenadas_estados',
    conn_id='spark_local',
    jars='/usr/local/airflow/jars/aws-java-sdk-dynamodb-1.11.534.jar,\
                                /usr/local/airflow/jars/aws-java-sdk-core-1.11.534.jar,\
                                /usr/local/airflow/jars/aws-java-sdk-s3-1.11.534.jar,\
                                /usr/local/airflow/jars/spark-excel_2.12-0.13.7.jar,\
                                /usr/local/airflow/jars/hadoop-aws-3.2.2.jar'.replace(' ', ''),
    application='/usr/local/airflow/dags/spark_scripts/extrair_coordenadas_estados_json.py',
    dag=dag
)

dag_finish = DummyOperator(
    task_id='dag_finish',
    dag=dag
)

start_dag >> [task1, task4, task7, task8, task9]
task1 >> task2 >> task3
task4 >> task5 >> task6
[task3, task6, task7, task8, task9] >> dag_finish

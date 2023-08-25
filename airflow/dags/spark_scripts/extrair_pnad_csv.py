import pandas as pd
import boto3
import configparser
import os

config = configparser.ConfigParser()
config.read('./config/config.ini')

# Defina as informações de conexão com o MinIO
minio_access_key = config['MinIO']['access_key']
minio_secret_key = config['MinIO']['secret_key']
minio_endpoint = config['MinIO']['endpoint']

# Bucket de origem dos arquivos XLS
minio_bucket = config['Bucket']['bucket_name']
source_prefix = config['Bucket']['prefix_pnad_xls']

# Bucket de destino dos arquivos CSV
target_prefix = config['Bucket']['prefix_pnad_csv']

# from pyspark.sql import SparkSession

# spark = SparkSession.builder \
#     .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.2,com.crealytics:spark-excel_2.12:0.13.7") \
#     .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
#     .config("spark.hadoop.fs.s3a.access.key", "aulafia") \
#     .config("spark.hadoop.fs.s3a.secret.key", "aulafia@123") \
#     .config("spark.hadoop.fs.s3a.path.style.access", "true") \
#     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
#     .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
#     .getOrCreate()


# Defina as informações de conexão com o MinIO
minio_access_key = minio_access_key
minio_secret_key = minio_secret_key
minio_endpoint = minio_endpoint
# minio_bucket = 'raw'

# Crie uma sessão do boto3 para acessar o MinIO
s3_client = boto3.client('s3', endpoint_url=minio_endpoint,
                         aws_access_key_id=minio_access_key,
                         aws_secret_access_key=minio_secret_key)

# Listar os objetos no bucket "raw"
bucket_objects = s3_client.list_objects(Bucket=minio_bucket, Prefix=source_prefix)['Contents']

# Iterar sobre os objetos e encontrar arquivos XLSX ou XLS
for obj in bucket_objects:
    file_key = obj['Key']
    if file_key.lower().endswith('.xlsx') or file_key.lower().endswith('.xls'):
        # Carregar o arquivo XLSX usando pandas
        xlsx_data = s3_client.get_object(Bucket=minio_bucket, Key=file_key)['Body'].read()
        xls_df = pd.read_excel(xlsx_data, sheet_name=None)  # None carrega todas as abas
        # Salvar cada aba como um arquivo CSV no bucket "raw"
        for sheet_name, sheet_data in xls_df.items():
            # A linha abaixo estava adicionando a estrutura
            # de origem do bucket
            # csv_filename = f"{file_key}-{sheet_name}.csv"
            # Linha corrigida
            csv_filename = f"{os.path.splitext(os.path.basename(file_key))[0]}-{sheet_name}.csv"
            csv_data = sheet_data.to_csv(index=False)
            # Enviar o arquivo CSV de volta para o MinIO
            s3_client.put_object(Bucket=minio_bucket, Key=target_prefix+csv_filename, Body=csv_data.encode())
            print(f"Arquivo CSV {csv_filename} gerado e enviado ao MinIO com sucesso!")

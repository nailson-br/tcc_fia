from pyspark.sql import SparkSession
import requests
import json
import configparser
from minio import Minio

config = configparser.ConfigParser()
config.read('./config/config.ini')

# URL para download do arquivo
municipios_ibge_url = config['URL']['municipios_ibge']

# Defina as informações de conexão com o MinIO
minio_access_key = config['MinIO']['access_key']
minio_secret_key = config['MinIO']['secret_key']
minio_endpoint = config['MinIO']['endpoint']

# Bucket onde será armazenado o arquivo zip baixado da internet
minio_bucket = config['Bucket']['bucket_raw']
prefix = config['Bucket']['prefix_municipios_ibge_json']

# Nome do arquivo que será gravado no MinIO
municipios_ibge = config['FILE']['municipios_ibge_json']

# Inicializar a sessão Spark
spark = (SparkSession.builder
         .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
         .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
         .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
         .config("spark.hadoop.fs.s3a.path.style.access", "true")
         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
         .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
         .getOrCreate()
         )

# Faz a requisição para obter os dados JSON
response = requests.get(municipios_ibge_url)
json_data = response.content.decode('utf-8')  # Decodifica usando UTF-8
# json_data = response.json()

# Transforma os dados JSON em uma string
json_string = json.dumps(json_data)

# Anter de gravar, verificar se o diretório já existe e apagar.
# O método saveAsTextFile não tem o modo 'overwrite'.

# Configuração das credenciais do MinIO
# minio_client = Minio(
#     minio_endpoint,
#     access_key=minio_access_key,
#     secret_key=minio_secret_key,
#     secure=False  # Define como False para usar HTTP em vez de HTTPS
# )

# Define o caminho completo do diretório a ser excluído
directory_to_delete = "s3a://" + minio_bucket + "/" + prefix + municipios_ibge

# Excluir objetos no diretório existente, se ele existir
# try:
#     objects = minio_client.list_objects(minio_bucket, prefix=directory_to_delete, recursive=True)
#     for obj in objects:
#         minio_client.remove_object(minio_bucket, obj.object_name)
# except Exception as e:
#     print("Erro ao excluir objetos:", str(e))


# Grava a string JSON no MinIO
rdd = spark.sparkContext.parallelize([json_string])
rdd.saveAsTextFile(directory_to_delete)

# Encerra a sessão Spark
spark.stop()
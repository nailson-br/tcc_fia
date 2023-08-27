from pyspark.sql import SparkSession
from io import BytesIO
import zipfile
import boto3
import configparser

config = configparser.ConfigParser()
config.read('./config/config.ini')

# bases_pnad = config['URL']['bases_pnad']

# Defina as informações de conexão com o MinIO
minio_access_key = config['MinIO']['access_key']
minio_secret_key = config['MinIO']['secret_key']
minio_endpoint = config['MinIO']['endpoint']

# Bucket de origem do arquivo
minio_bucket = config['Bucket']['bucket_raw']
source_prefix = config['Bucket']['prefix_pnad_zip']

# Bucket de destino dos arquivos XLSX
target_prefix = config['Bucket']['prefix_pnad_xls']

# Nome do arquivo de origem
# ToDo - Mudar essa rotina para pegar todos os arquivos zips recursivamente
file_name = config['FILE']['bases_pnad_zip']

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

# URL do arquivo zip
# LINHAS ABAIXO COMENTADAS - ACHO QUE NÃO ESTÃO SENDO NECESSÁRIAS
# zip_file_url = "https://dilxtq.dm.files.1drv.com/y4mLl3msS82sp3ccvgRoo_ntVPbzAJuS33DlgXnwVBRg6yyAalz_JSTa2Pi2ft5ubsclIBSFDx7NflTkgsb9qIw8Kq4S1oDwfypyBrSz8oj9wpL-tzYh3TZJNv5JJDNfoLYBL0H8p-XehLdEdjh80cgRiwesJXaPLA_pscwS9fqKM_sLWSN9y-7ZjDGL34DGAMjnXK6H0W0mKsc1KCdyUGXyg"
# # Caminho do bucket raw no MinIO
# #minio_raw_bucket = "s3a://minio-bucket/raw/"
# minio_raw_bucket = "raw"
# # Nome desejado para o arquivo no MinIO
# target_file_name = file_name

print("-------------------------------------------")
print("--    Função para extração dos xlsx      --")
print("-------------------------------------------")

# Função para extrair arquivos do arquivo zip
def extract_zip(zip_data):
    zip_buffer = BytesIO(zip_data)
    with zipfile.ZipFile(zip_buffer, 'r') as zip_ref:
        extracted_files = []
        for file_info in zip_ref.infolist():
            file_data = zip_ref.read(file_info)
            extracted_files.append((file_info.filename, file_data))
        return extracted_files
    
# Função para fazer upload dos arquivos extraídos para o MinIO
def upload_extracted_files(extracted_files):
    s3_client = boto3.client('s3', endpoint_url=minio_endpoint,
                             aws_access_key_id=minio_access_key,
                             aws_secret_access_key=minio_secret_key)
    
    for filename, file_data in extracted_files:
        s3_client.put_object(Bucket=minio_bucket, Key=target_prefix+filename, Body=file_data)
        print(f"Arquivo {filename} enviado ao MinIO com sucesso!")
        
# Carregue o arquivo zip do MinIO
s3_client = boto3.client('s3', endpoint_url=minio_endpoint,
                         aws_access_key_id=minio_access_key,
                         aws_secret_access_key=minio_secret_key)
zip_data = s3_client.get_object(Bucket=minio_bucket, Key=source_prefix+file_name)['Body'].read()

# Extraia os arquivos do zip
extracted_files = extract_zip(zip_data)

# Faça upload dos arquivos extraídos de volta para o MinIO
upload_extracted_files(extracted_files)

# Encerre a sessão Spark
spark.stop()
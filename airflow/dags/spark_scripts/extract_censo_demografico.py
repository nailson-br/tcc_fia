from pyspark.sql import SparkSession
import requests
from io import BytesIO
import boto3
import configparser

config = configparser.ConfigParser()
config.read('./config/config.ini')

bases_censo = config['URL']['bases_censo']

# Defina as informações de conexão com o MinIO
minio_access_key = config['MinIO']['access_key']
minio_secret_key = config['MinIO']['secret_key']
minio_endpoint = config['MinIO']['endpoint']

# Bucket onde será armazenado o arquivo zip baixado da internet
minio_bucket = config['Bucket']['bucket_name']
prefix = config['Bucket']['prefix_zip']

# URL para download do arquivo
zip_file_url = config['URL']['bases_censo']

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

def download_url(url, chunk_size=128):
    r = requests.get(url, stream=True)
    buffer = BytesIO()

    for chunk in r.iter_content(chunk_size=chunk_size):
        buffer.write(chunk)

    buffer.seek(0)
    return buffer

# URL do arquivo zip
# zip_file_url = "https://dilxtq.dm.files.1drv.com/y4mLl3msS82sp3ccvgRoo_ntVPbzAJuS33DlgXnwVBRg6yyAalz_JSTa2Pi2ft5ubsclIBSFDx7NflTkgsb9qIw8Kq4S1oDwfypyBrSz8oj9wpL-tzYh3TZJNv5JJDNfoLYBL0H8p-XehLdEdjh80cgRiwesJXaPLA_pscwS9fqKM_sLWSN9y-7ZjDGL34DGAMjnXK6H0W0mKsc1KCdyUGXyg"
# Caminho do bucket raw no MinIO
#minio_raw_bucket = "s3a://minio-bucket/raw/"
# minio_raw_bucket = "raw"
# minio_bucket = config["MinIO"]["bucket_zip"]
# Nome desejado para o arquivo no MinIO
target_file_name = config['FILE']['bases_censo_zip']

# Use a função para fazer o download
downloaded_data = download_url(zip_file_url)

# Upload do arquivo para o MinIO
s3_client = boto3.client('s3', endpoint_url=minio_endpoint,
                            aws_access_key_id=minio_access_key,
                            aws_secret_access_key=minio_secret_key)
s3_client.put_object(Bucket=minio_bucket, Key=prefix+target_file_name, Body=downloaded_data)

# Encerre a sessão Spark
spark.stop()
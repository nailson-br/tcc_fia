from pyspark.sql import SparkSession
import requests
from io import BytesIO
import boto3

# Inicializar a sessão Spark
spark = (SparkSession.builder
         .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
         .config("spark.hadoop.fs.s3a.access.key", "aulafia")
         .config("spark.hadoop.fs.s3a.secret.key", "aulafia@123")
         .config("spark.hadoop.fs.s3a.path.style.access", "true")
         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
         .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
         .getOrCreate()
         )

print("-----------------------------------------")
print("--    Sessão Spark inicializada        --")
print("-----------------------------------------")

def download_url(url, chunk_size=128):
    r = requests.get(url, stream=True)
    buffer = BytesIO()

    for chunk in r.iter_content(chunk_size=chunk_size):
        buffer.write(chunk)

    buffer.seek(0)
    return buffer

# URL do arquivo zip
zip_file_url = "https://dilxtq.dm.files.1drv.com/y4mLl3msS82sp3ccvgRoo_ntVPbzAJuS33DlgXnwVBRg6yyAalz_JSTa2Pi2ft5ubsclIBSFDx7NflTkgsb9qIw8Kq4S1oDwfypyBrSz8oj9wpL-tzYh3TZJNv5JJDNfoLYBL0H8p-XehLdEdjh80cgRiwesJXaPLA_pscwS9fqKM_sLWSN9y-7ZjDGL34DGAMjnXK6H0W0mKsc1KCdyUGXyg"
# Caminho do bucket raw no MinIO
#minio_raw_bucket = "s3a://minio-bucket/raw/"
minio_raw_bucket = "raw"
# Nome desejado para o arquivo no MinIO
target_file_name = "BasesCenso.zip"

# Use a função para fazer o download
downloaded_data = download_url(zip_file_url)

print("-----------------------------------------")
print("--       Iniciando a ingestão          --")
print("-----------------------------------------")
# Upload do arquivo para o MinIO
s3_client = boto3.client('s3', endpoint_url="http://minio:9000",
                            aws_access_key_id="aulafia",
                            aws_secret_access_key="aulafia@123")
s3_client.put_object(Bucket=minio_raw_bucket, Key=target_file_name, Body=downloaded_data)
print("------------------------------------------------")
print("--   Arquivo enviado ao MinIO com sucesso!    --")
print("------------------------------------------------")

# Encerre a sessão Spark
spark.stop()
from pyspark.sql import SparkSession
from io import BytesIO
import zipfile
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

# URL do arquivo zip
zip_file_url = "https://dilxtq.dm.files.1drv.com/y4mLl3msS82sp3ccvgRoo_ntVPbzAJuS33DlgXnwVBRg6yyAalz_JSTa2Pi2ft5ubsclIBSFDx7NflTkgsb9qIw8Kq4S1oDwfypyBrSz8oj9wpL-tzYh3TZJNv5JJDNfoLYBL0H8p-XehLdEdjh80cgRiwesJXaPLA_pscwS9fqKM_sLWSN9y-7ZjDGL34DGAMjnXK6H0W0mKsc1KCdyUGXyg"
# Caminho do bucket raw no MinIO
#minio_raw_bucket = "s3a://minio-bucket/raw/"
minio_raw_bucket = "raw"
# Nome desejado para o arquivo no MinIO
target_file_name = "BasesCenso.zip"

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
    s3_client = boto3.client('s3', endpoint_url="http://minio:9000",
                             aws_access_key_id="aulafia",
                             aws_secret_access_key="aulafia@123")
    
    for filename, file_data in extracted_files:
        s3_client.put_object(Bucket=minio_raw_bucket, Key=filename, Body=file_data)
        print(f"Arquivo {filename} enviado ao MinIO com sucesso!")
        
# Carregue o arquivo zip do MinIO
s3_client = boto3.client('s3', endpoint_url="http://minio:9000",
                         aws_access_key_id="aulafia",
                         aws_secret_access_key="aulafia@123")
zip_data = s3_client.get_object(Bucket=minio_raw_bucket, Key=target_file_name)['Body'].read()

# Extraia os arquivos do zip
extracted_files = extract_zip(zip_data)

# Faça upload dos arquivos extraídos de volta para o MinIO
upload_extracted_files(extracted_files)

# Encerre a sessão Spark
spark.stop()
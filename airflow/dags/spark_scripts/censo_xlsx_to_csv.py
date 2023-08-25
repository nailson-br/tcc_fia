from pyspark.sql import SparkSession
import boto3
import configparser
import os

config = configparser.ConfigParser()
config.read('./config/config.ini')

# bases_censo = config['URL']['bases_censo']

# Defina as informações de conexão com o MinIO
minio_access_key = config['MinIO']['access_key']
minio_secret_key = config['MinIO']['secret_key']
minio_endpoint = config['MinIO']['endpoint']

# Bucket de origem do arquivo
minio_bucket = config['Bucket']['bucket_name']
source_prefix = config['Bucket']['prefix_xls']

# Bucket de destino dos arquivos XLSX
target_prefix = config['Bucket']['prefix_csv']

# Inicializar a sessão Spark
spark = (SparkSession.builder
         .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
         .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
         .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
         .config("spark.hadoop.fs.s3a.path.style.access", "true")
         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
         .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
         .config("spark.jars.packages", "com.crealytics:spark-excel_2.12:0.13.7")
         .getOrCreate()
         )


# Função para ler arquivos xlsx e gerar arquivos csv
def convert_xlsx_to_csv(minio_file_key):
    s3_client = boto3.client('s3', endpoint_url=minio_endpoint,
                             aws_access_key_id=minio_access_key,
                             aws_secret_access_key=minio_secret_key)
    
    # Ler o arquivo xlsx
    xlsx_data = s3_client.get_object(Bucket=minio_bucket, Key=minio_file_key)['Body'].read()

    # Converter para DataFrame
    df = spark.read.format("org.apache.spark.sql.execution.datasources.xlsx").option("header", "true").load(xlsx_data)
    
    # Extrair as abas (planilhas) do arquivo xlsx
    sheets = df.select("sheet").distinct().collect()

    for sheet_row in sheets:
        sheet_name = sheet_row.sheet

        # Filtrar o DataFrame pela aba (planilha) atual
        filtered_df = df.filter(df.sheet == sheet_name)

        # Definir o nome do arquivo CSV
        csv_filename = f"{os.path.splitext(minio_file_key)[0]}-{sheet_name}.csv"

        # Gravar o DataFrame filtrado como arquivo CSV no mesmo bucket
        csv_data = filtered_df.toPandas().to_csv(index=False)
        s3_client.put_object(Bucket=minio_bucket, Key=target_prefix+csv_filename, Body=csv_data)

        print(f"Arquivo CSV {csv_filename} gerado e enviado ao MinIO com sucesso!")

# Listar os arquivos no bucket "raw"
s3_client = boto3.client('s3', endpoint_url=minio_endpoint,
                         aws_access_key_id=minio_access_key,
                         aws_secret_access_key=minio_secret_key)
bucket_objects = s3_client.list_objects(Bucket=minio_bucket, Prefix=source_prefix)['Contents']

# Filtrar arquivos xlsx e gerar arquivos CSV para cada aba
for obj in bucket_objects:
    if obj['Key'].lower().endswith('.xlsx'):
        convert_xlsx_to_csv(obj['Key'])

# Encerrar a sessão Spark
spark.stop()
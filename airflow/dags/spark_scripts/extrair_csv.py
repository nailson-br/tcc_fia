import pandas as pd
import boto3

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
minio_access_key = 'aulafia'
minio_secret_key = 'aulafia@123'
minio_endpoint = 'http://minio:9000'
minio_bucket = 'raw'

print("")
print("")
print("")
print("INICIANDO SESSÃO SPARK")
print("")
print("")
print("")

# Crie uma sessão do boto3 para acessar o MinIO
s3_client = boto3.client('s3', endpoint_url=minio_endpoint,
                         aws_access_key_id=minio_access_key,
                         aws_secret_access_key=minio_secret_key)


print("")
print("")
print("")
print("LISTANDO OBJETOS NO BUCKET")
print("")
print("")
print("")
# Listar os objetos no bucket "raw"
bucket_objects = s3_client.list_objects(Bucket=minio_bucket)['Contents']

print("")
print("")
print("")
print("ITERANDO NOS OBJETOS")
print("")
print("")
print("")
# Iterar sobre os objetos e encontrar arquivos XLSX ou XLS
for obj in bucket_objects:
    file_key = obj['Key']
    if file_key.lower().endswith('.xlsx') or file_key.lower().endswith('.xls'):
        print("")
        print("")
        print("")
        print("CARREGANDO OS ARQUIVOS USANDO PANDAS")
        print("")
        print("")
        print("")
        # Carregar o arquivo XLSX usando pandas
        print("")
        print("")
        print("")
        print("ARQUIVO:")
        print(file_key)
        print("")
        print("")
        xlsx_data = s3_client.get_object(Bucket=minio_bucket, Key=file_key)['Body'].read()
        print("")
        print("")
        print("")
        print("ABRINDO O EXCEL >>>>>>>>>>>>>>>>>>>>>>>>>>>")
        print("")
        print("")
        print("")
        xls_df = pd.read_excel(xlsx_data, sheet_name=None)  # None carrega todas as abas
        
        print("")
        print("")
        print("")
        print("SALVANDO ABAS")
        print("")
        print("")
        print("")
        # Salvar cada aba como um arquivo CSV no bucket "raw"
        for sheet_name, sheet_data in xls_df.items():
            csv_filename = f"{file_key}-{sheet_name}.csv"
            csv_data = sheet_data.to_csv(index=False)
               
            print("")
            print("")
            print("")
            print("ENVIANDO PARA O BUCKET")
            print("")
            print("")
            print("")
            # Enviar o arquivo CSV de volta para o MinIO
            s3_client.put_object(Bucket=minio_bucket, Key=csv_filename, Body=csv_data.encode())
            print(f"Arquivo CSV {csv_filename} gerado e enviado ao MinIO com sucesso!")

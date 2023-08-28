import boto3
from configparser import ConfigParser
import pandas as pd
import psycopg2
from io import BytesIO

# Carregar as configurações do arquivo ini
config = ConfigParser()
config.read("./config/config.ini")

# Configurações para conexão com o MinIO/S3
endpoint = config.get("MinIO", "endpoint")
access_key = config.get("MinIO", "access_key")
secret_key = config.get("MinIO", "secret_key")
bucket_trust = config.get("Bucket", "bucket_trust")

# Nome do arquivo a ser lido
source_filename = config.get("FILE", "censo_saude_csv")

# Configurar as credenciais do PostgreSQL
postgres_host = config.get("POSTGRESQL", "host_name")
postgres_port = config.get("POSTGRESQL", "port")  # Porta padrão do PostgreSQL
postgres_user = config.get("POSTGRESQL", "user")
postgres_password = config.get("POSTGRESQL", "user_pwd")
postgres_db = config.get("POSTGRESQL", "db_name")

# Inicializar o cliente boto3 para S3
minio_client = boto3.client("s3", 
                            endpoint_url=endpoint,
                            aws_access_key_id=access_key,
                            aws_secret_access_key=secret_key
)

# Obter o conteúdo do arquivo CSV do MinIO
response = minio_client.get_object(Bucket=bucket_trust, Key=source_filename)
csv_content = response['Body'].read()

# Ler o conteúdo do CSV em um DataFrame
data_frame = pd.read_csv(BytesIO(csv_content), dtype=str)

# Conectar ao PostgreSQL
postgres_connection = psycopg2.connect(
    host=postgres_host,
    port=postgres_port,
    user=postgres_user,
    password=postgres_password,
    dbname=postgres_db
)

try:
    # Apagar a tabela censo_saude_municipios (se existir)
    with postgres_connection.cursor() as cursor:
        cursor.execute("DROP TABLE IF EXISTS censo_saude_municipios")

    # Criar novamente a tabela censo_saude_municipios
    with postgres_connection.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE censo_saude_municipios (
                ANO VARCHAR(4),
                UF VARCHAR(2),
                Codmun6 VARCHAR(6),
                Codmun7 VARCHAR(7),
                Município VARCHAR(255),
                ESPVIDA VARCHAR(10),
                FECTOT VARCHAR(10),
                MORT1 VARCHAR(10),
                MORT5 VARCHAR(10),
                RAZDEP VARCHAR(10),
                SOBRE40 VARCHAR(10),
                SOBRE60 VARCHAR(10),
                T_ENV VARCHAR(10)
            )
        """)
        postgres_connection.commit()

    # Inserir os dados na tabela censo_saude_municipios
    with postgres_connection.cursor() as cursor:
        for index, row in data_frame.iterrows():
            sql = "INSERT INTO censo_saude_municipios VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            values = (row['ANO'], row['UF'], row['Codmun6'], row['Codmun7'], row['Município'],
                      row['ESPVIDA'], row['FECTOT'], row['MORT1'], row['MORT5'], row['RAZDEP'],
                      row['SOBRE40'], row['SOBRE60'], row['T_ENV'])
            cursor.execute(sql, values)
        postgres_connection.commit()

    print(f'Dados do arquivo {source_filename} inseridos na tabela censo_saude_municipios com sucesso.')

finally:
    postgres_connection.close()
    
minio_client.close()

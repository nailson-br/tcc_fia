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
bucket_context = config.get("Bucket", "bucket_context")

# Nome do arquivo a ser lido
source_filename = config.get("FILE", "coordenadas_municipios_csv")

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
response = minio_client.get_object(Bucket=bucket_context, Key=source_filename)
csv_content = response['Body'].read()

# Ler o conteúdo do CSV em um DataFrame
data_frame = pd.read_csv(BytesIO(csv_content))

# Conectar ao PostgreSQL
postgres_connection = psycopg2.connect(
    host=postgres_host,
    port=postgres_port,
    user=postgres_user,
    password=postgres_password,
    dbname=postgres_db
)

try:
    # Apagar a tabela coordenadas_municipios (se existir)
    with postgres_connection.cursor() as cursor:
        cursor.execute("DROP TABLE IF EXISTS coordenadas_municipios")

    # Criar novamente a tabela coordenadas
    with postgres_connection.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE coordenadas_municipios (
                codigo_ibge VARCHAR(100),
                nome VARCHAR(100),
                latitude VARCHAR(20),
                longitude VARCHAR(20),
                capital VARCHAR(4),
                codigo_uf VARCHAR(4),
                siafi_id VARCHAR(4),
                ddd VARCHAR(4),
                fuso_horario VARCHAR(32)
            )
        """)
    postgres_connection.commit()

    # Inserir os dados na tabela coordenadas_municipios
    with postgres_connection.cursor() as cursor:
        for index, row in data_frame.iterrows():
            sql = "INSERT INTO coordenadas_municipios VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
            values = (row['codigo_ibge'], row['nome'], row['latitude'], row['longitude'],
                      row['capital'], row['codigo_uf'], row['siafi_id'], row['ddd'], row['fuso_horario'])
            cursor.execute(sql, values)
        postgres_connection.commit()

    print(f'Dados do arquivo {source_filename} inseridos na tabela coordenadas_municipios com sucesso.')

finally:
    postgres_connection.close()
    
minio_client.close()

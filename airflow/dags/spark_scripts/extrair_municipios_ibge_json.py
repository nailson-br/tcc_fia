import pandas as pd
import requests
from configparser import ConfigParser
import boto3
from botocore.exceptions import NoCredentialsError
import io

# Carregar as configurações do arquivo ini
config = ConfigParser()
config.read('./config/config.ini')

# Caminho para download dos dados
url_municipios_ibge = config.get("URL", "municipios_ibge")

# Fazer a requisição para obter o JSON da URL
response = requests.get(url_municipios_ibge)
municipios_data = response.json()

# Carregar o JSON em um DataFrame Pandas
df = pd.json_normalize(municipios_data)

# Configurações para conexão com o MinIO/S3
endpoint = config.get("MinIO", "endpoint")
access_key = config.get("MinIO", "access_key")
secret_key = config.get("MinIO", "secret_key")
bucket_raw = config.get("Bucket", "bucket_raw")
bucket_context = config.get("Bucket", "bucket_context")
prefix_municipios_ibge_json = config.get("Bucket", "prefix_municipios_ibge_json")
# prefix_municipios_ibge_csv = config.get("Bucket", "prefix_municipios_ibge_csv") # Não está sendo utilizado

# Inicializar o cliente boto3 para S3
s3_client = boto3.client(
    "s3",
    endpoint_url=endpoint,
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
)

# Armazenar o JSON no bucket 'raw'
json_filename = config.get("FILE", "municipios_ibge_json")
json_s3_path = f"{prefix_municipios_ibge_json}{json_filename}"

s3_client.put_object(Bucket=bucket_raw,
                     Key=prefix_municipios_ibge_json 
                     + json_filename, 
                     Body=response.content)

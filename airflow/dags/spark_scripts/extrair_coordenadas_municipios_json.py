import pandas as pd
import requests
from configparser import ConfigParser
import boto3
from botocore.exceptions import NoCredentialsError
import io
import json
import codecs

# Carregar as configurações do arquivo ini
config = ConfigParser()
config.read("./config/config.ini")

# Caminho para download dos dados
url_coordenadas_municipios = config.get('URL', 'coordenadas_municipios_ibge')

# Fazer a requisição para obter o JSON da URL
response = requests.get(url_coordenadas_municipios)

# Decodificar o conteúdo da resposta removendo o BOM
content = response.content.decode('utf-8-sig')
coordenadas_data = json.loads(content)

# Carregar o JSON em um DataFrame Pandas
df = pd.json_normalize(coordenadas_data)

# Configurações para conexão com o MinIO/S3
endpoint = config.get("MinIO", "endpoint")
access_key = config.get("MinIO", "access_key")
secret_key = config.get("MinIO", "secret_key")
bucket_raw = config.get("Bucket", "bucket_raw")
prefix_coordenadas_json = config.get("Bucket", "prefix_coordenadas_json")

# Inicializar o cliente boto3 para S3
minio_client = boto3.client("s3", 
                            endpoint_url=endpoint,
                            aws_access_key_id=access_key,
                            aws_secret_access_key=secret_key
)

# Nome e caminho com o qual o arquivo será gravado
json_filename = config.get('FILE', 'coordenadas_municipios_json')
json_s3_path = f"{prefix_coordenadas_json}{json_filename}"

# Armazenar o JSON no bucket 'raw'
minio_client.put_object(Bucket=bucket_raw,
                        Key=prefix_coordenadas_json + 
                        json_filename, 
                        Body=response.content)

minio_client.close()
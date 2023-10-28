import boto3
from configparser import ConfigParser
import json
import pandas as pd
import io

# Carregar as configurações do arquivo ini
config = ConfigParser()
config.read("./config/config.ini")

# Configurações para conexão com o MinIO/S3
endpoint = config.get("MinIO", "endpoint")
access_key = config.get("MinIO", "access_key")
secret_key = config.get("MinIO", "secret_key")
bucket_raw = config.get("Bucket", "bucket_raw")
bucket_context = config.get("Bucket", "bucket_context")
prefix_coordenadas_estados_json = config.get("Bucket", "prefix_coordenadas_estados_json") # Prefixo do arquivo CSV na raw
prefix_coordenadas_estados_csv = config.get("Bucket", "prefix_coordenadas_estados_csv") # Prefixo do arquivo somente com as informações de saúde que vai para a context

# Nome do arquivo a ser lido
source_filename = config.get("FILE", "coordenadas_estados_json")
target_filename = config.get("FILE", "coordenadas_estados_csv")

# Inicializar o cliente boto3 para S3
minio_client = boto3.client("s3", 
                            endpoint_url=endpoint,
                            aws_access_key_id=access_key,
                            aws_secret_access_key=secret_key
)

# Obter o objeto do arquivo do bucket de origem
response = minio_client.get_object(Bucket=bucket_raw,
                                   Key=prefix_coordenadas_estados_json + 
                                   source_filename)
json_content = response['Body'].read()

# Decodificar o conteúdo JSON
decoded_json = json.loads(json_content)

# Converter o JSON para um DataFrame do pandas
data_frame = pd.json_normalize(decoded_json)

# Criar um fluxo de gravação em buffer para o arquivo CSV
output_buffer = io.StringIO()

# Salvar o DataFrame como um arquivo CSV no buffer
data_frame.to_csv(output_buffer, index=False, encoding='utf-8')

# Obter o conteúdo do buffer como bytes codificados em UTF-8
encoded_csv_content = output_buffer.getvalue().encode('utf-8')

# Enviar o arquivo CSV codificado para o bucket de destino
minio_client.put_object(Bucket=bucket_context,
                        Key=target_filename,
                        Body=encoded_csv_content)

minio_client.close()
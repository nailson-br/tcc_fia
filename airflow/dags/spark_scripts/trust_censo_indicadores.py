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
bucket_trust = config.get("Bucket", "bucket_trust")
bucket_raw = config.get("Bucket", "bucket_raw")

# Prefixo, nome do arquivo a ser lido e nome a ser gravado
prefix = config.get("Bucket", "prefix_censo_csv")
source_filename = config.get("RAW_BUCKET_FILES", "censo_indicadores_csv")
target_filename = config.get("FILE", "censo_indicadores_csv")

# Inicializar o cliente boto3 para S3
minio_client = boto3.client("s3", 
                            endpoint_url=endpoint,
                            aws_access_key_id=access_key,
                            aws_secret_access_key=secret_key
)

# Baixar o arquivo CSV do bucket 'raw'
response = minio_client.get_object(Bucket=bucket_raw,
                         Key=prefix + source_filename)
csv_content = response['Body'].read()

# Carregar o conteúdo CSV em um DataFrame do pandas
data_frame = pd.read_csv(io.BytesIO(csv_content))

# Criar uma coluna 'classe' com valores de acordo com as regras analisadas no arquivo fonte
data_frame['classe'] = ["identificação"] * 5 + ["População"] * 8 + ["Educação"] * 57 + ["Renda"] * 33 + ["Trabalho"] * 35 + ["Habitação"] * 8 + ["Educação"] * 2 + ["Habitação"] * 2 + ["Trabalho"] + ["Saúde"] * 2 + ["Trabalho"] * 4 + ["Habitação"] + ["População"] * 36 + ["Trabalho"] * 4 + ["População"] * 33 + ["IDH"] * 6

# Criar um fluxo de gravação em buffer para o novo arquivo CSV
output_buffer = io.StringIO()

# Salvar o DataFrame como um arquivo CSV no buffer
data_frame.to_csv(output_buffer, index=False, encoding='utf-8')

# Obter o conteúdo do buffer como bytes codificados em UTF-8
encoded_csv_content = output_buffer.getvalue().encode('utf-8')

# Enviar o novo arquivo CSV codificado para o bucket de destino
minio_client.put_object(Bucket=bucket_trust, Key=target_filename, Body=encoded_csv_content)

minio_client.close()
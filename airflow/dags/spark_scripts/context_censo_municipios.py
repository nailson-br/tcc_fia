import boto3
from configparser import ConfigParser
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
prefix_censo_csv = config.get("Bucket", "prefix_censo_csv") # Prefixo do arquivo CSV na raw
prefix_censo_municipios_csv = config.get("Bucket", "prefix_censo_municipios_csv") # Prefixo do arquivo somente com as informações de saúde que vai para a context

# Nome do arquivo a ser lido
source_filename = config.get("RAW_BUCKET_FILES", "censo_municipios_csv")
target_filename = config.get("FILE", "censo_municipios_csv")

# Inicializar o cliente boto3 para S3
minio_client = boto3.client("s3", 
                            endpoint_url=endpoint,
                            aws_access_key_id=access_key,
                            aws_secret_access_key=secret_key
)

# Obter o objeto do arquivo do bucket de origem
response = minio_client.get_object(Bucket=bucket_raw,
                                   Key=prefix_censo_csv + 
                                   source_filename)
csv_stream = response['Body']

# Iniciar um fluxo de gravação em buffer para o arquivo de destino
output_buffer = io.BytesIO()

# Ler e escrever em partes para lidar com arquivos grandes
chunk_size = 1024 * 1024  # Tamanho de cada parte em bytes (1 MB)

while True:
    chunk = csv_stream.read(chunk_size)
    if not chunk:
        break
    output_buffer.write(chunk)
    
# Decodificar o conteúdo do CSV usando UTF-8
decoded_csv_content = output_buffer.getvalue().decode('utf-8')

# Codificar novamente o conteúdo em UTF-8
encoded_csv_content = decoded_csv_content.encode('utf-8')

# Enviar o arquivo codificado para o bucket de destino
minio_client.put_object(Bucket=bucket_context,
                        Key=target_filename,
                        Body=encoded_csv_content)

minio_client.close()
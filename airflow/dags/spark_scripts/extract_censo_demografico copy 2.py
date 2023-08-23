# Arquivo original gerado com ChatGPT

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark import SparkFiles
import urllib.request

# URL do arquivo zip
zip_file_url = "https://dilxtq.dm.files.1drv.com/y4mLl3msS82sp3ccvgRoo_ntVPbzAJuS33DlgXnwVBRg6yyAalz_JSTa2Pi2ft5ubsclIBSFDx7NflTkgsb9qIw8Kq4S1oDwfypyBrSz8oj9wpL-tzYh3TZJNv5JJDNfoLYBL0H8p-XehLdEdjh80cgRiwesJXaPLA_pscwS9fqKM_sLWSN9y-7ZjDGL34DGAMjnXK6H0W0mKsc1KCdyUGXyg/Bases Censo.zip"
# Caminho do bucket raw no MinIO
minio_raw_bucket = "s3a://minio-bucket/raw/"
# Nome desejado para o arquivo no MinIO
target_file_name = "Bases Censo.zip"

# Baixar o arquivo zip
with urllib.request.urlopen(zip_file_url) as response:
    with open(target_file_name, "wb") as target_file:
        target_file.write(response.read())

spark = (SparkSession.builder
         .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
         .config("spark.hadoop.fs.s3a.access.key", "aulafia")
         .config("spark.hadoop.fs.s3a.secret.key", "aulafia@123")
         .config("spark.hadoop.fs.s3a.path.style.access", "true")
         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
         .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
         .getOrCreate()
         )

# Faz o upload do arquivo para o MinIO
spark.sparkContext.addFile(target_file_name)
file_path = "file://" + SparkFiles.get(target_file_name)
data = spark.sparkContext.binaryFiles(file_path).take(1)[0][1]
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "aulafia")
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "aulafia@123")

with open(target_file_name, 'wb') as f:
    f.write(data)

s3_path = minio_raw_bucket + target_file_name
spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
    spark.sparkContext._jsc.hadoopConfiguration()).copyFromLocalFile(
        False, True, False, None, spark._jvm.org.apache.hadoop.fs.Path(target_file_name),
        spark._jvm.org.apache.hadoop.fs.Path(s3_path)
    )

print("Upload completed successfully.")

# Encerrar a sessão do Spark
spark.stop()

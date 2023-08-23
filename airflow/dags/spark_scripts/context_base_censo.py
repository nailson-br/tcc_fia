from pyspark.sql import SparkSession
import pandas as pd

spark = (SparkSession.builder
         .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
         .config("spark.hadoop.fs.s3a.access.key", "aulafia")
         .config("spark.hadoop.fs.s3a.secret.key", "aulafia@123")
         .config("spark.hadoop.fs.s3a.path.style.access", True)
         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
         .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
         .getOrCreate()
         )

# Ler a planilha Excel
# df = spark.read.format("com.crealytics.spark.excel") \
#     .option("header", "true") \
#     .option("inferSchema", "true") \
#     .load("s3a://raw/xlsx/Atlas 2013_municipal, estadual e Brasil.xlsx/MUN 91-00-10")
df = pd.read_excel("s3a://raw/xlsx/Atlas 2013_municipal, estadual e Brasil.xlsx", sheet_name="MUN 91-00-10")

# Salvar no MinIO (camada CONTEXT)
(df.write
 .format("parquet")
 .mode("overwrite")
 .save("s3a://context/Atlas_2013_municipal_estadual_Brasil.parquet")
 )

spark.stop()
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
source_filename = config.get("FILE", "censo_completo_csv")

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
    # Apagar a tabela censo_completo_municipios (se existir)
    with postgres_connection.cursor() as cursor:
        cursor.execute("DROP TABLE IF EXISTS censo_completo_municipios")

    # Criar novamente a tabela censo_completo_municipios
    with postgres_connection.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE censo_completo_municipios (
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
                T_ENV VARCHAR(10),
                E_ANOSESTUDO VARCHAR(10),
                T_ANALF11A14 VARCHAR(10),
                T_ANALF15A17 VARCHAR(10),
                T_ANALF15M VARCHAR(10),
                T_ANALF18A24 VARCHAR(10),
                T_ANALF18M VARCHAR(10),
                T_ANALF25A29 VARCHAR(10),
                T_ANALF25M VARCHAR(10),
                T_ATRASO_0_BASICO VARCHAR(10),
                T_ATRASO_0_FUND VARCHAR(10),
                T_ATRASO_0_MED VARCHAR(10),
                T_ATRASO_1_BASICO VARCHAR(10),
                T_ATRASO_1_FUND VARCHAR(10),
                T_ATRASO_1_MED VARCHAR(10),
                T_ATRASO_2_BASICO VARCHAR(10),
                T_ATRASO_2_FUND VARCHAR(10),
                T_ATRASO_2_MED VARCHAR(10),
                T_FBBAS VARCHAR(10),
                T_FBFUND VARCHAR(10),
                T_FBMED VARCHAR(10),
                T_FBPRE VARCHAR(10),
                T_FBSUPER VARCHAR(10),
                T_FLBAS VARCHAR(10),
                T_FLFUND VARCHAR(10),
                T_FLMED VARCHAR(10),
                T_FLPRE VARCHAR(10),
                T_FLSUPER VARCHAR(10),
                T_FREQ0A3 VARCHAR(10),
                T_FREQ11A14 VARCHAR(10),
                T_FREQ15A17 VARCHAR(10),
                T_FREQ18A24 VARCHAR(10),
                T_FREQ25A29 VARCHAR(10),
                T_FREQ4A5 VARCHAR(10),
                T_FREQ4A6 VARCHAR(10),
                T_FREQ5A6 VARCHAR(10),
                T_FREQ6 VARCHAR(10),
                T_FREQ6A14 VARCHAR(10),
                T_FREQ6A17 VARCHAR(10),
                T_FREQFUND1517 VARCHAR(10),
                T_FREQFUND1824 VARCHAR(10),
                T_FREQFUND45 VARCHAR(10),
                T_FREQMED1824 VARCHAR(10),
                T_FREQMED614 VARCHAR(10),
                T_FREQSUPER1517 VARCHAR(10),
                T_FUND11A13 VARCHAR(10),
                T_FUND12A14 VARCHAR(10),
                T_FUND15A17 VARCHAR(10),
                T_FUND16A18 VARCHAR(10),
                T_FUND18A24 VARCHAR(10),
                T_FUND18M VARCHAR(10),
                T_FUND25M VARCHAR(10),
                T_MED18A20 VARCHAR(10),
                T_MED18A24 VARCHAR(10),
                T_MED18M VARCHAR(10),
                T_MED19A21 VARCHAR(10),
                T_MED25M VARCHAR(10),
                T_SUPER25M VARCHAR(10),
                CORTE1 VARCHAR(10),
                CORTE2 VARCHAR(10),
                CORTE3 VARCHAR(10),
                CORTE4 VARCHAR(10),
                CORTE9 VARCHAR(10),
                GINI VARCHAR(10),
                PIND VARCHAR(10),
                PINDCRI VARCHAR(10),
                PMPOB VARCHAR(10),
                PMPOBCRI VARCHAR(10),
                PPOB VARCHAR(10),
                PPOBCRI VARCHAR(10),
                PREN10RICOS VARCHAR(10),
                PREN20 VARCHAR(10),
                PREN20RICOS VARCHAR(10),
                PREN40 VARCHAR(10),
                PREN60 VARCHAR(10),
                PREN80 VARCHAR(10),
                PRENTRAB VARCHAR(10),
                R1040 VARCHAR(10),
                R2040 VARCHAR(10),
                RDPC VARCHAR(10),
                RDPC1 VARCHAR(10),
                RDPC10 VARCHAR(10),
                RDPC2 VARCHAR(10),
                RDPC3 VARCHAR(10),
                RDPC4 VARCHAR(10),
                RDPC5 VARCHAR(10),
                RDPCT VARCHAR(10),
                RIND VARCHAR(10),
                RMPOB VARCHAR(10),
                RPOB VARCHAR(10),
                THEIL VARCHAR(10),
                CPR VARCHAR(10),
                EMP VARCHAR(10),
                P_AGRO VARCHAR(10),
                P_COM VARCHAR(10),
                P_CONSTR VARCHAR(10),
                P_EXTR VARCHAR(10),
                P_FORMAL VARCHAR(10),
                P_FUND VARCHAR(10),
                P_MED VARCHAR(10),
                P_SERV VARCHAR(10),
                P_SIUP VARCHAR(10),
                P_SUPER VARCHAR(10),
                P_TRANSF VARCHAR(10),
                REN0 VARCHAR(10),
                REN1 VARCHAR(10),
                REN2 VARCHAR(10),
                REN3 VARCHAR(10),
                REN5 VARCHAR(10),
                RENOCUP VARCHAR(10),
                T_ATIV VARCHAR(10),
                T_ATIV1014 VARCHAR(10),
                T_ATIV1517 VARCHAR(10),
                T_ATIV1824 VARCHAR(10),
                T_ATIV18M VARCHAR(10),
                T_ATIV2529 VARCHAR(10),
                T_DES VARCHAR(10),
                T_DES1014 VARCHAR(10),
                T_DES1517 VARCHAR(10),
                T_DES1824 VARCHAR(10),
                T_DES18M VARCHAR(10),
                T_DES2529 VARCHAR(10),
                THEILtrab VARCHAR(10),
                TRABCC VARCHAR(10),
                TRABPUB VARCHAR(10),
                TRABSC VARCHAR(10),
                T_AGUA VARCHAR(10),
                T_BANAGUA VARCHAR(10),
                T_DENS VARCHAR(10),
                T_LIXO VARCHAR(10),
                T_LUZ VARCHAR(10),
                AGUA_ESGOTO VARCHAR(10),
                PAREDE VARCHAR(10),
                T_CRIFUNDIN_TODOS VARCHAR(10),
                T_FORA4A5 VARCHAR(10),
                T_FORA6A14 VARCHAR(10),
                T_FUNDIN_TODOS VARCHAR(10),
                T_FUNDIN_TODOS_MMEIO VARCHAR(10),
                T_FUNDIN18MINF VARCHAR(10),
                T_M10A14CF VARCHAR(10),
                T_M15A17CF VARCHAR(10),
                T_MULCHEFEFIF014 VARCHAR(10),
                T_NESTUDA_NTRAB_MMEIO VARCHAR(10),
                T_OCUPDESLOC_1 VARCHAR(10),
                T_RMAXIDOSO VARCHAR(10),
                T_SLUZ VARCHAR(10),
                HOMEM0A4 VARCHAR(10),
                HOMEM10A14 VARCHAR(10),
                HOMEM15A19 VARCHAR(10),
                HOMEM20A24 VARCHAR(10),
                HOMEM25A29 VARCHAR(10),
                HOMEM30A34 VARCHAR(10),
                HOMEM35A39 VARCHAR(10),
                HOMEM40A44 VARCHAR(10),
                HOMEM45A49 VARCHAR(10),
                HOMEM50A54 VARCHAR(10),
                HOMEM55A59 VARCHAR(10),
                HOMEM5A9 VARCHAR(10),
                HOMEM60A64 VARCHAR(10),
                HOMEM65A69 VARCHAR(10),
                HOMEM70A74 VARCHAR(10),
                HOMEM75A79 VARCHAR(10),
                HOMEMTOT VARCHAR(10),
                HOMENS80 VARCHAR(10),
                MULH0A4 VARCHAR(10),
                MULH10A14 VARCHAR(10),
                MULH15A19 VARCHAR(10),
                MULH20A24 VARCHAR(10),
                MULH25A29 VARCHAR(10),
                MULH30A34 VARCHAR(10),
                MULH35A39 VARCHAR(10),
                MULH40A44 VARCHAR(10),
                MULH45A49 VARCHAR(10),
                MULH50A54 VARCHAR(10),
                MULH55A59 VARCHAR(10),
                MULH5A9 VARCHAR(10),
                MULH60A64 VARCHAR(10),
                MULH65A69 VARCHAR(10),
                MULH70A74 VARCHAR(10),
                MULH75A79 VARCHAR(10),
                MULHER80 VARCHAR(10),
                MULHERTOT VARCHAR(10),
                PEA VARCHAR(10),
                PEA1014 VARCHAR(10),
                PEA1517 VARCHAR(10),
                PEA18M VARCHAR(10),
                peso1 VARCHAR(10),
                PESO1114 VARCHAR(10),
                PESO1113 VARCHAR(10),
                PESO1214 VARCHAR(10),
                peso13 VARCHAR(10),
                PESO15 VARCHAR(10),
                peso1517 VARCHAR(10),
                PESO1524 VARCHAR(10),
                PESO1618 VARCHAR(10),
                PESO18 VARCHAR(10),
                Peso1820 VARCHAR(10),
                PESO1824 VARCHAR(10),
                Peso1921 VARCHAR(10),
                PESO25 VARCHAR(10),
                peso4 VARCHAR(10),
                peso5 VARCHAR(10),
                peso6 VARCHAR(10),
                PESO610 VARCHAR(10),
                Peso617 VARCHAR(10),
                PESO65 VARCHAR(10),
                PESOM1014 VARCHAR(10),
                PESOM1517 VARCHAR(10),
                PESOM15M VARCHAR(10),
                PESOM25M VARCHAR(10),
                pesoRUR VARCHAR(10),
                pesotot VARCHAR(10),
                pesourb VARCHAR(10),
                PIA VARCHAR(10),
                PIA1014 VARCHAR(10),
                PIA1517 VARCHAR(10),
                PIA18M VARCHAR(10),
                POP VARCHAR(10),
                POPT VARCHAR(10),
                I_ESCOLARIDADE VARCHAR(10),
                I_FREQ_PROP VARCHAR(10),
                IDHM VARCHAR(10),
                IDHM_E VARCHAR(10),
                IDHM_L VARCHAR(10),
                IDHM_R VARCHAR(10)                
            )
        """)
        postgres_connection.commit()

    # Inserir os dados na tabela censo_completo_municipios
    with postgres_connection.cursor() as cursor:
        for index, row in data_frame.iterrows():
            sql = "INSERT INTO censo_completo_municipios VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            values = (
                row['ANO'],
                row['UF'],
                row['Codmun6'],
                row['Codmun7'],
                row['Município'],
                row['ESPVIDA'],
                row['FECTOT'],
                row['MORT1'],
                row['MORT5'],
                row['RAZDEP'],
                row['SOBRE40'],
                row['SOBRE60'],
                row['T_ENV'],
                row['E_ANOSESTUDO'],
                row['T_ANALF11A14'],
                row['T_ANALF15A17'],
                row['T_ANALF15M'],
                row['T_ANALF18A24'],
                row['T_ANALF18M'],
                row['T_ANALF25A29'],
                row['T_ANALF25M'],
                row['T_ATRASO_0_BASICO'],
                row['T_ATRASO_0_FUND'],
                row['T_ATRASO_0_MED'],
                row['T_ATRASO_1_BASICO'],
                row['T_ATRASO_1_FUND'],
                row['T_ATRASO_1_MED'],
                row['T_ATRASO_2_BASICO'],
                row['T_ATRASO_2_FUND'],
                row['T_ATRASO_2_MED'],
                row['T_FBBAS'],
                row['T_FBFUND'],
                row['T_FBMED'],
                row['T_FBPRE'],
                row['T_FBSUPER'],
                row['T_FLBAS'],
                row['T_FLFUND'],
                row['T_FLMED'],
                row['T_FLPRE'],
                row['T_FLSUPER'],
                row['T_FREQ0A3'],
                row['T_FREQ11A14'],
                row['T_FREQ15A17'],
                row['T_FREQ18A24'],
                row['T_FREQ25A29'],
                row['T_FREQ4A5'],
                row['T_FREQ4A6'],
                row['T_FREQ5A6'],
                row['T_FREQ6'],
                row['T_FREQ6A14'],
                row['T_FREQ6A17'],
                row['T_FREQFUND1517'],
                row['T_FREQFUND1824'],
                row['T_FREQFUND45'],
                row['T_FREQMED1824'],
                row['T_FREQMED614'],
                row['T_FREQSUPER1517'],
                row['T_FUND11A13'],
                row['T_FUND12A14'],
                row['T_FUND15A17'],
                row['T_FUND16A18'],
                row['T_FUND18A24'],
                row['T_FUND18M'],
                row['T_FUND25M'],
                row['T_MED18A20'],
                row['T_MED18A24'],
                row['T_MED18M'],
                row['T_MED19A21'],
                row['T_MED25M'],
                row['T_SUPER25M'],
                row['CORTE1'],
                row['CORTE2'],
                row['CORTE3'],
                row['CORTE4'],
                row['CORTE9'],
                row['GINI'],
                row['PIND'],
                row['PINDCRI'],
                row['PMPOB'],
                row['PMPOBCRI'],
                row['PPOB'],
                row['PPOBCRI'],
                row['PREN10RICOS'],
                row['PREN20'],
                row['PREN20RICOS'],
                row['PREN40'],
                row['PREN60'],
                row['PREN80'],
                row['PRENTRAB'],
                row['R1040'],
                row['R2040'],
                row['RDPC'],
                row['RDPC1'],
                row['RDPC10'],
                row['RDPC2'],
                row['RDPC3'],
                row['RDPC4'],
                row['RDPC5'],
                row['RDPCT'],
                row['RIND'],
                row['RMPOB'],
                row['RPOB'],
                row['THEIL'],
                row['CPR'],
                row['EMP'],
                row['P_AGRO'],
                row['P_COM'],
                row['P_CONSTR'],
                row['P_EXTR'],
                row['P_FORMAL'],
                row['P_FUND'],
                row['P_MED'],
                row['P_SERV'],
                row['P_SIUP'],
                row['P_SUPER'],
                row['P_TRANSF'],
                row['REN0'],
                row['REN1'],
                row['REN2'],
                row['REN3'],
                row['REN5'],
                row['RENOCUP'],
                row['T_ATIV'],
                row['T_ATIV1014'],
                row['T_ATIV1517'],
                row['T_ATIV1824'],
                row['T_ATIV18M'],
                row['T_ATIV2529'],
                row['T_DES'],
                row['T_DES1014'],
                row['T_DES1517'],
                row['T_DES1824'],
                row['T_DES18M'],
                row['T_DES2529'],
                row['THEILtrab'],
                row['TRABCC'],
                row['TRABPUB'],
                row['TRABSC'],
                row['T_AGUA'],
                row['T_BANAGUA'],
                row['T_DENS'],
                row['T_LIXO'],
                row['T_LUZ'],
                row['AGUA_ESGOTO'],
                row['PAREDE'],
                row['T_CRIFUNDIN_TODOS'],
                row['T_FORA4A5'],
                row['T_FORA6A14'],
                row['T_FUNDIN_TODOS'],
                row['T_FUNDIN_TODOS_MMEIO'],
                row['T_FUNDIN18MINF'],
                row['T_M10A14CF'],
                row['T_M15A17CF'],
                row['T_MULCHEFEFIF014'],
                row['T_NESTUDA_NTRAB_MMEIO'],
                row['T_OCUPDESLOC_1'],
                row['T_RMAXIDOSO'],
                row['T_SLUZ'],
                row['HOMEM0A4'],
                row['HOMEM10A14'],
                row['HOMEM15A19'],
                row['HOMEM20A24'],
                row['HOMEM25A29'],
                row['HOMEM30A34'],
                row['HOMEM35A39'],
                row['HOMEM40A44'],
                row['HOMEM45A49'],
                row['HOMEM50A54'],
                row['HOMEM55A59'],
                row['HOMEM5A9'],
                row['HOMEM60A64'],
                row['HOMEM65A69'],
                row['HOMEM70A74'],
                row['HOMEM75A79'],
                row['HOMEMTOT'],
                row['HOMENS80'],
                row['MULH0A4'],
                row['MULH10A14'],
                row['MULH15A19'],
                row['MULH20A24'],
                row['MULH25A29'],
                row['MULH30A34'],
                row['MULH35A39'],
                row['MULH40A44'],
                row['MULH45A49'],
                row['MULH50A54'],
                row['MULH55A59'],
                row['MULH5A9'],
                row['MULH60A64'],
                row['MULH65A69'],
                row['MULH70A74'],
                row['MULH75A79'],
                row['MULHER80'],
                row['MULHERTOT'],
                row['PEA'],
                row['PEA1014'],
                row['PEA1517'],
                row['PEA18M'],
                row['peso1'],
                row['PESO1114'],
                row['PESO1113'],
                row['PESO1214'],
                row['peso13'],
                row['PESO15'],
                row['peso1517'],
                row['PESO1524'],
                row['PESO1618'],
                row['PESO18'],
                row['Peso1820'],
                row['PESO1824'],
                row['Peso1921'],
                row['PESO25'],
                row['peso4'],
                row['peso5'],
                row['peso6'],
                row['PESO610'],
                row['Peso617'],
                row['PESO65'],
                row['PESOM1014'],
                row['PESOM1517'],
                row['PESOM15M'],
                row['PESOM25M'],
                row['pesoRUR'],
                row['pesotot'],
                row['pesourb'],
                row['PIA'],
                row['PIA1014'],
                row['PIA1517'],
                row['PIA18M'],
                row['POP'],
                row['POPT'],
                row['I_ESCOLARIDADE'],
                row['I_FREQ_PROP'],
                row['IDHM'],
                row['IDHM_E'],
                row['IDHM_L'],
                row['IDHM_R']
            )
            cursor.execute(sql, values)
        postgres_connection.commit()

    print(f'Dados do arquivo {source_filename} inseridos na tabela censo_completo_municipios com sucesso.')

finally:
    postgres_connection.close()
    
minio_client.close()
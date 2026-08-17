import os
import boto3
import pendulum
from airflow.sdk import dag, task
from airflow.sdk.exceptions import AirflowException
from databricks.sdk import WorkspaceClient

@dag(
    dag_id="dbt_build_models",
    description='Tratamento dos dados com dbt e gravação no MinIO',
    schedule=None,
    is_paused_upon_creation=False,
    start_date=pendulum.datetime(2026, 8, 1, tz="America/Sao_Paulo"),    
    tags=["transformacao", "dbt"],
)
def dbt_build_models():
    
    @task
    def calcular_ano_mes(data_interval_start=None) -> str:
        ano_mes = data_interval_start.in_timezone("America/Sao_Paulo").format("YYYY_MM")
        return ano_mes

    ano_mes = calcular_ano_mes()    

    @task.bash
    def dbt_build(ano_mes: str) -> str:
        return (
            f'export DBT_ANO_MES="{ano_mes}" && '
            f'export DBT_DB_PATH="/opt/airflow/duckdb/cnpj_data_lakehouse_{ano_mes}.duckdb" && '
            f'rm -f "$DBT_DB_PATH" && '
            f'cd /opt/airflow/dbt && '
            f'dbt build '            
            f'--vars \'{{"ano_mes": "{ano_mes}"}}\''
        )   
            
    @task
    def upload_minio_to_databricks_volume(schema: str,volumes: list, ano_mes: str):
        try:            
            endpoint = os.getenv("MINIO_ENDPOINT")
            user = os.getenv("MINIO_ROOT_USER")
            password = os.getenv("MINIO_ROOT_PASSWORD")
                    
            s3_client = boto3.client(
                's3',
                endpoint_url=endpoint,
                aws_access_key_id=user,
                aws_secret_access_key=password
            )

            DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
            DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")            
            DATABRICKS_CATALOG = "cnpj_data_lakehouse"
            DATABRICKS_SCHEMA = schema
                        
            w = WorkspaceClient(host=DATABRICKS_HOST, token=DATABRICKS_TOKEN)

        except Exception as e:
            raise AirflowException(f"Erro ao configurar os clientes: {e}")         

        BUCKET_PRINCIPAL = schema
        subpastas = volumes

        for pasta in subpastas:
            PREFIXO_CAMINHO = f"{pasta}/{ano_mes}/"
            print(f"--- A iniciar extração do MinIO: {PREFIXO_CAMINHO} ---")
                        
            paginator = s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=BUCKET_PRINCIPAL, Prefix=PREFIXO_CAMINHO)
            
            pasta_encontrada = False

            for page in pages:
                if 'Contents' not in page:
                    continue
                
                pasta_encontrada = True
                
                for file in page['Contents']:
                    file_key = file['Key']
                    
                    if file_key.endswith('_SUCCESS') or file_key.endswith('/'):
                        continue
                    
                    DATABRICKS_VOLUME = pasta
                    nome_arquivo_final = file_key.split('/')[-1]
                    caminho_final_databricks = f"/Volumes/{DATABRICKS_CATALOG}/{DATABRICKS_SCHEMA}/{DATABRICKS_VOLUME}/{ano_mes}/{nome_arquivo_final}"

                    print(f"📦 Iniciando Upload Streaming Direto via Databricks SDK: {nome_arquivo_final}")

                    minio_response = s3_client.get_object(Bucket=BUCKET_PRINCIPAL, Key=file_key)                                        
                    
                    with minio_response['Body'] as minio_stream:
                        w.files.upload(caminho_final_databricks, minio_stream, overwrite=True)

                    print(f"✅ Ficheiro {nome_arquivo_final} integrado com sucesso no Unity Catalog Volume!")

            if not pasta_encontrada:
                raise AirflowException(f"Erro: A pasta {PREFIXO_CAMINHO} não foi encontrada no MinIO ou está vazia.")

        print("🚀 Todos os ficheiros da camada Gold (incluindo os maiores de 5GB) foram sincronizados!")
    
    dbt_build(ano_mes) >> upload_minio_to_databricks_volume('bronze',['raw'],ano_mes) >> upload_minio_to_databricks_volume('silver',['raw','cleaned'],ano_mes) >> upload_minio_to_databricks_volume('gold',["bridge", "dim", "fact", "int", "agg"],ano_mes)

dag_execucao = dbt_build_models()
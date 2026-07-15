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
    start_date=pendulum.datetime(2026, 6, 1, tz="America/Sao_Paulo"),
    catchup=False,
    tags=["transformacao", "dbt"],
)
def dbt_build_models():

    ano_mes = pendulum.now("America/Sao_Paulo").format("YYYY_MM")

    @task.bash
    def dbt_build(ano_mes: str) -> str:      
        return f'cd /opt/airflow/dbt && dbt build --select staging marts intermediate --vars \'{{"ano_mes": "{ano_mes}"}}\''    
            
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
    
    dbt_build(ano_mes) >> upload_minio_to_databricks_volume('bronze',['raw'],ano_mes) >> upload_minio_to_databricks_volume('silver',['raw','cleaned'],ano_mes) >> upload_minio_to_databricks_volume('gold',["bridge", "dim", "fact", "int"],ano_mes)

dag_execucao = dbt_build_models()
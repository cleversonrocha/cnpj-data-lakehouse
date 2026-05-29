from airflow.sdk import dag, task
import pendulum
import requests
import zipfile
import os
import boto3

# Configurações do MinIO
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_USER = os.getenv("MINIO_ROOT_USER")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")
BUCKET_BRONZE = os.getenv("MINIO_BUCKET_BRONZE")

@dag(
    dag_id="ingestao_receita_federal_bronze_v1",
    schedule="@monthly",
    start_date=pendulum.datetime(2026, 5, 1, tz="America/Sao_Paulo"),
    catchup=False,
    tags=["ingestao", "receita_federal", "bronze", "otimizado"],
)
def rfb_datalake_ingestion():

    @task
    def gerar_link_dinamico() -> str:
        
        """Gera o link dinâmico baseado no mês atual (Maio de 2026)."""
        data_execucao = pendulum.now("America/Sao_Paulo")
        ano_mes_pasta = data_execucao.format("YYYY-MM")
        url_direta = f"https://arquivos.receitafederal.gov.br/public.php/dav/files/YggdBLfdninEJX9/{ano_mes_pasta}/?accept=zip"
        
        print(f"URL gerada para este mês: {url_direta}")
        return url_direta

    @task
    def baixar_e_enviar_minio(url: str):

        """Baixa em partes para o disco (se necessário), extrai e envia para o MinIO."""
        s3_client = boto3.client(
            's3', 
            endpoint_url=MINIO_ENDPOINT, 
            aws_access_key_id=MINIO_USER, 
            aws_secret_access_key=MINIO_PASSWORD
        )

        caminho_temporario = "/tmp/receita_dados_brutos.zip"

        # Verifica se o arquivo já está lá e não tem tamanho zero
        if os.path.exists(caminho_temporario) and os.path.getsize(caminho_temporario) > 1000000:
            print("Arquivo gigante já encontrado no disco temporário! Pulando o download da internet...")
        else:
            print("1. Iniciando download em CHUNKS para poupar RAM...")

            caminho_certificado = "/opt/airflow/certs/arquivos.receitafederal.gov.br.crt"
            
            with requests.get(url, stream=True, verify=caminho_certificado) as response:
                response.raise_for_status()
                with open(caminho_temporario, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8 * 1024 * 1024):
                        f.write(chunk)
                    
        print("2. Iniciando extração e envio Multipart para o MinIO...")
        with zipfile.ZipFile(caminho_temporario, 'r') as z_master:
            arquivos_internos = z_master.namelist()
            
            for nome_arquivo in arquivos_internos:
                # O MinIO não gosta de barras (/) invertidas ou duplas no nome. Garantimos um caminho limpo.
                nome_limpo = nome_arquivo.replace("\\", "/").split("/")[-1]
                
                if nome_limpo.endswith('.csv') or nome_limpo.endswith('.zip'):
                    caminho_s3 = f"dados_crus/referencia_{pendulum.now('America/Sao_Paulo').format('YYYY_MM')}/{nome_limpo}"
                    print(f" -> A enviar para o MinIO: {caminho_s3}")
                    
                    with z_master.open(nome_arquivo) as arquivo_extraido:
                        s3_client.upload_fileobj(
                            Fileobj=arquivo_extraido,
                            Bucket=BUCKET_BRONZE,
                            Key=caminho_s3
                        )
                        
        print("3. A limpar o disco temporário do Airflow...")
        os.remove(caminho_temporario)
        print("Carga da camada Bronze concluída com sucesso!")

    # Ordem de execução
    link = gerar_link_dinamico()
    baixar_e_enviar_minio(link)

# Instanciação da DAG
dag_execucao = rfb_datalake_ingestion()
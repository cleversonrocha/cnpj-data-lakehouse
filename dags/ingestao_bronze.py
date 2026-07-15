from airflow.sdk import dag, task
from airflow.sdk.exceptions import AirflowException
import pendulum
import requests
import zipfile
import os
import boto3
import logging

# 1. Configura o logger para a DAG/Task
logger = logging.getLogger("airflow.task")

# Configurações do MinIO
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_USER = os.getenv("MINIO_ROOT_USER")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")
MINIO_BUCKET_BRONZE = 'bronze'

@dag(
    dag_id="ingestao_bronze",
    schedule="@monthly",
    start_date=pendulum.datetime(2026, 6, 1, tz="America/Sao_Paulo"),
    catchup=False,
    tags=["ingestao", "bronze"],
)
def rfb_datalake_ingestion():       

    @task
    def gerar_link_dinamico() -> str:

        url_teste = "https://arquivos.receitafederal.gov.br/public.php/dav/files/YggdBLfdninEJX9/"                
        caminho_certificado = "/opt/airflow/certs/arquivos.receitafederal.gov.br.crt"
        
        print(f"Iniciando teste de conexão estrita...")
                
        try:
            #requests.head apenas testa a ligação e as credenciais de segurança, sem baixar dados
            response = requests.head(url_teste, verify=caminho_certificado, timeout=15)                       
            logger.info("-> O Airflow conseguiu validar a identidade do servidor do governo.")
            logger.info(f"-> Código de Resposta HTTP: {response.status_code}")                        
            
        except requests.exceptions.SSLError as ssl_err:
            logger.error("ERRO DE VALIDAÇÃO SSL!")
            logger.error(f"Detalhes técnicos: {ssl_err}")                        
            raise AirflowException(f"Erro crítico durante o download: {ssl_err}")                
            
        except Exception as e:            
            raise AirflowException(f"Ocorreu um erro inesperado de rede: {e}")
        
        """Gera o link dinâmico baseado no mês atual."""
        data_execucao = pendulum.now("America/Sao_Paulo")
        ano_mes_pasta = data_execucao.format("YYYY-MM")
        url_direta = f"https://arquivos.receitafederal.gov.br/public.php/dav/files/YggdBLfdninEJX9/{ano_mes_pasta}/?accept=zip"                       
        
        logger.info(f"URL gerada para este mês: {url_direta}")
        return url_direta

    @task
    def baixar_e_enviar_minio(url: str):     
        
        caminho_temporario = "/tmp/receita_dados_brutos.zip"               
        
        if not os.path.exists(caminho_temporario):            

            logger.info("1. Iniciando download em CHUNKS para poupar RAM...")
            caminho_certified = "/opt/airflow/certs/arquivos.receitafederal.gov.br.crt"

            try:
                with requests.get(
                    url, stream=True, verify=caminho_certified
                ) as response:
                    response.raise_for_status()

                    tamanho_total = int(response.headers.get("content-length", 0))
                    bytes_baixados = 0
                    proximo_log_mb = 500  # Meta inicial de log

                    with open(caminho_temporario, "wb") as f:
                        for chunk in response.iter_content(chunk_size=100 * 1024 * 1024):
                            if chunk:
                                f.write(chunk)
                                bytes_baixados += len(chunk)

                                mb_baixados = bytes_baixados / (1024 * 1024)

                                # Verifica se atingiu os próximos 500MB para registrar o log
                                if mb_baixados >= proximo_log_mb:
                                    if tamanho_total > 0:
                                        percentual = (bytes_baixados / tamanho_total) * 100
                                        logger.info(
                                            f"Progresso: {percentual:.2f}% ({mb_baixados:.0f} MB recebidos)"
                                        )
                                    else:
                                        logger.info(
                                            f"Baixando... {mb_baixados:.0f} MB recebidos"
                                        )

                                    proximo_log_mb += 500  # Define a próxima meta

                    logger.info(
                        f"Download concluído com sucesso! Total: {bytes_baixados / (1024 * 1024):.2f} MB"
                    )

            except requests.exceptions.RequestException as e:
                raise AirflowException(f"Erro crítico durante o download: {e}")                

        else:
            logger.info('Arquivo já existe!')                    

        logger.info("2. Iniciando extração e envio Multipart para o MinIO...")
        logger.info(f"Iniciando a verificação de integridade de {caminho_temporario}...")
        logger.info("Isso pode demorar alguns minutos devido ao tamanho (7 GB)...")      
        try:

            s3_client = boto3.client(
                's3', 
                endpoint_url=MINIO_ENDPOINT, 
                aws_access_key_id=MINIO_USER, 
                aws_secret_access_key=MINIO_PASSWORD
            )

            with zipfile.ZipFile(caminho_temporario, 'r') as z_master:                
                
                arquivo_corrompido = z_master.testzip()
                if arquivo_corrompido is None:

                    arquivos_internos = z_master.namelist()
                    for nome_arquivo in arquivos_internos:
                        # O MinIO não gosta de barras (/) invertidas ou duplas no nome. Garantimos um caminho limpo.
                        nome_limpo = nome_arquivo.replace("\\", "/").split("/")[-1]
                        
                        if nome_limpo.endswith('.csv') or nome_limpo.endswith('.zip'):
                            caminho_s3 = f"raw/{pendulum.now('America/Sao_Paulo').format('YYYY_MM')}/{nome_limpo}"
                            logger.info(f" -> A enviar para o MinIO: {caminho_s3}")
                            
                            with z_master.open(nome_arquivo) as arquivo_extraido:
                                s3_client.upload_fileobj(
                                    Fileobj=arquivo_extraido,
                                    Bucket=MINIO_BUCKET_BRONZE,
                                    Key=caminho_s3
                                )
                else:                    
                    logger.info(f"Erro: O arquivo está incompleto. Primeiro arquivo corrompido encontrado: {arquivo_corrompido}")

            logger.info("3. A limpar o disco temporário do Airflow...")
            os.remove(caminho_temporario)
            logger.info("Carga da camada Bronze concluída com sucesso!")
        
        except zipfile.BadZipFile:
            raise AirflowException("Erro: O arquivo não é um ZIP válido ou está severamente corrompido (incompleto).")

        except FileNotFoundError:
            raise AirflowException(f"Erro: O arquivo '{caminho_temporario}' não foi encontrado.")

        except PermissionError:
            raise AirflowException("Erro: Permissão negada. O arquivo ainda pode estar sendo usado por outro programa.")

        except OSError as e:
            raise AirflowException(f"Erro de sistema/disco ao ler o arquivo: {e}")

        except Exception as e:
            raise AirflowException(f"Ocorreu um erro inesperado: {MINIO_ENDPOINT}")                        

    # Ordem de execução
    link = gerar_link_dinamico()
    baixar_e_enviar_minio(link)
    
# Instanciação da DAG
dag_execucao = rfb_datalake_ingestion()
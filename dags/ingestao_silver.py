from airflow.sdk import dag, task
from airflow.sdk.exceptions import AirflowException
import pendulum
import os
import boto3
import duckdb
import logging

logger = logging.getLogger("airflow.task")

@dag(
    dag_id="ingestao_silver",
    description='Extração dos arquivos csv compactados e conversão para parquet',
    schedule=None,
    start_date=pendulum.datetime(2026, 6, 1, tz="America/Sao_Paulo"),
    catchup=False,
    tags=["ingestao", "silver"],
)    

def datalake_silver():       
    
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
    except Exception as e:
        raise AirflowException(f"Erro ao configurar o cliente S3: {e}")
                
    ano_mes = pendulum.now("America/Sao_Paulo").format("YYYY_MM")     

    @task
    def processar_entidades_unicas(tabela_nome: str, arquivo_zip: str):
        logger.info(f"Iniciando processamento da tabela de forma direta: {tabela_nome.upper()}")        
        
        try:            
            con = duckdb.connect(database=':memory:')
            con.execute("INSTALL zipfs FROM community; LOAD zipfs;")
            con.execute("INSTALL httpfs; LOAD httpfs;")        
            con.execute(f"""
                SET s3_endpoint='{endpoint.removeprefix('http://')}';
                SET s3_access_key_id='{user}';
                SET s3_secret_access_key='{password}';
                SET s3_use_ssl=false;
                SET s3_url_style='path';
            """)
                    
            # O DuckDB acessa o ZIP no S3 e busca o CSV correspondente lá dentro de forma virtual            
            caminho_bronze_zip = f"zip://s3://bronze/raw/{ano_mes}/{arquivo_zip}"
            caminho_silver = f"s3://silver/raw/{tabela_nome}.parquet"
            
            logger.info(f"Lendo diretamente do MinIO: {caminho_bronze_zip}")
            con.execute(f"""
                COPY (
                    SELECT *
                    FROM read_csv_auto(
                        '{caminho_bronze_zip}', 
                        sep=';', 
                        header=false,
                        encoding='latin-1',
                        all_varchar=true,
                        ignore_errors=false                                                
                    )
                ) TO '{caminho_silver}' (FORMAT PARQUET);
            """)            
            
            logger.info(f"Tabela {tabela_nome.upper()} finalizada 100% em memória: {caminho_silver}")
    
        except Exception as e:
            raise AirflowException(f"Erro ao processar {tabela_nome.upper()}: {e}")                        

    tarefa_anterior = None

    tabelas_para_processar = [
        {"nome": "motivos", "zip": "Motivos.zip"},
        {"nome": "qualificacoes", "zip": "Qualificacoes.zip"},
        {"nome": "naturezas", "zip": "Naturezas.zip"},
        {"nome": "paises", "zip": "Paises.zip"},
        {"nome": "cnaes", "zip": "Cnaes.zip"},
        {"nome": "municipios", "zip": "Municipios.zip"}      
    ]     

    # Loop Gerador de Tarefas       
    for tabela in tabelas_para_processar:
        # Usamos o .override() para dar um nome visual único a cada bloco no Airflow
        tarefa_atual = processar_entidades_unicas.override(task_id=f"processar_{tabela['nome']}")(
            tabela_nome=tabela['nome'],             
            arquivo_zip=tabela['zip']
        )

        # Se já existir uma tarefa anterior, forçamos a atual a esperar por ela
        if tarefa_anterior:
            tarefa_anterior >> tarefa_atual
            
        # A tarefa atual passa a ser a "anterior" para a próxima volta do loop
        tarefa_anterior = tarefa_atual
    
    @task
    def processar_entidades_particionadas(entidade_nome: str, prefixo_arquivo: str):
        logger.info(f"Iniciando processamento massivo e direto da entidade: {entidade_nome.upper()}")        
            
        try:
            con = duckdb.connect(database=':memory:')                   
            con.execute("INSTALL httpfs; LOAD httpfs;")        
            con.execute("INSTALL zipfs FROM community; LOAD zipfs;")
            con.execute(f"""
                SET s3_endpoint='{endpoint.removeprefix('http://')}';
                SET s3_access_key_id='{user}';
                SET s3_secret_access_key='{password}';
                SET s3_use_ssl=false;
                SET s3_url_style='path';
            """)
            
            caminho_silver = f"s3://silver/raw/{entidade_nome}.parquet"                       
            caminho_busca_zip = f"zip://s3://bronze/raw/{ano_mes}/{prefixo_arquivo}*.zip"
            
            logger.info(f"Vetorizando leitura dos ZIPs e gravando Parquet: {caminho_busca_zip}")
            
            con.execute(f"""
                COPY (
                    SELECT *
                    FROM read_csv_auto(
                        '{caminho_busca_zip}', 
                        sep=';', 
                        header=False,  
                        encoding='utf-8',
                        all_varchar=true,                    
                        ignore_errors=true
                    )
                ) TO '{caminho_silver}' (FORMAT PARQUET);
            """)        
            
            logger.info(f"Entidade {entidade_nome.upper()} unificada e salva com sucesso em: {caminho_silver}")

        except Exception as e:
            raise AirflowException(f"Erro no processamento unificado de {entidade_nome.upper()}: {e}")

    # Configuração das 3 grandes entidades do projeto
    # Estabelecimentos por último porque é o mais pesado.
    grandes_entidades = [
        {"nome": "socios", "prefixo": "Socios"},
        {"nome": "empresas", "prefixo": "Empresas"},
        {"nome": "estabelecimentos", "prefixo": "Estabelecimentos"}
    ]
    
    for entidade in grandes_entidades:
        # Instancia a tarefa atual
        tarefa_atual = processar_entidades_particionadas.override(task_id=f"unificar_{entidade['nome']}")(
            entidade_nome=entidade['nome'],
            prefixo_arquivo=entidade['prefixo']
        )
        
        # Se já existir uma tarefa anterior, forçamos a atual a esperar por ela
        if tarefa_anterior:
            tarefa_anterior >> tarefa_atual
            
        # A tarefa atual passa a ser a "anterior" para a próxima volta do loop
        tarefa_anterior = tarefa_atual

dag_execucao = datalake_silver()
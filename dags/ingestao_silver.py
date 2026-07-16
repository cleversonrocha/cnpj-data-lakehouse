from airflow.sdk import dag, task
from airflow.sdk.exceptions import AirflowException
import pendulum
import os
import boto3
import zipfile
import duckdb
import glob
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
    os.environ["DBT_ANO_MES"] = ano_mes

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
            caminho_silver = f"s3://silver/raw/{ano_mes}/{tabela_nome}.parquet"
            
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
        {"nome": "municipios", "zip": "Municipios.zip"},
        {"nome": "simples", "zip": "Simples.zip"}
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
        logger.info(f"Iniciando processamento massivo da entidade: {entidade_nome.upper()}")        
               
        pasta_temp = f"/opt/airflow/temp/{entidade_nome}"
        os.makedirs(pasta_temp, exist_ok=True)
        
        # Loop para baixar e extrair os 10 pedaços (0 a 9)          
        for i in range(10):        
            nome_zip = f"{prefixo_arquivo}{i}.zip"
            key_bronze = f"raw/{ano_mes}/{nome_zip}"
            caminho_zip_local = os.path.join(pasta_temp, nome_zip)
            
            logger.info(f"[{i+1}/10] Baixando {nome_zip}...")
            try:
                s3_client.download_file('bronze', key_bronze, caminho_zip_local)
                
                logger.info(f"[Operação Salva-Linha] Higienizando bytes da parte {i}...")
                with zipfile.ZipFile(caminho_zip_local, 'r') as z:
                    nome_interno = z.namelist()[0]
                    caminho_csv_local = os.path.join(pasta_temp, f"part_{i}_{nome_interno}")
                                        
                    with z.open(nome_interno) as f_in, open(caminho_csv_local, 'w', encoding='utf-8') as f_out:
                        for linha in f_in:
                            # 1. Remove os Null Bytes
                            linha_limpa = linha.replace(b'\x00', b'')                            
                            linha_final = linha_limpa.decode('latin-1', errors='replace')
                            
                            # 3. Grava a String no arquivo de texto
                            f_out.write(linha_final)                        
                                    
                    os.remove(caminho_zip_local)
                
            except Exception as e:
                logger.info(f"Aviso: Falha ao processar a parte {i}. Erro: {e}")
                continue
        
        logger.info("🦆 Iniciando unificação vetorizada com DuckDB...")
        
        pasta_duckdb_tmp = "/opt/airflow/temp/duckdb_cache"
        os.makedirs(pasta_duckdb_tmp, exist_ok=True)
        
        con = duckdb.connect(database=':memory:')
        con.execute(f"PRAGMA temp_directory='{pasta_duckdb_tmp}';")               
        con.execute("PRAGMA memory_limit='12GB';") 
        con.execute("INSTALL httpfs; LOAD httpfs;")        
        
        con.execute(f"""
            SET s3_endpoint='{endpoint.removeprefix('http://')}';
            SET s3_access_key_id='{user}';
            SET s3_secret_access_key='{password}';
            SET s3_use_ssl=false;
            SET s3_url_style='path';
        """)
        
        caminho_silver = f"s3://silver/raw/{ano_mes}/{entidade_nome}.parquet"
        # O asterisco engloba todos os arquivos extraídos na pasta temporária
        caminho_busca_csv = os.path.join(pasta_temp, "part_*")
        
        logger.info(f"Analisando, tipando e unificando partes em: {caminho_busca_csv}")
        con.execute(f"""
            COPY (
                SELECT *
                FROM read_csv_auto(
                    '{caminho_busca_csv}', 
                    sep=';', 
                    header=False,  
                    encoding='utf-8',                  
                    all_varchar=true,                    
                    ignore_errors=false
                )
            ) TO '{caminho_silver}' (FORMAT PARQUET);
        """)        

        logger.info("Limpeza profunda do disco temporário...")
        arquivos_para_limpar = glob.glob(os.path.join(pasta_temp, "*"))
        for f in arquivos_para_limpar:
            os.remove(f)
        os.rmdir(pasta_temp)        
        
        logger.info(f"Entidade {entidade_nome.upper()} unificada com sucesso em Parquet!")

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
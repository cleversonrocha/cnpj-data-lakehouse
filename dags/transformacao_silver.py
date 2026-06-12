from airflow.sdk import dag, task
import pendulum
import os
import boto3
import zipfile
import duckdb
import glob
import logging

logger = logging.getLogger("airflow.task")

@dag(
    dag_id="transformacao_silver",
    description='Extração dos arquivos csv compactados e conversão para parquet',
    schedule=None,
    start_date=pendulum.datetime(2026, 5, 1, tz="America/Sao_Paulo"),
    catchup=False,
    tags=["transformacao", "silver"],
)    

def datalake_silver():
        
    try:            
        endpoint = os.getenv("MINIO_ENDPOINT")
        user = os.getenv("MINIO_ROOT_USER")
        password = os.getenv("MINIO_ROOT_PASSWORD")
        bucket_bronze = os.getenv("MINIO_BUCKET_BRONZE") or "bronze"
        
        s3_client = boto3.client(
            's3',
            endpoint_url=endpoint,
            aws_access_key_id=user,
            aws_secret_access_key=password
        )
    except Exception as e:
        logger.error(f"Erro ao configurar o cliente S3: {e}")
        raise
    
    ano_mes = "2026_05"
    #ano_mes = pendulum.now("America/Sao_Paulo").format("YYYY_MM")     

    @task
    def processar_entidades_unicas(tabela_nome: str, arquivo_zip: str, tabela_colunas_originais: list):
        logger.info(f"Iniciando processamento da tabela: {tabela_nome.upper()}")        
        
        try:            
            key_bronze = f"dados_crus/referencia_{ano_mes}/{arquivo_zip}"
                    
            caminho_zip_local = f"/tmp/temp_{arquivo_zip}"
            caminho_csv_extraido = f"/tmp/extraido_{tabela_nome}.csv"
            
            logger.info(f"Baixando {arquivo_zip} da Bronze...")
            s3_client.download_file(bucket_bronze, key_bronze, caminho_zip_local)
            
            logger.info("Extraindo arquivo...")
            with zipfile.ZipFile(caminho_zip_local, 'r') as z:
                nome_arquivo_interno = z.namelist()[0]
                with z.open(nome_arquivo_interno) as f_in, open(caminho_csv_extraido, 'wb') as f_out:
                    f_out.write(f_in.read())
                    
            logger.info("Conectando ao DuckDB e convertendo para Parquet...")
            con = duckdb.connect(database=':memory:')
            con.execute("INSTALL httpfs; LOAD httpfs;")        
            
            con.execute(f"""
                SET s3_endpoint='{endpoint.removeprefix('http://')}';
                SET s3_access_key_id='{user}';
                SET s3_secret_access_key='{password}';
                SET s3_use_ssl=false;
                SET s3_url_style='path';
            """)
                    
            caminho_silver = f"s3://silver/{tabela_nome}/{tabela_nome}.parquet"
            
            con.execute(f"""
                COPY (
                    SELECT {', '.join(tabela_colunas_originais)}
                    FROM read_csv_auto(
                        '{caminho_csv_extraido}', 
                        sep=';', 
                        header=false,
                        encoding='latin-1',
                        all_varchar=true,
                        ignore_errors=false                                                
                    )
                ) TO '{caminho_silver}' (FORMAT PARQUET);
            """)            
            
            logger.info("🧹 Limpando o disco...")
            os.remove(caminho_zip_local)
            os.remove(caminho_csv_extraido)
            
            logger.info(f"Tabela {tabela_nome.upper()} finalizada: {caminho_silver}")
        
        except Exception as e:
            logger.error(f"Erro ao processar {tabela_nome.upper()}: {e}")            
            raise

    tarefa_anterior = None

    tabelas_para_processar = [
        {"nome": "motivos", "zip": "Motivos.zip", "colunas_originais": ["column0 AS codigo", "column1 AS descricao"]},
        {"nome": "qualificacoes", "zip": "Qualificacoes.zip", "colunas_originais": ["column0 AS codigo", "column1 AS descricao"]},
        {"nome": "naturezas", "zip": "Naturezas.zip", "colunas_originais": ["column0 AS codigo", "column1 AS descricao"]},
        {"nome": "paises", "zip": "Paises.zip", "colunas_originais": ["column0 AS codigo", "column1 AS descricao"]},
        {"nome": "cnaes", "zip": "Cnaes.zip", "colunas_originais": ["column0 AS codigo", "column1 AS descricao"]},
        {"nome": "municipios", "zip": "Municipios.zip", "colunas_originais": ["column0 AS codigo", "column1 AS descricao"]}      
    ]     

    # Loop Gerador de Tarefas       
    for tabela in tabelas_para_processar:
        # Usamos o .override() para dar um nome visual único a cada bloco no Airflow
        tarefa_atual = processar_entidades_unicas.override(task_id=f"processar_{tabela['nome']}")(
            tabela_nome=tabela['nome'],             
            arquivo_zip=tabela['zip'],
            tabela_colunas_originais=tabela['colunas_originais']            
        )

        # Se já existir uma tarefa anterior, forçamos a atual a esperar por ela
        if tarefa_anterior:
            tarefa_anterior >> tarefa_atual
            
        # A tarefa atual passa a ser a "anterior" para a próxima volta do loop
        tarefa_anterior = tarefa_atual
    
    @task
    def processar_entidades_particionadas(entidade_nome: str, prefixo_arquivo: str, tabela_colunas_originais: list):
        logger.info(f"Iniciando processamento massivo da entidade: {entidade_nome.upper()}")        
               
        pasta_temp = f"/opt/airflow/temp/{entidade_nome}"
        os.makedirs(pasta_temp, exist_ok=True)
        
        # Loop para baixar e extrair os 10 pedaços (0 a 9)          
        for i in range(10):        
            nome_zip = f"{prefixo_arquivo}{i}.zip"
            key_bronze = f"dados_crus/referencia_{ano_mes}/{nome_zip}"
            caminho_zip_local = os.path.join(pasta_temp, nome_zip)
            
            logger.info(f"[{i+1}/10] Baixando {nome_zip}...")
            try:
                s3_client.download_file(bucket_bronze, key_bronze, caminho_zip_local)
                
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
        con.execute("PRAGMA memory_limit='10GB';") 
        con.execute("INSTALL httpfs; LOAD httpfs;")        
        
        con.execute(f"""
            SET s3_endpoint='{endpoint.removeprefix('http://')}';
            SET s3_access_key_id='{user}';
            SET s3_secret_access_key='{password}';
            SET s3_use_ssl=false;
            SET s3_url_style='path';
        """)
        
        caminho_silver = f"s3://silver/{entidade_nome}/{entidade_nome}.parquet"
        # O asterisco engloba todos os arquivos extraídos na pasta temporária
        caminho_busca_csv = os.path.join(pasta_temp, "part_*")
        
        logger.info(f"Analisando, tipando e unificando partes em: {caminho_busca_csv}")
        con.execute(f"""
            COPY (
                SELECT {', '.join(tabela_colunas_originais)}
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
        {"nome": "socios", "prefixo": "Socios", "colunas_originais": ["column00 AS cnpj_basico",
                                                                      "column01 AS identificador", 
                                                                      "column02 AS nome_razao_social", 
                                                                      "column03 AS cpf_cnpj",                                                                       
                                                                      "column04 AS qualificacao", 
                                                                      "column05 AS data_entrada_sociedade", 
                                                                      "column06 AS pais", 
                                                                      "column07 AS representante_legal", 
                                                                      "column08 AS nome_do_representante", 
                                                                      "column09 AS qualificacao_representante",                                                                       
                                                                      "column10 AS faixa_etaria_socio"]},
        {"nome": "empresas", "prefixo": "Empresas", "colunas_originais": ["column0 AS cnpj_basico", 
                                                                          "column1 AS razao_social", 
                                                                          "column2 AS natureza_juridica",
                                                                          "column3 AS qualificacao_responsavel",
                                                                          "column4 AS capital_social",
                                                                          "column5 AS porte_empresa",
                                                                          "column6 AS ente_federativo_responsavel"]},
        {"nome": "estabelecimentos", "prefixo": "Estabelecimentos", "colunas_originais": ["column00 AS cnpj_basico",
                                                                                           "column01 AS cnpj_ordem",
                                                                                           "column02 AS cnpj_dv",
                                                                                           "column03 AS identificador_matriz_filial",
                                                                                           "column04 AS nome_fantasia",
                                                                                           "column05 AS situacao_cadastral",
                                                                                           "column06 AS data_situacao_cadastral",
                                                                                           "column07 AS motivo_situacao_cadastral",
                                                                                           "column08 AS nome_cidade_exterior",
                                                                                           "column09 AS pais",
                                                                                           "column10 AS data_inicio_atividade",
                                                                                           "column11 AS cnae_fiscal_principal",
                                                                                           "column12 AS cnae_fiscal_secundaria",
                                                                                           "column13 AS tipo_logradouro",
                                                                                           "column14 AS logradouro",
                                                                                           "column15 AS numero",
                                                                                           "column16 AS complemento",
                                                                                           "column17 AS bairro",
                                                                                           "column18 AS cep",
                                                                                           "column19 AS uf",
                                                                                           "column20 AS municipio",
                                                                                           "column21 AS ddd_1",
                                                                                           "column22 AS telefone_1",
                                                                                           "column23 AS ddd_2",
                                                                                           "column24 AS telefone_2",
                                                                                           "column25 AS ddd_fax",
                                                                                           "column26 AS fax",
                                                                                           "column27 AS email",
                                                                                           "column28 AS situacao_especial",
                                                                                           "column29 AS data_situacao_especial"]}
    ]

    for entidade in grandes_entidades:
        # Instancia a tarefa atual
        tarefa_atual = processar_entidades_particionadas.override(task_id=f"unificar_{entidade['nome']}")(
            entidade_nome=entidade['nome'],
            prefixo_arquivo=entidade['prefixo'],
            tabela_colunas_originais=entidade['colunas_originais']
        )
        
        # Se já existir uma tarefa anterior, forçamos a atual a esperar por ela
        if tarefa_anterior:
            tarefa_anterior >> tarefa_atual
            
        # A tarefa atual passa a ser a "anterior" para a próxima volta do loop
        tarefa_anterior = tarefa_atual

dag_execucao = datalake_silver()
from airflow.sdk import dag, task
import pendulum
import duckdb
import boto3
import zipfile
import os
import logging

# 1. Configura o logger para a DAG/Task
logger = logging.getLogger("airflow.task")

@dag(
    dag_id='auditoria_bronze_para_sillver',
    description='Verifica a quantidade de registros das camadas bronze(zip) e silver(parquet)',
    start_date=pendulum.datetime(2026, 5, 1, tz="America/Sao_Paulo"),
    schedule=None,
    catchup=False,
    tags=['auditoria', 'data lakehouse']
)
@task
def auditoria_ingestao_bronze_para_sillver():
    # ==========================================
    # ⚙️ CONFIGURAÇÕES
    # ==========================================
    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
    MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
    MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")
    BUCKET_BRONZE = 'bronze'
    PREFIXO_BRONZE = 'dados_crus/referencia_2026_05/' # Ajuste para a sua pasta na bronze
    PASTA_TEMP = 'D:/airflow_temp/auditoria'

    # Lista com todas as entidades a serem auditadas
    ENTIDADES = ['Motivos', 'Qualificacoes', 'Naturezas', 'Paises', 'Cnaes', 'Municipios', 'Socios', 'Empresas', 'Estabelecimentos']
    
    os.makedirs(PASTA_TEMP, exist_ok=True)

    # ==========================================
    # 🦆 INICIALIZAÇÃO DUCKDB E BOTO3
    # ==========================================
    logger.info("⚙️ Configurando conexões...")
    con = duckdb.connect()
    con.execute(f"""
        INSTALL httpfs; LOAD httpfs;
        SET s3_endpoint='{MINIO_ENDPOINT.removeprefix('http://')}';
        SET s3_access_key_id='{MINIO_ACCESS_KEY}';
        SET s3_secret_access_key='{MINIO_SECRET_KEY}';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
    """)

    s3_client = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )

    # Busca todos os objetos da Bronze de uma vez para otimizar
    objetos = s3_client.list_objects_v2(Bucket=BUCKET_BRONZE, Prefix=PREFIXO_BRONZE)
    todos_zips_bronze = [
        obj['Key'] for obj in objetos.get('Contents', []) 
        if obj['Key'].lower().endswith('.zip')
    ]

    # Lista para armazenar o resumo final
    resultados_auditoria = []

    # ==========================================
    # 🔄 LOOP DE AUDITORIA POR ENTIDADE
    # ==========================================
    for entidade in ENTIDADES:
        logger.info(f"\n{'='*50}")
        logger.info(f"🔍 AUDITANDO: {entidade.upper()}")
        logger.info(f"{'='*50}")
        
        nome_entidade_min = entidade.lower()
        
        # ------------------------------------------
        # 1. CONTAGEM DO DESTINO (PARQUET)
        # ------------------------------------------
        # Ajuste este caminho se a estrutura de pastas da Silver for diferente
        caminho_parquet = f"s3://silver/{nome_entidade_min}/{nome_entidade_min}.parquet"
        
        try:
            logger.info(f"🦆 Lendo Parquet: {caminho_parquet}")
            total_parquet = con.execute(f"SELECT COUNT(*) FROM '{caminho_parquet}'").fetchone()[0]
            logger.info(f"✅ Total no Parquet: {total_parquet:,} registros")
        except Exception as e:
            logger.info(f"❌ Erro ao ler Parquet de {entidade}: {e}")
            total_parquet = 0

        # ------------------------------------------
        # 2. CONTAGEM DA ORIGEM (ZIPS NA BRONZE)
        # ------------------------------------------
        zips_para_contar = [
            key for key in todos_zips_bronze 
            if nome_entidade_min in key.split('/')[-1].lower()
        ]
        
        total_origem = 0
        logger.info("📦 Contando arquivos ZIP originais...")
        
        if not zips_para_contar:
            logger.info("⚠️ Nenhum arquivo ZIP encontrado para esta entidade.")
        
        for key in zips_para_contar:
            nome_arquivo = key.split('/')[-1]
            caminho_local = os.path.join(PASTA_TEMP, nome_arquivo)
            
            logger.info(f"   📥 Baixando {nome_arquivo}...")
            s3_client.download_file(BUCKET_BRONZE, key, caminho_local)
            
            linhas_neste_zip = 0
            with zipfile.ZipFile(caminho_local, 'r') as z:
                nome_interno = z.namelist()[0]
                with z.open(nome_interno, 'r') as f:
                    for bloco in iter(lambda: f.read(10 * 1024 * 1024), b''):
                        linhas_neste_zip += bloco.count(b'\n')
                        
            logger.info(f"      -> {linhas_neste_zip:,} linhas")
            total_origem += linhas_neste_zip
            
            # Apaga o ficheiro
            os.remove(caminho_local)
            
        logger.info(f"✅ Total nos ZIPs: {total_origem:,} registros")
        
        # ------------------------------------------
        # 3. REGISTRA RESULTADO
        # ------------------------------------------
        diferenca = total_origem - total_parquet
        status = "✅ OK" if diferenca == 0 else "⚠️ DIVERGÊNCIA"
        
        resultados_auditoria.append({
            'Entidade': entidade,
            'Origem': total_origem,
            'Destino': total_parquet,
            'Diferenca': diferenca,
            'Status': status
        })

    # ==========================================
    # ⚖️ O VEREDITO DA AUDITORIA (RELATÓRIO FINAL)
    # ==========================================
    logger.info("\n" + "="*85)
    logger.info("⚖️ RELATÓRIO DE CONCILIAÇÃO FINAL")
    logger.info("="*85)
    logger.info(f"{'ENTIDADE':<18} | {'ORIGEM (ZIP)':<15} | {'DESTINO (PQ)':<15} | {'DIFERENÇA':<12} | {'STATUS'}")
    logger.info("-" * 85)

    for r in resultados_auditoria:
        logger.info(f"{r['Entidade']:<18} | {r['Origem']:<15,} | {r['Destino']:<15,} | {r['Diferenca']:<12,} | {r['Status']}")

    logger.info("="*85 + "\n")

dag_execucao = auditoria_ingestao_bronze_para_sillver()
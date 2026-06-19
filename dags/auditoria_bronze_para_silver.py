from airflow.sdk import dag, task
import pendulum
import duckdb
import os
import logging

# 1. Configura o logger para a DAG/Task
logger = logging.getLogger("airflow.task")

@dag(
    dag_id='auditoria_bronze_para_silver',
    description='Verifica a quantidade de registros das camadas bronze(zip) e silver(parquet)',
    start_date=pendulum.datetime(2026, 5, 1, tz="America/Sao_Paulo"),
    schedule=None,
    catchup=False,
    tags=['auditoria', 'data lakehouse']
)
@task
def auditoria_ingestao_bronze_para_silver():
    # ==========================================
    # ⚙️ CONFIGURAÇÕES
    # ==========================================
    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
    MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
    MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")
        
    # Lista com todas as entidades a serem auditadas
    ENTIDADES = ['Motivos', 'Qualificacoes', 'Naturezas', 'Paises', 'Cnaes', 'Municipios', 'Socios', 'Empresas', 'Estabelecimentos']

    ano_mes = pendulum.now("America/Sao_Paulo").format("YYYY_MM")     
        
    # ==========================================
    # 🦆 INICIALIZAÇÃO DUCKDB
    # ==========================================
    logger.info("⚙️ Configurando conexões...")
    con = duckdb.connect(database=':memory:')
    con.execute("INSTALL zipfs FROM community; LOAD zipfs;")
    con.execute(f"""        
        INSTALL httpfs; LOAD httpfs;
        SET s3_endpoint='{MINIO_ENDPOINT.removeprefix('http://')}';
        SET s3_access_key_id='{MINIO_ACCESS_KEY}';
        SET s3_secret_access_key='{MINIO_SECRET_KEY}';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
    """)   

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
        caminho_parquet = f"s3://silver/raw/{ano_mes}/{nome_entidade_min}.parquet"
        
        try:
            logger.info(f"🦆 Lendo Parquet: {caminho_parquet}")
            total_parquet = con.execute(f"SELECT COUNT(*) FROM '{caminho_parquet}'").fetchone()[0]
            total_parquet_f = f"{total_parquet:,}".replace(",", ".")
            logger.info(f"✅ Total no Parquet: {total_parquet_f} registros")
        except Exception as e:
            logger.info(f"❌ Erro ao ler Parquet de {entidade}: {e}")
            total_parquet = 0

        # ------------------------------------------
        # 2. CONTAGEM DA ORIGEM (ZIPS NA BRONZE)
        # ------------------------------------------                
        logger.info("📦 Contando arquivos ZIP originais...")                 
        logger.info(f"   📥 Contando {entidade}...")
            
        total_origem = con.execute(f"""
                                SELECT count(*) as qtd FROM read_csv_auto('zip://s3://bronze/raw/{ano_mes}/{entidade}*.zip',
                                sep=';', 
                                header=False,  
                                encoding='utf-8',
                                all_varchar=true,                    
                                ignore_errors=true);
                            """).fetchone()                                 
        
        total_origem_f = f"{total_origem[0]:,}".replace(",", ".")
        logger.info(f"✅ Total no ZIP: {total_origem_f} registros")
        
        # ------------------------------------------
        # 3. REGISTRA RESULTADO
        # ------------------------------------------
        diferenca = total_origem[0] - total_parquet
        status = "✅ OK" if diferenca == 0 else "⚠️ DIVERGÊNCIA"
        
        resultados_auditoria.append({
            'Entidade': entidade,
            'Origem': total_origem_f,
            'Destino': total_parquet_f,
            'Diferenca': f"{diferenca:,}".replace(',','.'),
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
        logger.info(f"{r['Entidade']:<18} | {r['Origem']:<15} | {r['Destino']:<15} | {r['Diferenca']:<12} | {r['Status']}")

    logger.info("="*85 + "\n")

dag_execucao = auditoria_ingestao_bronze_para_silver()
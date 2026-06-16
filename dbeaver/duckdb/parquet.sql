-- 1. Instala e carrega a extensão de rede/S3
INSTALL httpfs;
LOAD httpfs;

-- 2. Configura as credenciais do seu MinIO local
SET s3_endpoint='localhost:9000';  -- A porta da API do MinIO (geralmente 9000)
SET s3_access_key_id='cleverson';
SET s3_secret_access_key='cleverson';
SET s3_use_ssl=false; -- Como é localhost, não usa HTTPS
SET s3_region='us-east-1'; -- Valor padrão, apenas para preencher o requisito do protocolo
SET s3_url_style='path'; -- MUITO IMPORTANTE para o MinIO local funcionar!

SELECT * FROM read_parquet('s3://silver/cleaned/cnaes.parquet');
SELECT * FROM read_parquet('s3://silver/cleaned/motivos.parquet');
SELECT * FROM read_parquet('s3://silver/cleaned/municipios.parquet');
SELECT * FROM read_parquet('s3://silver/cleaned/naturezas.parquet');
SELECT * FROM read_parquet('s3://silver/cleaned/paises.parquet');
SELECT * FROM read_parquet('s3://silver/cleaned/qualificacoes.parquet');
SELECT * FROM read_parquet('s3://silver/cleaned/socios.parquet') LIMIT 1000;
SELECT * FROM read_parquet('s3://silver/cleaned/empresas.parquet') LIMIT 1000;
SELECT * FROM read_parquet('s3://silver/cleaned/estabelecimentos.parquet') LIMIT 1000;
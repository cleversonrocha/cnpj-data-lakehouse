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

SELECT * FROM read_parquet('s3://silver/cleaned/2026_06/qualificacoes.parquet');
SELECT * FROM read_parquet('s3://silver/cleaned/2026_06/motivos.parquet');
SELECT * FROM read_parquet('s3://silver/cleaned/2026_06/naturezas.parquet');
SELECT * FROM read_parquet('s3://silver/cleaned/2026_06/paises.parquet');
SELECT * FROM read_parquet('s3://silver/cleaned/2026_06/cnaes.parquet');
SELECT * FROM read_parquet('s3://silver/cleaned/2026_06/municipios.parquet');
SELECT * FROM read_parquet('s3://silver/cleaned/2026_06/simples.parquet') LIMIT 1000;
SELECT * FROM read_parquet('s3://silver/cleaned/2026_06/socios.parquet') LIMIT 1000;
SELECT * FROM read_parquet('s3://silver/cleaned/2026_06/empresas.parquet') LIMIT 1000;
SELECT * FROM read_parquet('s3://silver/cleaned/2026_06/estabelecimentos.parquet') LIMIT 1000;

SELECT * FROM read_parquet('s3://silver/cleaned/2026_06/empresas.parquet') em 
JOIN read_parquet('s3://silver/cleaned/2026_06/estabelecimentos.parquet') es ON es.cnpj_basico = em.cnpj_basico
JOIN read_parquet('s3://silver/cleaned/2026_06/municipios.parquet') mu ON mu.codigo = es.municipio
JOIN read_parquet('s3://silver/cleaned/2026_06/naturezas.parquet') n ON n.codigo = em.natureza_juridica
JOIN read_parquet('s3://silver/cleaned/2026_06/cnaes.parquet') cfp ON cfp.codigo = es.cnae_fiscal_principal
LEFT JOIN read_parquet('s3://silver/cleaned/2026_06/qualificacoes.parquet') q ON q.codigo = em.qualificacao_responsavel
LIMIT 1000;
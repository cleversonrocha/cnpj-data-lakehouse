-- 1. Inicializa o motor do DuckDB no DBeaver
INSTALL iceberg; LOAD iceberg;
INSTALL httpfs; LOAD httpfs;

-- 2. Conecta no seu MinIO
SET s3_endpoint='localhost:9000';
SET s3_access_key_id='cleverson';
SET s3_secret_access_key='cleverson';
SET s3_use_ssl=false;
SET s3_url_style='path';

-- 3. Habilita o DuckDB a varrer a pasta para achar a versão mais recente do Lakehouse
SET unsafe_enable_version_guessing = true;

-- 4. Consulta
SELECT * FROM iceberg_scan('s3://gold/cnaes');
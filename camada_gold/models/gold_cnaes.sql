{{ config(
    materialized='external',
    location='s3://gold/cnaes/cnaes.parquet'
) }}

SELECT 
    column0 AS codigo_cnae,
    column1 AS descricao_cnae
FROM 's3://silver/cnaes/cnaes.parquet'
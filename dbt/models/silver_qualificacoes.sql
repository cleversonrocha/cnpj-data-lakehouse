{{ config(
    materialized='external',
    location='s3://silver/cleaned/qualificacoes.parquet'
) }}

SELECT 
    CAST(column0 AS INTEGER) AS codigo,
    column1 AS descricao,    
    NOW() AS data_processamento
FROM 's3://silver/raw/qualificacoes.parquet'

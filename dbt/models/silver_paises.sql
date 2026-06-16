{{ config(
    materialized='external',    
    location='s3://silver/cleaned/paises.parquet'
) }}

SELECT 
    CAST(column0 AS INTEGER) AS codigo,
    column1 AS descricao,    
    NOW() AS data_processamento
FROM 's3://silver/raw/paises.parquet'

{{ config(
    materialized='table'    
) }}

SELECT    
    CAST(column0 AS VARCHAR) AS cnpj_basico,        
    CAST(column1 AS VARCHAR) AS razao_social,
    CAST(column2 AS INTEGER) AS natureza_juridica,
    CAST(column3 AS INTEGER) AS qualificacao_responsavel,
    CAST(column5 AS INTEGER) AS porte_empresa,
    CAST(column6 AS VARCHAR) AS ente_federativo_responsavel,         
    TRY_CAST(REPLACE(column4, ',', '.') AS DECIMAL(15, 2)) AS capital_social,        
    NOW() AS data_processamento
FROM 's3://silver/raw/empresas.parquet'
--Necessário pelo registro duplicado e sem razão social na base de 06/2026
WHERE column0 != '08314885' AND column1 IS NOT NULL

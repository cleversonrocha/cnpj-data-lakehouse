{{ config(
    materialized='external',
    location='s3://silver/cleaned/empresas.parquet'
) }}

SELECT    
    column0::VARCHAR AS cnpj_basico,        
    column1::VARCHAR AS razao_social,
    column2::INTEGER AS natureza_juridica,
    column3::INTEGER AS qualificacao_responsavel,
    column5::INTEGER AS porte_empresa,
    column6::VARCHAR AS ente_federativo_responsavel,         
    TRY_CAST(REPLACE(column4, ',', '.') AS DECIMAL(15, 2)) AS capital_social,        
    NOW() AS data_processamento
FROM 's3://silver/raw/empresas.parquet'

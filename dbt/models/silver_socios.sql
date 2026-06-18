{{ config(
    materialized='table'
) }}

SELECT    
    CAST(column01 AS INTEGER) AS identificador,
    CAST(column00 AS VARCHAR) AS cnpj_basico,
    CAST(column02 AS VARCHAR) AS nome_razao_social,
    CAST(column03 AS VARCHAR) AS cpf_cnpj,
    CAST(column04 AS INTEGER) AS qualificacao,
    TRY_CAST(column05 AS DATE) AS data_entrada_sociedade,        
    CAST(column06 AS INTEGER) AS pais,
    CAST(column09 AS INTEGER) AS qualificacao_representante,
    CAST(column10 AS INTEGER) AS faixa_etaria_socio,       
    CAST(column07 AS VARCHAR) AS representante_legal,
    CAST(column08 AS VARCHAR) AS nome_do_representante,     
    NOW() AS data_processamento
FROM 's3://silver/raw/socios.parquet'

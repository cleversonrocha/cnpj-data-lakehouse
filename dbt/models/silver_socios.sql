{{ config(
    materialized='table'
) }}

SELECT    
    column01::INTEGER AS identificador,
    column00::VARCHAR AS cnpj_basico,
    column02::VARCHAR AS nome_razao_social,
    column03::VARCHAR AS cpf_cnpj,
    column04::INTEGER AS qualificacao,
    TRY_CAST(column05 AS DATE) AS data_entrada_sociedade,        
    column06::INTEGER AS pais,
    column09::INTEGER AS qualificacao_representante,
    column10::INTEGER AS faixa_etaria_socio,       
    column07::VARCHAR AS representante_legal,
    column08::VARCHAR AS nome_do_representante,     
    NOW() AS data_processamento
FROM 's3://silver/raw/socios.parquet'

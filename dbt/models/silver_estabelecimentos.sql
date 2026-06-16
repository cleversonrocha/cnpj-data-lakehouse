{{ config(
    materialized='table'        
) }}

SELECT    
    column00::VARCHAR AS cnpj_basico,
    column01::VARCHAR AS cnpj_ordem,
    column02::VARCHAR AS cnpj_dv,        
    column03::INTEGER AS identificador_matriz_filial,
    column04::VARCHAR AS nome_fantasia,
    column05::INTEGER AS situacao_cadastral,
    TRY_CAST(column06 AS DATE) AS data_situacao_cadastral,
    column07::INTEGER AS motivo_situacao_cadastral,
    column08::VARCHAR AS nome_cidade_exterior,
    column09::INTEGER AS pais,
    TRY_CAST(column10 AS DATE) AS data_inicio_atividade,
    column11::VARCHAR AS cnae_fiscal_principal,
    column12::VARCHAR AS cnae_fiscal_secundaria, 
    column13::VARCHAR AS tipo_logradouro,
    column14::VARCHAR AS logradouro,    
    column15::VARCHAR AS numero,    
    column16::VARCHAR AS complemento,
    column17::VARCHAR AS bairro,
    column18::VARCHAR AS cep,
    column19::VARCHAR AS uf,        
    column20::INTEGER AS municipio,   
    column21::VARCHAR AS ddd_1,
    column22::VARCHAR AS telefone_1,
    column23::VARCHAR AS ddd_2,
    column24::VARCHAR AS telefone_2,
    column25::VARCHAR AS ddd_fax,
    column26::VARCHAR AS fax,
    column27::VARCHAR AS email,      
    column28::VARCHAR AS situacao_especial,
    TRY_CAST(column29 AS DATE) AS data_situacao_especial,        
    NOW() AS data_processamento
FROM 's3://silver/raw/estabelecimentos.parquet'

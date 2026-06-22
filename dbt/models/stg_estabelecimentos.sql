{{ config(
    materialized='table',
    post_hook=[
        export_to_s3(bucket_path='silver/cleaned', file_name='estabelecimentos')
    ]
) }}

SELECT    
    CAST(column00 AS VARCHAR) AS cnpj_basico,
    CAST(column01 AS VARCHAR) AS cnpj_ordem,
    CAST(column02 AS VARCHAR) AS cnpj_dv,        
    CAST(column03 AS INTEGER) AS identificador_matriz_filial,
    CAST(column04 AS VARCHAR) AS nome_fantasia,
    CAST(column05 AS INTEGER) AS situacao_cadastral,    
    CASE 
        WHEN LENGTH(TRIM(column06)) = 8 AND column06 != '00000000'
        THEN TRY_CAST(strptime(column06, '%Y%m%d') AS DATE)
        ELSE NULL
    END AS data_situacao_cadastral,
    CAST(column07 AS INTEGER) AS motivo_situacao_cadastral,
    CAST(column08 AS VARCHAR) AS nome_cidade_exterior,
    CAST(column09 AS INTEGER) AS pais,
    CASE 
        WHEN LENGTH(TRIM(column10)) = 8 AND column10 != '00000000'
        THEN TRY_CAST(strptime(column10, '%Y%m%d') AS DATE)
        ELSE NULL
    END AS data_inicio_atividade,
    CAST(column11 AS VARCHAR) AS cnae_fiscal_principal,    
    array_to_string(
        list_distinct(
            list_transform(string_split(column12, ','), x -> TRIM(x))
        ), ','
    ) AS cnae_fiscal_secundaria, 
    CAST(column13 AS VARCHAR) AS tipo_logradouro,
    CAST(column14 AS VARCHAR) AS logradouro,    
    CAST(column15 AS VARCHAR) AS numero,    
    CAST(column16 AS VARCHAR) AS complemento,
    CAST(column17 AS VARCHAR) AS bairro,
    CAST(column18 AS VARCHAR) AS cep,
    CAST(column19 AS VARCHAR) AS uf,        
    CAST(column20 AS INTEGER) AS municipio,   
    CAST(column21 AS VARCHAR) AS ddd_1,
    CAST(column22 AS VARCHAR) AS telefone_1,
    CAST(column23 AS VARCHAR) AS ddd_2,
    CAST(column24 AS VARCHAR) AS telefone_2,
    CAST(column25 AS VARCHAR) AS ddd_fax,
    CAST(column26 AS VARCHAR) AS fax,
    CAST(column27 AS VARCHAR) AS email,      
    CAST(column28 AS VARCHAR) AS situacao_especial,    
    CASE 
        WHEN LENGTH(TRIM(column29)) = 8 AND column29 != '00000000'
        THEN TRY_CAST(strptime(column29, '%Y%m%d') AS DATE)
        ELSE NULL
    END AS data_situacao_especial,        
    NOW() AS data_processamento
FROM {{ get_s3_path(base_path='silver/raw', file_name='estabelecimentos') }}

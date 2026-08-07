{{ config(    
    post_hook=[
        export_to_s3(bucket_path='silver/cleaned', file_name='stg_estabelecimentos')
    ]
) }}

SELECT    
    column00 AS cnpj_basico,
    column01 AS cnpj_ordem,
    column02 AS cnpj_dv,
    CASE 
        WHEN column00 = '08314885' AND column01 = '0051' AND column02 = '74' THEN CAST(2 AS TINYINT)
        ELSE CAST(column03 AS TINYINT)
    END AS identificador_matriz_filial,
    column04 AS nome_fantasia,
    CAST(column05 AS TINYINT) AS situacao_cadastral,    
    TRY_CAST(try_strptime(column06, '%Y%m%d') AS DATE) AS data_situacao_cadastral,            
    CAST(column07 AS TINYINT) AS motivo_situacao_cadastral,
    column08 AS cidade_exterior,
    CAST(column09 AS SMALLINT) AS pais,        
    TRY_CAST(try_strptime(column10, '%Y%m%d') AS DATE) AS data_inicio_atividade,            
    column11 AS cnae_fiscal_principal,
    array_to_string(
        list_distinct(
            list_transform(string_split(column12, ','), x -> TRIM(x))
        ), ','
    ) AS cnae_fiscal_secundaria, 
    column13 AS tipo_logradouro,
    column14 AS logradouro,    
    column15 AS numero,    
    column16 AS complemento,
    column17 AS bairro,    
    REGEXP_REPLACE(column18, '[^0-9]', '') AS cep,
    column19 AS uf,
    CAST(column20 AS SMALLINT) AS municipio,
    REGEXP_REPLACE(column21, '[^0-9]', '') AS ddd_1,
    REGEXP_REPLACE(column22, '[^0-9]', '') AS telefone_1,
    REGEXP_REPLACE(column23, '[^0-9]', '') AS ddd_2,
    REGEXP_REPLACE(column24, '[^0-9]', '') AS telefone_2,
    REGEXP_REPLACE(column25, '[^0-9]', '') AS ddd_fax,
    REGEXP_REPLACE(column26, '[^0-9]', '') AS fax,
    column27 AS email,
    column28 AS situacao_especial,    
    TRY_CAST(try_strptime(column29, '%Y%m%d') AS DATE) AS data_situacao_especial,                   
    NOW() AS data_processamento
FROM {{ get_s3_path(base_path='silver/raw', file_name='estabelecimentos') }}
{{ config(
    materialized='table',
    post_hook=[
        export_to_s3(bucket_path='silver/cleaned', file_name='socios')
    ]
) }}

SELECT    
    CAST(column01 AS INTEGER) AS identificador,
    CAST(column00 AS VARCHAR) AS cnpj_basico,
    CAST(column02 AS VARCHAR) AS nome_razao_social,
    CAST(column03 AS VARCHAR) AS cpf_cnpj,
    CAST(column04 AS INTEGER) AS qualificacao,    
    CASE 
        WHEN LENGTH(TRIM(column05)) = 8 AND column05 != '00000000'
        THEN TRY_CAST(strptime(column05, '%Y%m%d') AS DATE)
        ELSE NULL
    END AS data_entrada_sociedade,        
    CAST(column06 AS INTEGER) AS pais,
    CAST(column09 AS INTEGER) AS qualificacao_representante,
    CAST(column10 AS INTEGER) AS faixa_etaria_socio,       
    CAST(column07 AS VARCHAR) AS representante_legal,
    CAST(column08 AS VARCHAR) AS nome_do_representante,     
    NOW() AS data_processamento
FROM {{ get_s3_path(base_path='silver/raw', file_name='socios') }}

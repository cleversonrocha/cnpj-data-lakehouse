{{ config(
    post_hook=[
        export_to_s3(bucket_path='silver/cleaned', file_name='stg_socios')
    ]
) }}

SELECT        
    column00 AS cnpj_basico,        
    CAST(column01 AS TINYINT) AS identificador,
    column02 AS nome_razao_social,
    column03 AS cpf_cnpj,
    CAST(column04 AS TINYINT) AS qualificacao,    
    TRY_CAST(try_strptime(column05, '%Y%m%d') AS DATE) AS data_entrada_sociedade,    
    CAST(column06 AS SMALLINT) AS pais,
    column07 AS representante_legal,
    column08 AS nome_do_representante,
    CAST(column09 AS TINYINT) AS qualificacao_representante,
    CAST(column10 AS TINYINT) AS faixa_etaria_socio,
    NOW() AS data_processamento
FROM {{ get_s3_path(base_path='silver/raw', file_name='socios') }}
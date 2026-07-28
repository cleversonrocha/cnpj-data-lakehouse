{{ config(    
    post_hook=[
        export_to_s3(bucket_path='silver/cleaned', file_name='stg_empresas')
    ]
) }}

SELECT    
    column0 AS cnpj_basico,        
    column1 AS razao_social,
    CAST(column2 AS SMALLINT) AS natureza_juridica,
    CAST(column3 AS TINYINT) AS qualificacao_responsavel,    
    TRY_CAST(REPLACE(column4, ',', '.') AS DECIMAL(15, 2)) AS capital_social,
    CAST(column5 AS TINYINT) AS porte_empresa,
    column6 AS ente_federativo_responsavel,        
    NOW() AS data_processamento
FROM {{ get_s3_path(base_path='silver/raw', file_name='empresas') }}
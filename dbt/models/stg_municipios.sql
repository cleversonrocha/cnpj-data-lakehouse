{{ config(
    materialized='table',
    post_hook=[
        export_to_s3(bucket_path='silver/cleaned', file_name='municipios')
    ]
) }}

SELECT 
    CAST(column0 AS INTEGER) AS codigo,
    column1 AS descricao,    
    NOW() AS data_processamento
FROM {{ get_s3_path(base_path='silver/raw', file_name='municipios') }}

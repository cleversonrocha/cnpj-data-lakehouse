{{ config(
    materialized='table',
    post_hook=[
        export_to_s3(bucket_path='silver/cleaned', file_name='paises')
    ]
) }}

SELECT 
    CAST(column0 AS INTEGER) AS codigo,
    column1 AS descricao,    
    NOW() AS data_processamento
FROM {{ get_raw_path(base_path='silver/raw', file_name='paises') }}

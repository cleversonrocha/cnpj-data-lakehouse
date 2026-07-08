{{ config(
    post_hook=[
        export_to_s3(bucket_path='silver/cleaned', file_name='naturezas')
    ]
) }}

SELECT
    CAST(column0 AS INTEGER) AS codigo,
    SUBSTR(column0, 1, 3) || '-' ||  SUBSTR(column0, 4, 1) AS codigo_formatado,
    column1 AS descricao,    
    NOW() AS data_processamento
FROM {{ get_s3_path(base_path='silver/raw', file_name='naturezas') }}

{{ config(    
    post_hook=[
        export_to_s3(bucket_path='silver/cleaned', file_name='is_mei')
    ]
) }}

WITH status_mei AS (
    SELECT 1 AS codigo, 'SIM' AS descricao
    UNION ALL
    SELECT 2 AS codigo, 'NÃO' AS descricao
)

SELECT 
    CAST(ROW_NUMBER() OVER(ORDER BY codigo,descricao) AS TINYINT) AS sk_id,
    CAST(codigo AS TINYINT) AS codigo,
    descricao,
    NOW() AS data_processamento
FROM status_mei
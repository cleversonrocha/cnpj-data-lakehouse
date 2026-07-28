{{ config(
    post_hook=[
        export_to_s3(bucket_path='gold/int', file_name='int_municipios')
    ]
) }}

WITH municipios AS (
    SELECT 
        codigo,
        descricao     
    FROM {{ ref('stg_municipios') }}
    
    UNION ALL

    SELECT -1 AS codigo,'NÃO INFORMADO' AS descricao

    UNION ALL

    SELECT -2 AS codigo,'NÃO IDENTIFICADO' AS descricao
)

SELECT    
    CAST(codigo AS SMALLINT) AS codigo,
    descricao,    
    NOW() AS data_processamento
FROM municipios
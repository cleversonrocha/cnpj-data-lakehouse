{{ config(
    post_hook=[
        export_to_s3(bucket_path='gold/int', file_name='int_naturezas')
    ]
) }}

WITH naturezas_juridicas AS (
    SELECT
        codigo,
        SUBSTR(CAST(codigo AS VARCHAR), 1, 3) || '-' ||  SUBSTR(CAST(codigo AS VARCHAR), 4, 1) AS codigo_formatado,
        descricao
    FROM {{ ref('stg_naturezas') }}

    UNION ALL

    SELECT -1 AS codigo, 'NÃO INFORMADO' AS codigo_formatado, 'NÃO INFORMADO' AS descricao

    UNION ALL

    SELECT -2 AS codigo, 'NÃO IDENTIFICADO'AS codigo_formatado, 'NÃO IDENTIFICADO' AS descricao
)

SELECT    
    CAST(codigo AS SMALLINT) AS codigo,
    codigo_formatado,
    descricao,
    NOW() AS data_processamento
FROM naturezas_juridicas
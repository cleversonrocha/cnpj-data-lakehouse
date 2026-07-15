{{ config(
    post_hook=[
        export_to_s3(bucket_path='silver/cleaned', file_name='naturezas')
    ]
) }}

WITH naturezas_juridicas AS (
    SELECT
        column0 AS codigo,
        SUBSTR(column0, 1, 3) || '-' ||  SUBSTR(column0, 4, 1) AS codigo_formatado,
        column1 AS descricao
    FROM {{ get_s3_path(base_path='silver/raw', file_name='naturezas') }}

    UNION ALL

    SELECT -1 AS codigo, 'NÃO INFORMADO' AS codigo_formatado, 'NÃO INFORMADO' AS descricao

    UNION ALL

    SELECT -2 AS codigo, 'NÃO IDENTIFICADO'AS codigo_formatado, 'NÃO IDENTIFICADO' AS descricao
)

SELECT
    CAST(ROW_NUMBER() OVER(ORDER BY codigo,descricao) AS SMALLINT) AS sk_id,
    CAST(codigo AS SMALLINT) AS codigo,
    codigo_formatado,
    descricao,
    NOW() AS data_processamento
FROM naturezas_juridicas
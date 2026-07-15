{{ config(    
    post_hook=[
        export_to_s3(bucket_path='silver/cleaned', file_name='cnaes')
    ]
) }}

WITH cnaes AS (
    SELECT        
        column0 AS codigo,
        CASE 
            WHEN LENGTH(column0) = 7 THEN
                SUBSTR(column0, 1, 2) || '.' ||
                SUBSTR(column0, 3, 2) || '-' ||
                SUBSTR(column0, 5, 1) || '-' ||
                SUBSTR(column0, 6, 2)
            ELSE column0 
        END AS codigo_formatado,
        column1 AS descricao
    FROM {{ get_s3_path(base_path='silver/raw', file_name='cnaes') }}

    UNION ALL

    SELECT -1 AS codigo, '--1' AS codigo_formatado, 'NÃO INFORMADO' AS descricao

    UNION ALL

    SELECT -2 AS codigo, '--2' AS codigo_formatado,'NÃO IDENTIFICADO' AS descricao
)

SELECT
    CAST(ROW_NUMBER() OVER(ORDER BY codigo,descricao) AS SMALLINT) AS sk_id,
    codigo,
    codigo_formatado,
    descricao,    
    NOW() AS data_processamento
FROM cnaes
{{ config(    
    post_hook=[
        export_to_s3(bucket_path='silver/cleaned', file_name='portes_empresas')
    ]
) }}

WITH portes_empresas_distintas AS (
    SELECT
        DISTINCT
        column5 AS codigo,
        CASE 
            WHEN column5 = 1 THEN 'ME'
            WHEN column5 = 3 THEN 'EPP'
            WHEN column5 = 5 THEN 'DEMAIS'            
        END AS sigla,
        CASE             
            WHEN column5 = 1 THEN 'MICRO EMPRESA'
            WHEN column5 = 3 THEN 'EMPRESA DE PEQUENO PORTE'
            WHEN column5 = 5 THEN 'DEMAIS'            
        END AS descricao
    FROM {{ get_s3_path(base_path='silver/raw', file_name='empresas') }}
    WHERE column5 IS NOT NULL

    UNION ALL

    SELECT -1 AS codigo, 'NÃO INF.' AS sigla, 'NÃO INFORMADO' AS descricao

    UNION ALL

    SELECT -2 AS codigo, 'NÃO IDENT.' AS sigla, 'NÃO IDENTIFICADO' AS descricao
)

SELECT    
    CAST(ROW_NUMBER() OVER(ORDER BY codigo) AS TINYINT) AS sk_id,
    CAST(codigo AS TINYINT) AS codigo,
    sigla,
    descricao,
    NOW() AS data_processamento
FROM portes_empresas_distintas
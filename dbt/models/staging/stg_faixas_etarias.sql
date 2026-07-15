{{ config(    
    post_hook=[
        export_to_s3(bucket_path='silver/cleaned', file_name='faixas_etarias')
    ]
) }}

WITH faixas_etarias_distintas AS (
    SELECT
        DISTINCT
        column10 AS codigo,    
        CASE 
            WHEN column10 = 1 THEN '0 a 12 anos' 
            WHEN column10 = 2 THEN '13 a 20 anos' 
            WHEN column10 = 3 THEN '21 a 30 anos' 
            WHEN column10 = 4 THEN '31 a 40 anos' 
            WHEN column10 = 5 THEN '41 a 50 anos' 
            WHEN column10 = 6 THEN '51 a 60 anos' 
            WHEN column10 = 7 THEN '61 a 70 anos' 
            WHEN column10 = 8 THEN '71 a 80 anos' 
            WHEN column10 = 9 THEN 'maiores de 80 anos' 
            WHEN column10 = 0 THEN 'não se aplica'            
        END AS descricao    
    FROM {{ get_s3_path(base_path='silver/raw', file_name='socios') }}
    WHERE column10 IS NOT NULL

    UNION ALL

    SELECT -1 AS codigo,'não informado' AS descricao

    UNION ALL

    SELECT -2 AS codigo,'não identificado' AS descricao
)

SELECT
    CAST(ROW_NUMBER() OVER(ORDER BY codigo,descricao) AS TINYINT) AS sk_id,
    CAST(codigo AS TINYINT) AS codigo,
    descricao,    
    NOW() AS data_processamento
FROM faixas_etarias_distintas
{{ config(    
    post_hook=[
        export_to_s3(bucket_path='silver/cleaned', file_name='tipos_pessoas')
    ]
) }}

WITH tipos_pessoas_distintas AS (
    SELECT
        DISTINCT
        column01 AS codigo,
        CASE column01    	
            WHEN 1 THEN 'PESSOA FÍSICA'
            WHEN 2 THEN 'PESSOA JURÍDICA'
            WHEN 3 THEN 'ESTRANGEIRO'            
        END AS descricao
    FROM {{ get_s3_path(base_path='silver/raw', file_name='socios') }}
    WHERE column01 IS NOT NULL    

    UNION ALL

    SELECT -1 AS codigo, 'NÃO INFORMADO'

    UNION ALL

    SELECT -2 AS codigo, 'NÃO IDENTIFICADO'
)

SELECT
    CAST(ROW_NUMBER() OVER(ORDER BY codigo,descricao) AS TINYINT) AS sk_id,    
    CAST(codigo AS TINYINT) AS codigo,
    descricao,
    NOW() AS data_processamento 
FROM tipos_pessoas_distintas
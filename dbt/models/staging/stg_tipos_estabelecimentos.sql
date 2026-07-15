{{ config(    
    post_hook=[
        export_to_s3(bucket_path='silver/cleaned', file_name='tipos_estabelecimentos')
    ]
) }}

WITH tipos_estabelecimentos_distintos AS (
    SELECT DISTINCT column03 AS identificador_matriz_filial 
    FROM {{ get_s3_path(base_path='silver/raw', file_name='estabelecimentos') }}
    WHERE column03 IS NOT NULL
),

tipos_estabelecimentos_codificada AS (
    SELECT        
        identificador_matriz_filial AS codigo,
        CASE        
            WHEN identificador_matriz_filial = 1 THEN 'MATRIZ'
            WHEN identificador_matriz_filial = 2 THEN 'FILIAL'            
        END AS descricao
    FROM 
        tipos_estabelecimentos_distintos
    
    UNION ALL

    SELECT -1 AS codigo, 'NÃO INFORMADO' AS descricao

    UNION ALL

    SELECT -2 AS codigo, 'NÃO IDENTIFICADO' AS descricao
)

SELECT
    CAST(ROW_NUMBER() OVER(ORDER BY codigo,descricao) AS TINYINT) AS sk_id,
    CAST(codigo AS TINYINT) AS codigo,
    descricao,
    NOW() AS data_processamento
FROM tipos_estabelecimentos_codificada
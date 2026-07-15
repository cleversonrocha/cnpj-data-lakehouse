{{ config(    
    post_hook=[
        export_to_s3(bucket_path='silver/cleaned', file_name='situacoes_cadastrais')
    ]
) }}

WITH situacao_cadastral_distintas AS (
    SELECT DISTINCT column05 AS situacao_cadastral 
    FROM {{ get_s3_path(base_path='silver/raw', file_name='estabelecimentos') }}
    WHERE column05 IS NOT NULL
),

situacao_cadastral_codificada AS (
    SELECT                
        situacao_cadastral AS codigo,
        CASE 
            WHEN situacao_cadastral = 1 THEN 'NULA'
            WHEN situacao_cadastral = 2 THEN 'ATIVA'
            WHEN situacao_cadastral = 3 THEN 'SUSPENSA'
            WHEN situacao_cadastral = 4 THEN 'INAPTA'
            WHEN situacao_cadastral = 8 THEN 'BAIXADA'        
        END AS descricao
    FROM 
        situacao_cadastral_distintas

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
FROM situacao_cadastral_codificada
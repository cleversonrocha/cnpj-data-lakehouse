{{ config(    
    post_hook=[
        export_to_s3(bucket_path='silver/cleaned', file_name='situacoes_especiais')
    ]
) }}

WITH situacoes_especiais_distintas AS (
    SELECT DISTINCT column28 AS situacao_especial
    FROM {{ get_s3_path(base_path='silver/raw', file_name='estabelecimentos') }}
    WHERE column28 IS NOT NULL    
),

situacoes_especiais_codificada AS (
    SELECT        
        ROW_NUMBER() OVER (ORDER BY situacao_especial) AS codigo,
        situacao_especial AS descricao
    FROM situacoes_especiais_distintas

    UNION ALL

    SELECT -1 AS codigo,'NÃO INFORMADO' AS descricao

    UNION ALL

    SELECT -2 AS codigo,'NÃO IDENTIFICADO' AS descricao
)

SELECT
    CAST(ROW_NUMBER() OVER(ORDER BY codigo,descricao) AS TINYINT) AS sk_id,
    CAST(codigo AS TINYINT) AS codigo,    
    descricao,    
    NOW() AS data_processamento
FROM situacoes_especiais_codificada
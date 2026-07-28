{{ config(    
    post_hook=[
        export_to_s3(bucket_path='gold/int', file_name='int_situacoes_especiais')
    ]
) }}

WITH situacoes_especiais_distintas AS (
    SELECT DISTINCT situacao_especial
    FROM {{ ref('stg_estabelecimentos') }}
    WHERE situacao_especial IS NOT NULL    
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
    CAST(codigo AS TINYINT) AS codigo,    
    descricao,    
    NOW() AS data_processamento
FROM situacoes_especiais_codificada